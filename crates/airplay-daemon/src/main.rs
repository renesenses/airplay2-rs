//! AirPlay 2 headless daemon for integration with music servers.
//!
//! Reads JSON commands from stdin, emits JSON events on stdout.
//! Designed to run as a subprocess managed by Tune Server.
//!
//! Commands:
//!   {"cmd":"discover","timeout_s":5}
//!   {"cmd":"pair_pin_start","ip":"192.168.1.37","port":7000}
//!   {"cmd":"connect","ip":"192.168.1.37","port":7000,"pin":"1234"}
//!   {"cmd":"play","path":"/path/to/file.flac"}
//!   {"cmd":"pause"}
//!   {"cmd":"resume"}
//!   {"cmd":"stop"}
//!   {"cmd":"volume","level":0.8}
//!   {"cmd":"status"}
//!   {"cmd":"disconnect"}
//!
//! Events (stdout, one JSON per line):
//!   {"event":"device","name":"HomePod","ip":"...","port":7000,"airplay2":true}
//!   {"event":"connected","device":"HomePod"}
//!   {"event":"paired","device":"HomePod"}
//!   {"event":"playing","position_s":1.0}
//!   {"event":"stopped"}
//!   {"event":"error","message":"..."}

use std::io::BufRead;
use std::net::IpAddr;
use std::time::Duration;

use airplay_audio::AudioDecoder;
use airplay_client::Connection;
use airplay_core::device::{Device, DeviceId};
use airplay_core::features::Features;
use airplay_core::stream::{StreamType, TimingProtocol, PtpMode};
use airplay_core::{AudioFormat, StreamConfig, AudioCodec};
use airplay_audio::AlacEncoder;
use airplay_discovery::{Discovery, ServiceBrowser};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct Command {
    cmd: String,
    // discover
    timeout_s: Option<u64>,
    // connect / pair
    ip: Option<String>,
    port: Option<u16>,
    pin: Option<String>,
    device_id: Option<String>,
    // play
    path: Option<String>,
    // volume
    level: Option<f64>,
}

#[derive(Serialize)]
struct Event {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    airplay2: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position_s: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume: Option<f64>,
}

impl Event {
    fn simple(event: &str) -> Self {
        Self {
            event: event.into(),
            name: None, ip: None, port: None, airplay2: None,
            device: None, message: None, position_s: None,
            model: None, state: None, volume: None,
        }
    }

    fn error(msg: &str) -> Self {
        let mut e = Self::simple("error");
        e.message = Some(msg.into());
        e
    }
}

fn emit(event: Event) {
    if let Ok(json) = serde_json::to_string(&event) {
        println!("{json}");
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async_main());
}

async fn async_main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .init();

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(16);

    // Stdin reader thread
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            let line = line.trim().to_string();
            if line.is_empty() { continue; }
            match serde_json::from_str::<Command>(&line) {
                Ok(cmd) => {
                    if cmd_tx.blocking_send(cmd).is_err() { break; }
                }
                Err(e) => {
                    emit(Event::error(&format!("invalid command: {e}")));
                }
            }
        }
    });

    let mut connection: Option<Connection> = None;
    let mut device_name = String::new();

    emit(Event::simple("ready"));

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd.cmd.as_str() {
            "discover" => {
                let timeout = cmd.timeout_s.unwrap_or(5);
                let browser = match ServiceBrowser::new() {
                    Ok(b) => b,
                    Err(e) => { emit(Event::error(&format!("discovery init failed: {e}"))); continue; }
                };
                match browser.scan(Duration::from_secs(timeout)).await {
                    Ok(devices) => {
                        for dev in &devices {
                            let ip = dev.addresses.iter()
                                .find(|a| a.is_ipv4())
                                .or(dev.addresses.first())
                                .map(|a| a.to_string())
                                .unwrap_or_default();
                            let mut ev = Event::simple("device");
                            ev.name = Some(dev.name.clone());
                            ev.ip = Some(ip);
                            ev.port = Some(dev.port);
                            ev.airplay2 = Some(dev.features.supports_buffered_audio());
                            ev.model = Some(dev.model.clone());
                            ev.device = Some(dev.id.to_mac_string());
                            emit(ev);
                        }
                        let mut ev = Event::simple("discover_done");
                        ev.message = Some(format!("{} devices found", devices.len()));
                        emit(ev);
                    }
                    Err(e) => emit(Event::error(&format!("discovery failed: {e}"))),
                }
            }

            "pair_pin_start" => {
                let Some(ip) = &cmd.ip else {
                    emit(Event::error("ip required")); continue;
                };
                let port = cmd.port.unwrap_or(7000);
                let url = format!("http://{}:{}/pair-pin-start", ip, port);
                match reqwest::Client::new().post(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        let mut ev = Event::simple("pin_requested");
                        ev.ip = Some(ip.clone());
                        emit(ev);
                    }
                    Ok(resp) => emit(Event::error(&format!("pair-pin-start: HTTP {}", resp.status()))),
                    Err(e) => emit(Event::error(&format!("pair-pin-start failed: {e}"))),
                }
            }

            "connect" => {
                let Some(ip_str) = &cmd.ip else {
                    emit(Event::error("ip required")); continue;
                };
                let ip: IpAddr = match ip_str.parse() {
                    Ok(ip) => ip,
                    Err(e) => { emit(Event::error(&format!("invalid ip: {e}"))); continue; }
                };
                let port = cmd.port.unwrap_or(7000);
                let pin = cmd.pin.as_deref().unwrap_or("3939");
                let dev_id_str = cmd.device_id.as_deref().unwrap_or("00:00:00:00:00:00");

                let features = Features::from_txt_value("0x4A7FCA00,0x3C354BD0").unwrap_or_default();
                let device_id = match DeviceId::from_mac_string(dev_id_str) {
                    Ok(id) => id,
                    Err(e) => { emit(Event::error(&format!("invalid device_id: {e}"))); continue; }
                };
                let device = Device {
                    id: device_id,
                    name: "AirPlay Device".into(),
                    model: "Unknown".into(),
                    manufacturer: None,
                    serial_number: None,
                    addresses: vec![ip],
                    port,
                    features,
                    required_sender_features: None,
                    public_key: None,
                    source_version: Default::default(),
                    firmware_version: None,
                    os_version: None,
                    protocol_version: None,
                    requires_password: false,
                    status_flags: 0,
                    access_control: None,
                    pairing_identity: None,
                    system_pairing_identity: None,
                    bluetooth_address: None,
                    homekit_home_id: None,
                    group_id: None,
                    is_group_leader: false,
                    group_public_name: None,
                    group_contains_discoverable_leader: false,
                    home_group_id: None,
                    household_id: None,
                    parent_group_id: None,
                    parent_group_contains_discoverable_leader: false,
                    tight_sync_id: None,
                    raop_port: None,
                    raop_encryption_types: None,
                    raop_codecs: None,
                    raop_transport: None,
                    raop_metadata_types: None,
                    raop_digest_auth: false,
                    vodka_version: None,
                };

                let audio_format = AudioFormat::default();
                let asc = if audio_format.codec == AudioCodec::Alac {
                    AlacEncoder::new(audio_format.clone()).ok().map(|e| e.magic_cookie())
                } else {
                    None
                };

                let config = StreamConfig {
                    stream_type: StreamType::Realtime,
                    audio_format,
                    timing_protocol: TimingProtocol::Ntp,
                    ptp_mode: PtpMode::Master,
                    latency_min: 22050,
                    latency_max: 88200,
                    supports_dynamic_stream_id: true,
                    asc,
                };

                match Connection::connect_with_pin_pairing(device, config, pin).await {
                    Ok(conn) => {
                        device_name = conn.device().name.clone();
                        connection = Some(conn);
                        let mut ev = Event::simple("connected");
                        ev.device = Some(device_name.clone());
                        emit(ev);
                    }
                    Err(e) => emit(Event::error(&format!("connect failed: {e}"))),
                }
            }

            "play" => {
                let Some(conn) = connection.as_mut() else {
                    emit(Event::error("not connected")); continue;
                };
                let Some(path) = &cmd.path else {
                    emit(Event::error("path required")); continue;
                };

                let decoder = match AudioDecoder::open(path) {
                    Ok(d) => d,
                    Err(e) => { emit(Event::error(&format!("decode error: {e}"))); continue; }
                };

                if let Err(e) = conn.setup().await {
                    emit(Event::error(&format!("setup failed: {e}"))); continue;
                }

                match conn.start_streaming(decoder).await {
                    Ok(()) => {
                        let mut ev = Event::simple("playing");
                        ev.device = Some(device_name.clone());
                        emit(ev);
                    }
                    Err(e) => emit(Event::error(&format!("play failed: {e}"))),
                }
            }

            "stop" => {
                if let Some(conn) = connection.as_mut() {
                    conn.stop().await.ok();
                    emit(Event::simple("stopped"));
                }
            }

            "volume" => {
                if let Some(conn) = connection.as_mut() {
                    let level = cmd.level.unwrap_or(0.5);
                    conn.set_volume(level as f32).await.ok();
                    let mut ev = Event::simple("volume_set");
                    ev.volume = Some(level);
                    emit(ev);
                }
            }

            "status" => {
                if let Some(conn) = connection.as_ref() {
                    let pos = conn.playback_position();
                    let state = conn.playback_state();
                    let mut ev = Event::simple("status");
                    ev.position_s = Some(pos as f64);
                    ev.state = Some(format!("{:?}", state));
                    ev.device = Some(device_name.clone());
                    emit(ev);
                } else {
                    let mut ev = Event::simple("status");
                    ev.state = Some("disconnected".into());
                    emit(ev);
                }
            }

            "disconnect" => {
                if let Some(mut conn) = connection.take() {
                    conn.stop().await.ok();
                    conn.disconnect().await.ok();
                    emit(Event::simple("disconnected"));
                }
            }

            other => {
                emit(Event::error(&format!("unknown command: {other}")));
            }
        }
    }
}
