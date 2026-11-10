//! AirPlay 2 headless daemon for integration with music servers.
//!
//! Reads JSON commands from stdin, emits JSON events on stdout.
//! Designed to run as a subprocess managed by Tune Server.
//!
//! Commands:
//!   {"cmd":"discover","timeout_s":5}
//!   {"cmd":"pair_pin_start","ip":"192.168.1.37","port":7000}
//!   {"cmd":"connect","ip":"192.168.1.37","port":7000,"pin":"1234"}
//!     Without a pin (or with the default "3939"): pair-verify with the
//!     identity saved for `device_id`, else transient pairing like an iPhone,
//!     else HomeKit pair-setup with 3939 (the former behaviour). A pin typed
//!     by the user goes straight to HomeKit pair-setup with that pin (#8347).
//!     Saved identities live in $AIRPLAY2_IDENTITY_DIR (current dir if unset).
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
//!   {"event":"connected","device":"HomePod","auth":"transient"}
//!     auth: "pair_verify" | "transient" | "pin_pairing"
//!   {"event":"paired","device":"HomePod"}
//!   {"event":"playing","position_s":1.0}
//!   {"event":"paused"}
//!   {"event":"stopped"}
//!   {"event":"error","message":"..."}
//!     A pairing refusal adds tlv_error (HomeKit kTLVError code),
//!     tlv_error_name, pairing_step and, when sent, retry_after_s.

use std::io::BufRead;
use std::net::IpAddr;
use std::time::Duration;

use airplay_audio::AudioDecoder;
use airplay_client::Connection;
use airplay_core::device::{Device, DeviceId};
use airplay_core::features::Features;
use airplay_core::stream::{StreamType, TimingProtocol, PtpMode};
use airplay_core::{AudioFormat, StreamConfig, AudioCodec};
use airplay_core::error::{Error as CoreError, PairingError, RtspError};
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
    // #8347 — additions only; Tune ignores fields it does not know.
    /// How `connect` paired: "pair_verify", "transient" or "pin_pairing".
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    /// kTLVError code sent by the receiver when it refused pairing.
    #[serde(skip_serializing_if = "Option::is_none")]
    tlv_error: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tlv_error_name: Option<String>,
    /// Pairing step that was refused, e.g. "pair-setup M4".
    #[serde(skip_serializing_if = "Option::is_none")]
    pairing_step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_s: Option<u16>,
}

impl Event {
    fn simple(event: &str) -> Self {
        Self {
            event: event.into(),
            name: None, ip: None, port: None, airplay2: None,
            device: None, message: None, position_s: None,
            model: None, state: None, volume: None,
            auth: None, tlv_error: None, tlv_error_name: None,
            pairing_step: None, retry_after_s: None,
        }
    }

    fn error(msg: &str) -> Self {
        let mut e = Self::simple("error");
        e.message = Some(msg.into());
        e
    }
}

/// PIN of HomeKit transient pairing, sent by Tune when the user typed nothing.
const TRANSIENT_PIN: &str = "3939";

/// One way of pairing with the receiver during `connect` (#8347).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PairingRoute {
    /// Pair-verify with the identity saved by an earlier pair-setup.
    PairVerify,
    /// HomeKit transient pair-setup with 3939, as an iPhone does.
    Transient,
    /// HomeKit pair-setup (M1-M6) with 3939: what the daemon always did
    /// before #8347, kept as a last resort for receivers that only took it.
    LegacyPinPairing,
    /// HomeKit pair-setup with the code the user typed.
    UserPinPairing,
}

impl PairingRoute {
    /// Value of the `auth` field of the `connected` event.
    fn auth(self) -> &'static str {
        match self {
            PairingRoute::PairVerify => "pair_verify",
            PairingRoute::Transient => "transient",
            PairingRoute::LegacyPinPairing | PairingRoute::UserPinPairing => "pin_pairing",
        }
    }

    fn label(self) -> &'static str {
        match self {
            PairingRoute::PairVerify => "pair-verify",
            PairingRoute::Transient => "transient pairing",
            PairingRoute::LegacyPinPairing => "pair-setup with 3939",
            PairingRoute::UserPinPairing => "pair-setup with the user code",
        }
    }
}

/// The code the user typed, if any. Tune sends "3939" when the user typed
/// nothing, so neither an absent, empty nor "3939" pin counts as typed.
fn user_pin(pin: Option<&str>) -> Option<&str> {
    match pin.map(str::trim) {
        Some(p) if !p.is_empty() && p != TRANSIENT_PIN => Some(p),
        _ => None,
    }
}

/// Is `device_id` a real receiver identifier? Saved identities are keyed on
/// it, so the all-zero placeholder must never select one.
fn device_id_is_known(device_id: &DeviceId) -> bool {
    device_id.to_mac_string().replace(':', "").chars().any(|c| c != '0')
}

/// Routes `connect` tries, in order.
fn plan_connect(user_pin: Option<&str>, device_id_known: bool, has_identity: bool) -> Vec<PairingRoute> {
    if user_pin.is_some() {
        return vec![PairingRoute::UserPinPairing];
    }
    let mut routes = Vec::with_capacity(3);
    if device_id_known && has_identity {
        routes.push(PairingRoute::PairVerify);
    }
    routes.push(PairingRoute::Transient);
    routes.push(PairingRoute::LegacyPinPairing);
    routes
}

/// The receiver's TLV refusal carried by `err`, if any.
fn tlv_refusal(err: &CoreError) -> Option<(&'static str, u8, &'static str, Option<u16>)> {
    match err {
        CoreError::Pairing(PairingError::DeviceError { step, code, name, retry_after }) => {
            Some((*step, *code, *name, *retry_after))
        }
        _ => None,
    }
}

/// After `route` failed with `err`, may `connect` try the next route?
///
/// A failed pair-verify (stale identity) always falls through. Otherwise
/// stop when the receiver cannot be reached, or when it said that more
/// attempts would make things worse: backoff (3), max peers (4),
/// max tries (5), busy (7).
fn may_try_next_route(route: PairingRoute, err: &CoreError) -> bool {
    if route == PairingRoute::PairVerify {
        return true;
    }
    match err {
        CoreError::Connection(_) | CoreError::Timeout => false,
        CoreError::Rtsp(RtspError::ConnectionRefused) => false,
        _ => !matches!(tlv_refusal(err), Some((_, 3 | 4 | 5 | 7, _, _))),
    }
}

/// `error` event for a failed `connect`: the message lists every route
/// tried; the TLV fields describe the receiver's last refusal.
fn connect_error_event(failures: &[(PairingRoute, CoreError)]) -> Event {
    let detail = failures
        .iter()
        .map(|(route, e)| format!("{}: {e}", route.label()))
        .collect::<Vec<_>>()
        .join("; ");
    let mut ev = Event::error(&format!("connect failed: {detail}"));
    if let Some((step, code, name, retry_after)) =
        failures.iter().rev().find_map(|(_, e)| tlv_refusal(e))
    {
        ev.tlv_error = Some(code);
        ev.tlv_error_name = Some(name.into());
        ev.pairing_step = Some(step.into());
        ev.retry_after_s = retry_after.filter(|s| *s > 0);
    }
    ev
}

async fn connect_by_route(
    route: PairingRoute,
    device: Device,
    config: StreamConfig,
    pin: Option<&str>,
) -> Result<Connection, CoreError> {
    match route {
        PairingRoute::PairVerify => Connection::connect_with_saved_identity(device, config)
            .await
            .unwrap_or_else(|| Err(RtspError::SetupFailed("no saved identity".into()).into())),
        PairingRoute::Transient => Connection::connect_with_pin(device, config, TRANSIENT_PIN).await,
        PairingRoute::LegacyPinPairing => {
            Connection::connect_with_pin_pairing(device, config, TRANSIENT_PIN).await
        }
        PairingRoute::UserPinPairing => {
            Connection::connect_with_pin_pairing(device, config, pin.unwrap_or(TRANSIENT_PIN)).await
        }
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
                let typed_pin = user_pin(cmd.pin.as_deref());
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

                let id_known = device_id_is_known(&device.id);
                let has_identity = id_known
                    && typed_pin.is_none()
                    && Connection::has_saved_identity(&device.id.to_mac_string());
                let routes = plan_connect(typed_pin, id_known, has_identity);

                let mut failures: Vec<(PairingRoute, CoreError)> = Vec::new();
                let mut connected = None;
                for route in routes {
                    match connect_by_route(route, device.clone(), config.clone(), typed_pin).await {
                        Ok(conn) => {
                            connected = Some((route, conn));
                            break;
                        }
                        Err(e) => {
                            let next = may_try_next_route(route, &e);
                            tracing::warn!("connect: {} failed: {e}", route.label());
                            failures.push((route, e));
                            if !next {
                                break;
                            }
                        }
                    }
                }

                match connected {
                    Some((route, conn)) => {
                        device_name = conn.device().name.clone();
                        connection = Some(conn);
                        let mut ev = Event::simple("connected");
                        ev.device = Some(device_name.clone());
                        ev.auth = Some(route.auth().into());
                        emit(ev);
                    }
                    None => emit(connect_error_event(&failures)),
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

            "pause" => {
                let Some(conn) = connection.as_mut() else {
                    emit(Event::error("not connected")); continue;
                };
                match conn.pause().await {
                    Ok(()) => emit(Event::simple("paused")),
                    Err(e) => emit(Event::error(&format!("pause failed: {e}"))),
                }
            }

            "resume" => {
                let Some(conn) = connection.as_mut() else {
                    emit(Event::error("not connected")); continue;
                };
                match conn.resume().await {
                    Ok(()) => {
                        let mut ev = Event::simple("playing");
                        ev.device = Some(device_name.clone());
                        emit(ev);
                    }
                    Err(e) => emit(Event::error(&format!("resume failed: {e}"))),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(step: &'static str, code: u8) -> CoreError {
        PairingError::device_error(step, code, None).into()
    }

    #[test]
    fn default_pin_is_not_a_typed_code() {
        assert_eq!(user_pin(None), None);
        assert_eq!(user_pin(Some("")), None);
        assert_eq!(user_pin(Some(" ")), None);
        assert_eq!(user_pin(Some("3939")), None);
        assert_eq!(user_pin(Some("1234")), Some("1234"));
        assert_eq!(user_pin(Some(" 0042 ")), Some("0042"));
    }

    #[test]
    fn typed_code_keeps_pin_pairing_only() {
        assert_eq!(plan_connect(Some("1234"), true, true), vec![PairingRoute::UserPinPairing]);
        assert_eq!(plan_connect(Some("1234"), false, false), vec![PairingRoute::UserPinPairing]);
    }

    #[test]
    fn saved_identity_goes_first_to_pair_verify() {
        assert_eq!(
            plan_connect(None, true, true),
            vec![PairingRoute::PairVerify, PairingRoute::Transient, PairingRoute::LegacyPinPairing]
        );
    }

    #[test]
    fn no_identity_starts_with_transient_like_an_iphone() {
        assert_eq!(
            plan_connect(None, true, false),
            vec![PairingRoute::Transient, PairingRoute::LegacyPinPairing]
        );
    }

    #[test]
    fn placeholder_device_id_never_uses_a_saved_identity() {
        let zero = DeviceId::from_mac_string("00:00:00:00:00:00").unwrap();
        let real = DeviceId::from_mac_string("AA:BB:CC:00:11:22").unwrap();
        assert!(!device_id_is_known(&zero));
        assert!(device_id_is_known(&real));
        assert_eq!(plan_connect(None, false, true)[0], PairingRoute::Transient);
    }

    #[test]
    fn failed_pair_verify_always_falls_through() {
        assert!(may_try_next_route(PairingRoute::PairVerify, &refusal("pair-verify M2", 2)));
        assert!(may_try_next_route(PairingRoute::PairVerify, &refusal("pair-verify M4", 3)));
    }

    #[test]
    fn transient_refusal_falls_back_unless_retrying_hurts() {
        let t = PairingRoute::Transient;
        assert!(may_try_next_route(t, &refusal("transient pair-setup M4", 1)));
        assert!(may_try_next_route(t, &refusal("transient pair-setup M4", 2)));
        assert!(may_try_next_route(t, &refusal("transient pair-setup M2", 6)));
        for code in [3, 4, 5, 7] {
            assert!(!may_try_next_route(t, &refusal("transient pair-setup M2", code)), "code {code}");
        }
        assert!(may_try_next_route(t, &RtspError::UnexpectedStatus(470).into()));
        assert!(!may_try_next_route(t, &RtspError::ConnectionRefused.into()));
        assert!(!may_try_next_route(t, &CoreError::Timeout));
    }

    #[test]
    fn tlv_codes_carry_their_homekit_meaning() {
        let names: Vec<_> = (1..=8).map(airplay_core::error::tlv_error_name).collect();
        assert_eq!(
            names,
            ["unknown", "authentication", "backoff", "max peers", "max tries", "unavailable", "busy", "unrecognised code"]
        );
    }

    #[test]
    fn connect_error_reports_every_route_and_the_last_refusal() {
        let failures = vec![
            (PairingRoute::Transient, refusal("transient pair-setup M4", 2)),
            (PairingRoute::LegacyPinPairing, PairingError::device_error("pair-setup M2", 3, Some(60)).into()),
        ];
        let ev = connect_error_event(&failures);
        let json: serde_json::Value = serde_json::to_value(&ev).unwrap();
        let msg = json["message"].as_str().unwrap();
        assert!(msg.starts_with("connect failed: "), "{msg}");
        assert!(msg.contains("transient pairing: Pairing error: receiver refused transient pair-setup M4: TLV error 2 (authentication)"), "{msg}");
        assert!(msg.contains("pair-setup with 3939: Pairing error: receiver refused pair-setup M2: TLV error 3 (backoff), retry after 60 s"), "{msg}");
        assert_eq!(json["event"], "error");
        assert_eq!(json["tlv_error"], 3);
        assert_eq!(json["tlv_error_name"], "backoff");
        assert_eq!(json["pairing_step"], "pair-setup M2");
        assert_eq!(json["retry_after_s"], 60);
    }

    #[test]
    fn connect_error_without_refusal_adds_no_field() {
        let ev = connect_error_event(&[(PairingRoute::Transient, RtspError::ConnectionRefused.into())]);
        let json = serde_json::to_string(&ev).unwrap();
        assert_eq!(json, r#"{"event":"error","message":"connect failed: transient pairing: RTSP error: Connection refused"}"#);
    }

    #[test]
    fn connected_event_keeps_old_fields_and_adds_auth() {
        let mut ev = Event::simple("connected");
        ev.device = Some("Mania".into());
        ev.auth = Some(PairingRoute::Transient.auth().into());
        assert_eq!(
            serde_json::to_string(&ev).unwrap(),
            r#"{"event":"connected","device":"Mania","auth":"transient"}"#
        );
        assert_eq!(PairingRoute::PairVerify.auth(), "pair_verify");
        assert_eq!(PairingRoute::UserPinPairing.auth(), "pin_pairing");
        assert_eq!(PairingRoute::LegacyPinPairing.auth(), "pin_pairing");
    }
}
