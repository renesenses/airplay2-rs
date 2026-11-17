//! #8933 : le volume connu précède le premier paquet, sans enceinte.
use super::*;
use airplay_core::{
    codec::{AudioCodec, AudioFormat, SampleRate},
    stream::StreamType,
    DeviceId,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket as TokioUdpSocket};

fn make_test_device() -> Device {
    Device {
        id: DeviceId::from_mac_string("AA:BB:CC:DD:EE:FF").unwrap(),
        name: "Test Device".to_string(),
        addresses: vec!["192.168.1.100".parse().unwrap()],
        port: 7000,
        model: "TestModel1,1".to_string(),
        manufacturer: None,
        serial_number: None,
        features: Default::default(),
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
    }
}

fn make_test_config() -> StreamConfig {
    StreamConfig {
        stream_type: StreamType::Buffered,
        audio_format: AudioFormat {
            codec: AudioCodec::Alac,
            sample_rate: SampleRate::Hz44100,
            channels: 2,
            bit_depth: 16,
            frames_per_packet: 352,
        },
        timing_protocol: TimingProtocol::Ntp,
        ptp_mode: airplay_core::PtpMode::Master,
        latency_min: 22050,
        latency_max: 132300,
        supports_dynamic_stream_id: true,
        asc: None,
    }
}

async fn requete(stream: &mut TcpStream) -> (String, String) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
        assert!(head.len() < 8192);
    }
    let head = String::from_utf8(head).unwrap();
    let len: usize = head
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap_or(0);
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await.unwrap();
    (head, String::from_utf8(body).unwrap())
}

async fn repondre(stream: &mut TcpStream, head: &str) {
    let cseq = head
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("cseq").then(|| value.trim())
        })
        .unwrap();
    stream
        .write_all(
            format!("RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\nContent-Length: 0\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
}

async fn connexion(addr: SocketAddr, data_port: u16) -> Connection {
    let mut device = make_test_device();
    device.addresses = vec![addr.ip()];
    device.port = addr.port();
    let config = make_test_config();
    let mut session = RtspSession::new(device.clone(), config.clone());
    session.set_connected().unwrap();
    session.set_paired().unwrap();
    session.process_setup_phase1_response(
        br#"<?xml version="1.0"?><plist version="1.0"><dict><key>eventPort</key><integer>0</integer><key>timingPort</key><integer>0</integer></dict></plist>"#
    ).unwrap();
    let phase2 = format!(
        r#"<?xml version="1.0"?><plist version="1.0"><dict><key>streams</key><array><dict><key>type</key><integer>{}</integer><key>dataPort</key><integer>{data_port}</integer><key>controlPort</key><integer>{data_port}</integer><key>streamID</key><integer>1</integer></dict></array></dict></plist>"#,
        config.stream_type as u32
    );
    session
        .process_setup_phase2_response(phase2.as_bytes())
        .unwrap();
    let mut rtsp = RtspConnection::new(addr);
    rtsp.connect().await.unwrap();
    Connection {
        device,
        rtsp,
        session,
        streamer: None,
        playback_state: PlaybackState::Stopped,
        volume: 0.25,
        stream_config: config,
        timing_offset: None,
        timing_tx: None,
        timing_task: None,
        timing_server: None,
        ptp_master: None,
        ptp_master_sync_task: None,
        ptp_master_clock_id: None,
        control_receiver: None,
        control_task: None,
        events_stream: None,
        render_delay_ms: 0,
        eq_config: None,
        eq_params: None,
        stream_stats: crate::stats::StreamStats::new(),
    }
}

fn wav_silencieux() -> Vec<u8> {
    let data_len = 44_100_u32 * 2 * 2 * 2;
    let mut wav = Vec::new();
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&2_u16.to_le_bytes());
    wav.extend_from_slice(&44_100_u32.to_le_bytes());
    wav.extend_from_slice(&(44_100_u32 * 4).to_le_bytes());
    wav.extend_from_slice(&4_u16.to_le_bytes());
    wav.extend_from_slice(&16_u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    wav.resize(44 + data_len as usize, 0);
    wav
}

async fn recevoir_audio(udp: &TokioUdpSocket) {
    let mut packet = [0_u8; 8192];
    loop {
        let len = udp.recv(&mut packet).await.unwrap();
        if len >= 12 && matches!(packet[1] & 0x7f, 96 | 103) {
            return;
        }
    }
}

async fn eprouver(live: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut conn = connexion(
        listener.local_addr().unwrap(),
        udp.local_addr().unwrap().port(),
    )
    .await;
    let receiver = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let (flush, _) = requete(&mut tcp).await;
        assert!(flush.starts_with("FLUSH "));
        repondre(&mut tcp, &flush).await;
        let (volume, body) = requete(&mut tcp).await;
        assert!(volume.starts_with("SET_PARAMETER "));
        assert!(body.contains("volume: -12.04"), "{body}");
        // Le récepteur diffère sa réponse : aucun audio ne doit partir
        // tant que le réglage de volume n'a pas terminé.
        let avant_volume = tokio::time::timeout(Duration::from_secs(2), recevoir_audio(&udp))
            .await
            .is_ok();
        repondre(&mut tcp, &volume).await;
        let apres_volume = tokio::time::timeout(Duration::from_secs(5), recevoir_audio(&udp))
            .await
            .is_ok();
        (avant_volume, apres_volume)
    });
    let result = if live {
        let (tx, decoder) = LiveAudioDecoder::create_pair(44_100, 2, 2);
        assert!(tx.send(airplay_audio::LivePcmFrame {
            samples: vec![0; 44_100 * 2 * 2],
            channels: 2,
            sample_rate: 44_100,
        }));
        drop(tx);
        conn.start_streaming_live(decoder).await
    } else {
        conn.start_streaming(AudioDecoder::from_bytes(&wav_silencieux(), Some("wav")).unwrap())
            .await
    };
    let observation = receiver.await.unwrap();
    conn.stop().await.unwrap();
    result.unwrap();
    assert!(
        !observation.0,
        "audio reçu avant la fin du SET_PARAMETER volume (#8933)"
    );
    assert!(
        observation.1,
        "témoin : un paquet doit arriver après le volume"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fichier_volume_avant_paquet_8933() {
    tokio::time::timeout(Duration::from_secs(30), eprouver(false))
        .await
        .expect("banc RTSP borné");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_volume_avant_paquet_8933() {
    tokio::time::timeout(Duration::from_secs(30), eprouver(true))
        .await
        .expect("banc RTSP borné");
}
