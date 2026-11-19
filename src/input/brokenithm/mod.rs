//! A TCP server implementation specifically for Brokenithm clients
//!
//! Brokenithm is a UDP-based mobile virtual controller for CHUNITHM-style games.
//!
//! This module provides a TCP server/client that connects to Brokenithm clients,
//! allowing them to send input events and receive updates.
//!
//! NOTE: This module will be deprecated and turned into a separate plugin.
//!
//! If you want to use a Brokenithm-like frontend on an iOS client, consider
//! using the Web version, or the new plugin once complete
//!
//! This backend will be removed once the dedicated plugin is complete.

use crate::feedback::{FeedbackEvent, FeedbackEventPacket, LedEvent};
use crate::input::io_server::{
    ClientNode, ClientSettings, IoClientMessage, IoEventServer, IoInboundMessage,
};
use crate::input::{InputBackend, InputEvent, InputEventPacket, KeyboardEvent};
use crate::output::rgb_to_brg;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

/// Feedback device id the CHUNITHM RGB service tags slider LED frames with.
const LED_SLIDER_DEVICE_ID: &str = "chuni_jvs_board_2";
/// How long the touch pattern keeps ownership after the last game LED frame.
const LED_FEEDBACK_TIMEOUT_MS: u64 = 500;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Exponential reconnect delay with a short ceiling.
///
/// A disconnected iPad used to be retried every 500 ms forever, which turned a
/// single closed socket into thousands of log lines per minute. Backing off to a
/// few seconds keeps the log readable while still reconnecting quickly once the
/// app is back.
struct ReconnectBackoff {
    current: std::time::Duration,
}

impl ReconnectBackoff {
    const INITIAL: std::time::Duration = std::time::Duration::from_millis(500);
    const MAX: std::time::Duration = std::time::Duration::from_secs(5);
    /// A connection that lived at least this long counts as healthy, so the next
    /// attempt starts from the short delay again.
    const STABLE_AFTER: std::time::Duration = std::time::Duration::from_secs(2);

    fn next_delay(&mut self) -> std::time::Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(Self::MAX);
        delay
    }

    fn reset(&mut self) {
        self.current = Self::INITIAL;
    }

    /// Called after a connection ends. A connection that stayed up long enough
    /// resets the delay; a flapping one keeps backing off so a half-open socket
    /// cannot spin the reconnect loop. Returns the delay before the next attempt.
    fn after_disconnect(&mut self, lived: std::time::Duration) -> std::time::Duration {
        if lived >= Self::STABLE_AFTER {
            self.reset();
        }
        self.next_delay()
    }
}

/// Log a dropped connection once, with how long it lasted and when the next
/// attempt happens. Reconnect churn used to log a line per attempt at info level.
fn log_disconnect(addr: SocketAddr, lived: std::time::Duration, delay: std::time::Duration) {
    tracing::info!(
        "Brokenithm connection to {} closed after {:.1}s; retrying in {:?}",
        addr,
        lived.as_secs_f64(),
        delay
    );
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            current: Self::INITIAL,
        }
    }
}

mod idevice_proxy;
use idevice_proxy::spawn_iproxy;

/// Shared Brokenithm input state that can be polled by chuniio_proxy
#[derive(Debug, Clone, PartialEq)]
pub struct BoardInputState {
    pub air: Vec<u8>,    // 6 air zones
    pub slider: Vec<u8>, // 32 slider zones
    pub test_button: bool,
    pub service_button: bool,
    pub coin_pulse: bool, // Coin insertion pulse
}

impl BoardInputState {
    pub fn new() -> Self {
        Self {
            air: vec![0; 6],
            slider: vec![0; 32],
            test_button: false,
            service_button: false,
            coin_pulse: false,
        }
    }
}

/// Write half of a connected client. Reads and writes must not share a lock: the
/// read loop holds its half across `await`, so a shared mutex would block the LED
/// broadcast for as long as the client is idle.
type ClientWriter = Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>;

type ClientList = Arc<RwLock<Vec<ClientWriter>>>;

fn new_client_list() -> ClientList {
    Arc::new(RwLock::new(Vec::new()))
}

type ClientCleanup = (ClientList, ClientWriter);

/// Global shared state for Brokenithm input
static BROKENITHM_SHARED_STATE: OnceLock<RwLock<Option<BoardInputState>>> = OnceLock::new();

/// Enable shared state tracking for Brokenithm
pub async fn enable_brokenithm_shared_state() {
    let state_lock =
        BROKENITHM_SHARED_STATE.get_or_init(|| RwLock::new(Some(BoardInputState::new())));
    let mut state = state_lock.write().await;
    if state.is_none() {
        *state = Some(BoardInputState::new());
    }
    tracing::info!("Brokenithm shared state enabled");
}

/// Disable shared state tracking for Brokenithm
pub async fn disable_brokenithm_shared_state() {
    if let Some(state_lock) = BROKENITHM_SHARED_STATE.get() {
        let mut state = state_lock.write().await;
        *state = None;
        tracing::info!("Brokenithm shared state disabled");
    }
}

/// Get current Brokenithm shared state (for polling by chuniio_proxy)
pub async fn get_brokenithm_state() -> Option<BoardInputState> {
    if let Some(state_lock) = BROKENITHM_SHARED_STATE.get() {
        let state = state_lock.read().await;
        state.clone()
    } else {
        None
    }
}

/// Update Brokenithm shared state (called by input backends)
pub async fn set_brokenithm_state(new_state: BoardInputState) {
    if let Some(state_lock) = BROKENITHM_SHARED_STATE.get() {
        let mut state = state_lock.write().await;
        if state.is_some() {
            *state = Some(new_state);
        }
    }
}

pub struct BrokenithmTcpBackend {
    pub config: BrokenithmTcpConfig,
    /// Inbound sender into unified IoEventServer (produces IoInboundMessage::Input)
    pub inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
    /// Optional hub used to subscribe to LED feedback for the connected client
    pub io_server: Option<Arc<IoEventServer>>,
}

#[derive(Debug, Clone)]
pub enum BrokenithmTcpConfig {
    /// Listen for incoming connections (server mode)
    Server { bind_addr: SocketAddr },
    /// Connect directly to a TCP address
    Client { connect_addr: SocketAddr },
    /// Connect via iproxy device forwarding
    DeviceProxy {
        local_port: u16,
        device_port: u16,
        udid: Option<String>,
    },
}

impl BrokenithmTcpBackend {
    pub fn new(
        config: BrokenithmTcpConfig,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
    ) -> Self {
        Self {
            config,
            inbound_tx,
            io_server: None,
        }
    }

    /// Attach the unified IoEventServer so LED feedback can be forwarded to the client.
    pub fn with_io_server(mut self, io_server: Arc<IoEventServer>) -> Self {
        self.io_server = Some(io_server);
        self
    }

    pub fn server(
        bind_addr: SocketAddr,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
    ) -> Self {
        Self::new(BrokenithmTcpConfig::Server { bind_addr }, inbound_tx)
    }

    pub fn client(
        connect_addr: SocketAddr,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
    ) -> Self {
        Self::new(BrokenithmTcpConfig::Client { connect_addr }, inbound_tx)
    }

    pub fn device_proxy(
        local_port: u16,
        device_port: u16,
        udid: Option<String>,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
    ) -> Self {
        Self::new(
            BrokenithmTcpConfig::DeviceProxy {
                local_port,
                device_port,
                udid,
            },
            inbound_tx,
        )
    }
}

/// Build the 100-byte `cLED` frame the iOS client expects.
///
/// The client copies the 96 payload bytes, reverses them, and reads lane `i` as
/// `ledArray[6i..6i+6]` = main slot `[g, r, b]` then split slot `[g, r, b]`.
/// So slots are laid out in client order here and the payload is reversed on the
/// way out, which keeps the wire format byte-identical to the old broadcast.
fn cled_frame(slots: &[[u8; 3]; 32]) -> Vec<u8> {
    let mut led_msg = Vec::with_capacity(4 + 32 * 3);
    led_msg.extend_from_slice(&[99, 76, 69, 68]); // "cLED"
    for rgb in slots.iter().rev() {
        led_msg.extend_from_slice(&rgb_to_brg(rgb));
    }
    led_msg
}

/// Convert slider LED feedback into a `cLED` frame.
///
/// The RGB service either emits one event per clamped zone (16 by default) or one
/// per raw LED (31). A zone lights both its main and its split slot, so a 16-zone
/// frame fills all 16 lanes instead of only the first eight.
fn led_feedback_to_cled(feedback: &FeedbackEventPacket) -> Vec<u8> {
    let mut zones = [[0u8, 0u8, 0u8]; 32];
    let mut count = 0usize;
    for event in &feedback.events {
        if let FeedbackEvent::Led(LedEvent::Set {
            led_id,
            rgb: Some((r, g, b)),
            ..
        }) = event
        {
            if (*led_id as usize) < 32 {
                zones[*led_id as usize] = [*r, *g, *b];
                count = count.max(*led_id as usize + 1);
            }
        }
    }

    let mut slots = [[0u8, 0u8, 0u8]; 32];
    if count <= 16 {
        // One colour per lane: duplicate it into the main and split slot.
        for lane in 0..16 {
            let rgb = zones[lane];
            slots[lane * 2] = rgb;
            slots[lane * 2 + 1] = rgb;
        }
    } else {
        slots.copy_from_slice(&zones);
    }

    cled_frame(&slots)
}

/// LED pattern: set only active slider zones to white, others off.
///
/// The client mirrors its 32 zones before sending (`lanes.reversed()`), so the
/// wire array is in reverse display order: zone `2i` of lane `i` arrives at
/// index `31 - 2i` and its split slot at `30 - 2i`. Reading `2i` directly lit
/// the opposite end of the board.
fn led_slider_active_pattern(slider: &[u8]) -> Vec<u8> {
    let mut slots = [[0u8, 0u8, 0u8]; 32];
    for lane in 0..16 {
        let main = slider.get(31 - lane * 2).copied().unwrap_or(0) >= 128;
        let split = slider.get(30 - lane * 2).copied().unwrap_or(0) >= 128;
        if main {
            slots[lane * 2] = [255, 255, 255];
        }
        if split {
            slots[lane * 2 + 1] = [255, 255, 255];
        }
    }
    cled_frame(&slots)
}

#[async_trait::async_trait]
impl InputBackend for BrokenithmTcpBackend {
    async fn run(&mut self) -> eyre::Result<()> {
        // Enable shared state for chuniio_proxy polling
        enable_brokenithm_shared_state().await;

        match self.config.clone() {
            BrokenithmTcpConfig::Server { bind_addr } => self.run_server(bind_addr).await,
            BrokenithmTcpConfig::Client { connect_addr } => self.run_client(connect_addr).await,
            BrokenithmTcpConfig::DeviceProxy {
                local_port,
                device_port,
                udid,
            } => {
                self.run_device_proxy(local_port, device_port, udid.as_deref())
                    .await
            }
        }
    }
}

/// Drop a client from the broadcast list, matching by pointer rather than index so
/// the broadcast task and a closing connection cannot remove each other's entry.
async fn remove_client(clients: &ClientList, target: &ClientWriter) {
    let mut guard = clients.write().await;
    if let Some(pos) = guard.iter().position(|c| Arc::ptr_eq(c, target)) {
        guard.remove(pos);
    }
}

impl BrokenithmTcpBackend {
    /// Subscribe to slider LED feedback and mirror it to every connected client.
    fn spawn_led_broadcast(&self, clients: ClientList, last_feedback_ms: Arc<AtomicU64>) {
        let Some(io_server) = self.io_server.clone() else {
            tracing::debug!("No IoEventServer attached; Brokenithm LED feedback disabled");
            return;
        };

        // The TCP and iDevice backends both run at once, and IoEventServer keys
        // clients by id, so each subscription needs its own.
        static NEXT_SUBSCRIBER: AtomicU64 = AtomicU64::new(0);
        let subscriber_id = format!(
            "brokenithm_led_{}",
            NEXT_SUBSCRIBER.fetch_add(1, Ordering::Relaxed)
        );

        tokio::spawn(async move {
            let mut rx = io_server
                .register_client(ClientNode {
                    id: subscriber_id,
                    client_settings: ClientSettings {
                        feedback_listener_device_ids: vec![LED_SLIDER_DEVICE_ID.to_string()],
                        ..Default::default()
                    },
                })
                .await;
            tracing::info!("Brokenithm LED feedback subscribed to {LED_SLIDER_DEVICE_ID}");

            while let Some(msg) = rx.recv().await {
                let IoClientMessage::Feedback(packet) = msg else {
                    continue;
                };
                let led_msg = led_feedback_to_cled(&packet);
                last_feedback_ms.store(now_ms(), Ordering::Relaxed);

                let dead: Vec<ClientWriter> = {
                    let guard = clients.read().await;
                    let mut dead = Vec::new();
                    for client in guard.iter() {
                        let mut sock = client.lock().await;
                        if let Err(e) = sock.write_all(&led_msg).await {
                            tracing::warn!("Failed to send LED frame to Brokenithm client: {e}");
                            dead.push(client.clone());
                        }
                    }
                    dead
                };
                for client in dead {
                    remove_client(&clients, &client).await;
                }
            }

            tracing::info!("Brokenithm LED feedback channel closed");
        });
    }

    async fn run_server(&mut self, bind_addr: SocketAddr) -> eyre::Result<()> {
        let listener = TcpListener::bind(bind_addr).await?;
        tracing::info!("Brokenithm TCP backend listening on {}", bind_addr);

        let clients = new_client_list();
        let last_feedback_ms = Arc::new(AtomicU64::new(0));
        self.spawn_led_broadcast(clients.clone(), last_feedback_ms.clone());

        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::info!("Accepted TCP connection from {}", addr);
            let (read_half, write_half) = socket.into_split();
            let writer: ClientWriter = Arc::new(tokio::sync::Mutex::new(write_half));

            // Register this client for feedback
            {
                let mut clients_guard = clients.write().await;
                clients_guard.push(writer.clone());
            }

            let clients_for_removal = clients.clone();
            let inbound_tx = self.inbound_tx.clone();
            let last_feedback_ms = last_feedback_ms.clone();
            tokio::spawn(async move {
                Self::handle_connection(
                    read_half,
                    writer,
                    Some(clients_for_removal),
                    Some(addr),
                    inbound_tx,
                    last_feedback_ms,
                )
                .await;
            });
        }
    }

    async fn run_client(&mut self, connect_addr: SocketAddr) -> eyre::Result<()> {
        let clients = new_client_list();
        let last_feedback_ms = Arc::new(AtomicU64::new(0));
        self.spawn_led_broadcast(clients.clone(), last_feedback_ms.clone());

        let mut backoff = ReconnectBackoff::default();
        loop {
            match TcpStream::connect(connect_addr).await {
                Ok(socket) => {
                    tracing::debug!("Connected to Brokenithm device at {}", connect_addr);
                    let (read_half, write_half) = socket.into_split();
                    let writer: ClientWriter = Arc::new(tokio::sync::Mutex::new(write_half));
                    clients.write().await.push(writer.clone());
                    let inbound_tx = self.inbound_tx.clone();
                    let connected_at = std::time::Instant::now();
                    Self::handle_connection(
                        read_half,
                        writer,
                        Some(clients.clone()),
                        None,
                        inbound_tx,
                        last_feedback_ms.clone(),
                    )
                    .await;
                    let lived = connected_at.elapsed();
                    let delay = backoff.after_disconnect(lived);
                    log_disconnect(connect_addr, lived, delay);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    let delay = backoff.next_delay();
                    tracing::debug!(
                        "Failed to connect to {}: {}. Retrying in {:?}...",
                        connect_addr,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn run_device_proxy(
        &mut self,
        local_port: u16,
        device_port: u16,
        udid: Option<&str>,
    ) -> eyre::Result<()> {
        // Start iproxy (libimobiledevice) to forward device_port to local_port
        let _iproxy_child = spawn_iproxy(local_port, device_port, udid).await?;
        tracing::info!(
            "Started iproxy for device port {} -> localhost:{}",
            device_port,
            local_port
        );

        let clients = new_client_list();
        let last_feedback_ms = Arc::new(AtomicU64::new(0));
        self.spawn_led_broadcast(clients.clone(), last_feedback_ms.clone());

        // Connect to the local forwarded port
        let connect_addr = format!("127.0.0.1:{local_port}");
        let mut backoff = ReconnectBackoff::default();
        loop {
            match TcpStream::connect(&connect_addr).await {
                Ok(socket) => {
                    tracing::info!(
                        "Connected to Brokenithm device via iproxy at {}",
                        connect_addr
                    );
                    let (read_half, write_half) = socket.into_split();
                    let writer: ClientWriter = Arc::new(tokio::sync::Mutex::new(write_half));
                    clients.write().await.push(writer.clone());
                    let inbound_tx = self.inbound_tx.clone();
                    let connected_at = std::time::Instant::now();
                    Self::handle_connection(
                        read_half,
                        writer,
                        Some(clients.clone()),
                        None,
                        inbound_tx,
                        last_feedback_ms.clone(),
                    )
                    .await;
                    let lived = connected_at.elapsed();
                    let delay = backoff.after_disconnect(lived);
                    log_disconnect(
                        connect_addr.parse().expect("connect_addr is a socket addr"),
                        lived,
                        delay,
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    let delay = backoff.next_delay();
                    tracing::debug!(
                        "Failed to connect to {}: {}. Retrying in {:?}...",
                        connect_addr,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn handle_connection(
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        writer: ClientWriter,
        client_list: Option<ClientList>,
        addr: Option<SocketAddr>,
        inbound_tx: tokio::sync::mpsc::UnboundedSender<IoInboundMessage>,
        last_feedback_ms: Arc<AtomicU64>,
    ) {
        let mut buffer = Vec::new();
        let mut read_buf = [0u8; 256];
        let mut state_tracker = BrokenithmInputStateTracker::new();

        loop {
            match read_half.read(&mut read_buf).await {
                Ok(0) => {
                    if let Some(addr) = addr {
                        tracing::info!("Connection closed by {}", addr);
                    } else {
                        tracing::info!("Connection closed by remote");
                    }

                    if let Some(clients) = client_list {
                        remove_client(&clients, &writer).await;
                    }
                    break;
                }
                Ok(len) => {
                    // Append new data to buffer
                    buffer.extend_from_slice(&read_buf[..len]);

                    // Extract and process all complete messages
                    let (messages, consumed) = extract_brokenithm_messages(&buffer);
                    for message in messages {
                        if addr.is_some() {
                            tracing::trace!("Parsed message from {:?}: {:?}", addr, message);
                        }

                        match message {
                            BrokenithmMessage::Input {
                                air,
                                slider,
                                test_button,
                                service_button,
                            } => {
                                if let Some(packet) = state_tracker.diff_and_packet(
                                    &air,
                                    &slider,
                                    test_button,
                                    service_button,
                                ) {
                                    let _ = inbound_tx.send(IoInboundMessage::Input {
                                        client_id: "brokenithm".to_string(),
                                        packet,
                                    });
                                }
                                // Always update shared state regardless of event emission
                                state_tracker.update_shared_state(test_button, service_button);
                                // Fall back to the touch pattern only while the game is not
                                // sending LED frames; otherwise the broadcast task owns the LEDs.
                                if last_feedback_ms.load(Ordering::Relaxed) == 0
                                    || now_ms()
                                        .saturating_sub(last_feedback_ms.load(Ordering::Relaxed))
                                        > LED_FEEDBACK_TIMEOUT_MS
                                {
                                    let led_msg = led_slider_active_pattern(&slider);
                                    let mut sock = writer.lock().await;
                                    let _ = sock.write_all(&led_msg).await;
                                }
                            }
                            BrokenithmMessage::InsertCoin => {
                                state_tracker.pulse_coin();
                                let air = state_tracker.prev_air.clone();
                                let slider = state_tracker.prev_slider.clone();
                                let test_button = state_tracker.prev_test;
                                let service_button = state_tracker.prev_service;
                                if let Some(packet) = state_tracker.diff_and_packet(
                                    &air,
                                    &slider,
                                    test_button,
                                    service_button,
                                ) {
                                    let _ = inbound_tx.send(IoInboundMessage::Input {
                                        client_id: "brokenithm".to_string(),
                                        packet,
                                    });
                                }
                                // Always update shared state after coin pulse
                                state_tracker.update_shared_state(test_button, service_button);
                            }
                            BrokenithmMessage::Welcome => {
                                // Don't send test pattern after WEL message
                                // Only slider-active pattern is sent after input updates
                            }
                            BrokenithmMessage::EnableAir(_)
                            | BrokenithmMessage::TapCard
                            | BrokenithmMessage::Unknown(_) => {
                                // Do nothing
                            }
                        }
                    }

                    // Remove consumed bytes from buffer
                    if consumed > 0 {
                        buffer.drain(..consumed);
                    }
                }
                Err(e) => {
                    tracing::error!("TCP read error: {}", e);

                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum BrokenithmMessage {
    Welcome,
    InsertCoin,
    TapCard,
    EnableAir(bool),
    Input {
        air: Vec<u8>,
        slider: Vec<u8>,
        test_button: bool,
        service_button: bool,
    },
    Unknown(Vec<u8>),
}

fn parse_brokenithm_message(data: &[u8]) -> Option<BrokenithmMessage> {
    if data.len() < 4 {
        return None;
    }

    let length = data[0] as usize;
    let packet_name = &data[1..4];

    // Check if we have enough data for the full message
    if data.len() < length + 1 {
        return None;
    }

    // Debug logging
    // trace!(
    //     "Parsing message: len={}, name={:?} (ascii: {}), data length={}",
    //     length,
    //     packet_name,
    //     String::from_utf8_lossy(packet_name),
    //     data.len()
    // );

    match packet_name {
        [0x57, 0x45, 0x4C] if length == 3 => Some(BrokenithmMessage::Welcome), // "WEL"
        [0x46, 0x4E, 0x43] if length >= 4 => {
            // "FNC" - Function button
            let func_btn = data[4];
            match func_btn {
                1 => Some(BrokenithmMessage::InsertCoin),
                2 => Some(BrokenithmMessage::TapCard),
                _ => Some(BrokenithmMessage::Unknown(data.to_vec())),
            }
        }
        [0x41, 0x49, 0x52] if length >= 4 => {
            // "AIR" - Air enable/disable
            let air_enabled = data[4] != 0;
            // Restore to just EnableAir, let state tracker handle event emission
            Some(BrokenithmMessage::EnableAir(air_enabled))
        }
        [0x49, 0x4E, 0x50] if length >= 38 => {
            // "INP" - Input packet
            // Minimum: 6 air + 32 slider = 38 bytes payload
            // Full: 6 air + 32 slider + 1 test + 1 service = 40 bytes payload
            let payload = &data[4..];
            let payload_len = payload.len();

            // trace!(
            //     "INP message: expected_payload_len={}, actual_payload_len={}, total_len={}",
            //     length - 3,
            //     payload_len,
            //     data.len()
            // );

            if payload_len >= 38 {
                let air = reorder_air_zones(&payload[0..6]);
                let slider = payload[6..38].to_vec();

                // Test and service buttons are optional (may not be present in shorter messages)
                let test_button = if payload_len >= 39 {
                    payload[38] != 0
                } else {
                    false
                };
                let service_button = if payload_len >= 40 {
                    payload[39] != 0
                } else {
                    false
                };
                Some(BrokenithmMessage::Input {
                    air,
                    slider,
                    test_button,
                    service_button,
                })
            } else {
                tracing::warn!(
                    "INP message too short: {} bytes (need at least 38)",
                    payload.len()
                );
                Some(BrokenithmMessage::Unknown(data.to_vec()))
            }
        }
        _ => {
            tracing::debug!(
                "Unknown message: len={} name={:?} data={:x?}",
                length,
                packet_name,
                data
            );
            Some(BrokenithmMessage::Unknown(data.to_vec()))
        }
    }
}

/// Tracks previous input state to emit only changed events
struct BrokenithmInputStateTracker {
    prev_air: Vec<u8>,
    prev_slider: Vec<u8>,
    prev_test: bool,
    prev_service: bool,
    coin_pulse: bool,
}

impl BrokenithmInputStateTracker {
    fn new() -> Self {
        Self {
            prev_air: vec![0; 6],
            prev_slider: vec![0; 32],
            prev_test: false,
            prev_service: false,
            coin_pulse: false,
        }
    }

    /// Returns only the changed events as InputEventPacket (simplified, no debouncing)
    fn diff_and_packet(
        &mut self,
        air: &[u8],
        slider: &[u8],
        test_button: bool,
        service_button: bool,
    ) -> Option<InputEventPacket> {
        let mut events = Vec::new();

        // Air zones (CHUNIIO_IR_N as KeyPress/KeyRelease)
        for (i, (&prev, &curr)) in self.prev_air.iter().zip(air.iter()).enumerate() {
            let key = format!("CHUNIIO_IR_{i}");
            if prev == 0 && curr > 0 {
                events.push(InputEvent::Keyboard(KeyboardEvent::KeyPress {
                    key: key.clone(),
                }));
            } else if prev > 0 && curr == 0 {
                events.push(InputEvent::Keyboard(KeyboardEvent::KeyRelease {
                    key: key.clone(),
                }));
            }
        }

        // Slider - direct comparison without debouncing (like C implementation)
        for (i, (&prev, &curr)) in self.prev_slider.iter().zip(slider.iter()).enumerate() {
            let key = format!("CHUNIIO_SLIDER_{i}");
            if prev < 128 && curr >= 128 {
                events.push(InputEvent::Keyboard(KeyboardEvent::KeyPress {
                    key: key.clone(),
                }));
            } else if prev >= 128 && curr < 128 {
                events.push(InputEvent::Keyboard(KeyboardEvent::KeyRelease {
                    key: key.clone(),
                }));
            }
        }

        // Test button
        const TEST_BUTTON: &str = "CHUNIIO_TEST";
        if self.prev_test != test_button {
            events.push(InputEvent::Keyboard(if test_button {
                KeyboardEvent::KeyPress {
                    key: TEST_BUTTON.to_string(),
                }
            } else {
                KeyboardEvent::KeyRelease {
                    key: TEST_BUTTON.to_string(),
                }
            }));
        }

        // Service button
        const SERVICE_BUTTON: &str = "CHUNIIO_SERVICE";
        if self.prev_service != service_button {
            events.push(InputEvent::Keyboard(if service_button {
                KeyboardEvent::KeyPress {
                    key: SERVICE_BUTTON.to_string(),
                }
            } else {
                KeyboardEvent::KeyRelease {
                    key: SERVICE_BUTTON.to_string(),
                }
            }));
        }

        // Coin input (pulse)
        if self.coin_pulse {
            events.push(InputEvent::Keyboard(KeyboardEvent::KeyPress {
                key: "CHUNIIO_COIN".to_string(),
            }));
            events.push(InputEvent::Keyboard(KeyboardEvent::KeyRelease {
                key: "CHUNIIO_COIN".to_string(),
            }));
            self.coin_pulse = false;
        }

        // Update state - use raw slider values directly (like C implementation)
        self.prev_air.copy_from_slice(air);
        self.prev_slider.copy_from_slice(slider);
        self.prev_test = test_button;
        self.prev_service = service_button;

        if events.is_empty() {
            None
        } else {
            let device_id = "brokenithm".to_string();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            Some(InputEventPacket {
                device_id,
                timestamp,
                events,
            })
        }
    }

    /// Update shared state for chuniio_proxy polling (called after every input)
    fn update_shared_state(&self, test_button: bool, service_button: bool) {
        let prev_air = self.prev_air.clone();
        let prev_slider = self.prev_slider.clone();
        let coin_pulse = self.coin_pulse;
        tokio::spawn(async move {
            let new_state = BoardInputState {
                air: prev_air,
                slider: prev_slider,
                test_button,
                service_button,
                coin_pulse,
            };
            // Only update if state actually changed
            let current = get_brokenithm_state().await;
            if current.as_ref() != Some(&new_state) {
                set_brokenithm_state(new_state).await;
            }
        });
    }

    /// Call this to pulse a coin event
    fn pulse_coin(&mut self) {
        self.coin_pulse = true;
    }
}

fn extract_brokenithm_messages(buf: &[u8]) -> (Vec<BrokenithmMessage>, usize) {
    let mut messages = Vec::new();
    let mut consumed = 0;
    let mut pos = 0;

    while pos < buf.len() {
        // Need at least 4 bytes for length + 3-byte packet name
        if pos + 4 > buf.len() {
            break;
        }

        let length = buf[pos] as usize;

        // Check if we have enough data for the full message
        if pos + 1 + length > buf.len() {
            break;
        }

        let message_data = &buf[pos..pos + 1 + length];
        if let Some(parsed) = parse_brokenithm_message(message_data) {
            messages.push(parsed);
        }

        pos += 1 + length;
        consumed = pos;
    }

    (messages, consumed)
}

// Reorder air zones to logical order: 1,2,3,4,5,6 from the received 2,1,4,3,6,5
fn reorder_air_zones(air: &[u8]) -> Vec<u8> {
    match air.len() {
        6 => vec![air[1], air[0], air[3], air[2], air[5], air[4]],
        _ => air.to_vec(),
    }
}

fn led_test_pattern() -> Vec<u8> {
    let mut slots = [[0u8, 0u8, 0u8]; 32];
    for (i, slot) in slots.iter_mut().enumerate() {
        *slot = if i % 2 == 0 {
            [255, 0, 0] // Red
        } else {
            [0, 0, 255] // Blue
        };
    }
    cled_frame(&slots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feedback::LedPattern;

    /// Mirror of the iOS client: copy the payload, reverse it, then read lane `i`
    /// as main `[g, r, b]` at 6i and split `[g, r, b]` at 6i+3.
    fn client_lane(frame: &[u8], lane: usize) -> ((u8, u8, u8), (u8, u8, u8)) {
        assert_eq!(&frame[..4], &[99, 76, 69, 68]);
        let payload = &frame[4..];
        assert_eq!(payload.len(), 96);
        let reversed: Vec<u8> = payload.iter().rev().copied().collect();
        let at = |offset: usize| (reversed[offset + 1], reversed[offset], reversed[offset + 2]);
        (at(lane * 6), at(lane * 6 + 3))
    }

    #[test]
    fn cled_frame_matches_client_layout() {
        let mut slots = [[0u8; 3]; 32];
        slots[0] = [10, 20, 30];
        slots[1] = [40, 50, 60];
        slots[31] = [70, 80, 90];
        let frame = cled_frame(&slots);
        assert_eq!(frame.len(), 100);
        assert_eq!(client_lane(&frame, 0), ((10, 20, 30), (40, 50, 60)));
        assert_eq!(client_lane(&frame, 15).1, (70, 80, 90));
    }

    #[test]
    fn slider_pattern_lights_touched_lanes() {
        // The client mirrors its zones before sending, so a touch on lane 0
        // arrives at index 31 and a touch on lane 15 at index 1.
        let mut slider = vec![0u8; 32];
        slider[31] = 128; // lane 0 main
        slider[30] = 128; // lane 0 split
        slider[4] = 128; // lane 13 split
        let frame = led_slider_active_pattern(&slider);
        assert_eq!(client_lane(&frame, 0), ((255, 255, 255), (255, 255, 255)));
        assert_eq!(client_lane(&frame, 13), ((0, 0, 0), (255, 255, 255)));
        assert_eq!(client_lane(&frame, 1), ((0, 0, 0), (0, 0, 0)));
    }

    #[test]
    fn slider_pattern_is_not_mirrored() {
        // Regression: reading slider[2i] lit the opposite end of the board, so a
        // touch on the far left tile lit the far right one.
        let mut slider = vec![0u8; 32];
        slider[31] = 128; // far left lane, main slot
        let frame = led_slider_active_pattern(&slider);
        assert_eq!(client_lane(&frame, 0), ((255, 255, 255), (0, 0, 0)));
        assert_eq!(client_lane(&frame, 15), ((0, 0, 0), (0, 0, 0)));
    }

    #[test]
    fn feedback_zones_fill_both_slots() {
        let events = (0..16)
            .map(|i| {
                FeedbackEvent::Led(LedEvent::Set {
                    led_id: i as u8,
                    on: true,
                    brightness: Some(255),
                    rgb: Some((i as u8, 0, 0)),
                })
            })
            .collect();
        let packet = FeedbackEventPacket {
            device_id: LED_SLIDER_DEVICE_ID.to_string(),
            timestamp: 0,
            events,
        };
        let frame = led_feedback_to_cled(&packet);
        for lane in 0..16 {
            let expected = (lane as u8, 0, 0);
            assert_eq!(client_lane(&frame, lane), (expected, expected));
        }
    }

    #[test]
    fn feedback_raw_leds_keep_slots() {
        let events = (0..31)
            .map(|i| {
                FeedbackEvent::Led(LedEvent::Set {
                    led_id: i as u8,
                    on: true,
                    brightness: Some(255),
                    rgb: Some((0, i as u8, 0)),
                })
            })
            .collect();
        let packet = FeedbackEventPacket {
            device_id: LED_SLIDER_DEVICE_ID.to_string(),
            timestamp: 0,
            events,
        };
        let frame = led_feedback_to_cled(&packet);
        assert_eq!(client_lane(&frame, 0), ((0, 0, 0), (0, 1, 0)));
        assert_eq!(client_lane(&frame, 1), ((0, 2, 0), (0, 3, 0)));
    }

    #[test]
    fn non_rgb_events_do_not_panic() {
        let packet = FeedbackEventPacket {
            device_id: LED_SLIDER_DEVICE_ID.to_string(),
            timestamp: 0,
            events: vec![
                FeedbackEvent::Led(LedEvent::SetPattern {
                    led_id: 0,
                    pattern: LedPattern::Solid,
                }),
                FeedbackEvent::Led(LedEvent::Set {
                    led_id: 40,
                    on: true,
                    brightness: Some(255),
                    rgb: Some((1, 2, 3)),
                }),
            ],
        };
        let frame = led_feedback_to_cled(&packet);
        assert_eq!(frame.len(), 100);
        assert_eq!(client_lane(&frame, 0), ((0, 0, 0), (0, 0, 0)));
    }

    #[test]
    fn test_pattern_is_still_valid() {
        let frame = led_test_pattern();
        assert_eq!(frame.len(), 100);
        assert_eq!(client_lane(&frame, 0).0, (255, 0, 0));
    }

    #[test]
    fn reconnect_backoff_grows_then_resets() {
        let mut backoff = ReconnectBackoff::default();
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), std::time::Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), ReconnectBackoff::MAX);
        assert_eq!(backoff.next_delay(), ReconnectBackoff::MAX);

        backoff.reset();
        assert_eq!(backoff.next_delay(), std::time::Duration::from_millis(500));
    }
}
