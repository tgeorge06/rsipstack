//! Raw SIP messages carry credentials, phone numbers and SDP. The transports
//! log them at DEBUG only, so an application running at INFO never writes
//! them out.
use crate::sip::SipMessage;
use crate::transport::{udp::UdpConnection, TransportEvent};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tracing::Level;

const MARKER: &str = "Digest username=\"secret-marker\"";

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Receive one SIP message and send one over UDP with a subscriber at
/// `level`; returns everything it logged.
async fn udp_exchange_logged_at(level: Level) -> crate::Result<String> {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    // Callsite interest is cached process-wide; recompute it for this
    // subscriber.
    tracing::callsite::rebuild_interest_cache();

    let bob = UdpConnection::create_connection("127.0.0.1:0".parse()?, None, None).await?;
    let alice = UdpConnection::create_connection("127.0.0.1:0".parse()?, None, None).await?;
    let (bob_tx, mut bob_rx) = unbounded_channel();
    let text = format!(
        "REGISTER sip:bob@example.com SIP/2.0\r\n\
         Via: SIP/2.0/UDP 127.0.0.1:5061;branch=z9hG4bKlog\r\n\
         CSeq: 1 REGISTER\r\n\
         Authorization: {MARKER}\r\n\
         Content-Length: 0\r\n\r\n"
    );
    let received = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        alice.send_raw(text.as_bytes(), bob.get_addr()).await?;
        loop {
            if let Some(TransportEvent::Incoming(msg, _, _)) = bob_rx.recv().await {
                return Ok::<SipMessage, crate::Error>(msg);
            }
        }
    };
    let msg = tokio::select! {
        r = received => r?,
        _ = bob.serve_loop(bob_tx) => panic!("serve_loop exited"),
        _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("no message received"),
    };
    tracing::callsite::rebuild_interest_cache();
    bob.send(msg, Some(alice.get_addr())).await?;

    let out = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    Ok(out)
}

/// One test, both levels in turn: scoped subscribers in concurrent tests
/// would race on the process-wide callsite interest cache.
#[tokio::test]
async fn test_udp_raw_messages_are_logged_at_debug_only() -> crate::Result<()> {
    // The INFO check is only meaningful if the raw message is logged at all:
    // at DEBUG both directions are.
    // Other tests register the same callsites concurrently, which can race
    // the interest rebuild: retry the DEBUG sanity check before failing it.
    let mut logged = String::new();
    for _ in 0..3 {
        logged = udp_exchange_logged_at(Level::DEBUG).await?;
        if logged.contains("udp received") && logged.contains("udp send") {
            break;
        }
    }
    assert!(
        logged.contains("udp received") && logged.contains("udp send"),
        "got:\n{logged}"
    );
    assert!(logged.contains("secret-marker"));

    let logged = udp_exchange_logged_at(Level::INFO).await?;
    assert!(
        !logged.contains("secret-marker"),
        "raw SIP must not be logged at INFO, got:\n{logged}"
    );
    Ok(())
}
