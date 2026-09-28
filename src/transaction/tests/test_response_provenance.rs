//! In-process provenance on responses: `Response::synthetic` marks a response
//! the stack generated locally for the transaction user, and
//! `Response::received_from` records where a received response came from.
//! Neither is ever written onto the wire.
use crate::sip::{Response, SipMessage, StatusCode};
use crate::transaction::key::{TransactionKey, TransactionRole};
use crate::transaction::transaction::{Transaction, TransactionEvent};
use crate::transaction::{TransactionState, TransactionTimer};
use crate::Result;
use std::time::Duration;
use tokio::net::UdpSocket;

fn make_request(method: &str, target: std::net::SocketAddr) -> Result<crate::sip::Request> {
    let text = format!(
        "{method} sip:bob@{target} SIP/2.0\r\n\
         Via: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-provenance-{method}\r\n\
         Max-Forwards: 70\r\n\
         From: <sip:alice@example.com>;tag=from-tag\r\n\
         To: <sip:bob@example.com>\r\n\
         Call-ID: provenance@example.com\r\n\
         CSeq: 1 {method}\r\n\
         Content-Length: 0\r\n\r\n"
    );
    Ok(crate::sip::Request::try_from(text.as_str())?)
}

#[tokio::test]
async fn local_timer_responses_keep_provenance_at_the_transaction_user() -> Result<()> {
    let endpoint = super::create_test_endpoint(Some("127.0.0.1:0")).await?;
    for proceeding in [false, true] {
        let request = make_request("INVITE", "127.0.0.1:9".parse().unwrap())?;
        let key = TransactionKey::from_request(&request, TransactionRole::Client)?;
        let mut tx = Transaction::new_client(key.clone(), request, endpoint.inner.clone(), None);
        tx.state = if proceeding {
            TransactionState::Proceeding
        } else {
            TransactionState::Calling
        };
        let timer = if proceeding {
            TransactionTimer::TimerC(key)
        } else {
            TransactionTimer::TimerB(key)
        };
        tx.tu_sender.send(TransactionEvent::Timer(timer)).unwrap();
        let message = tokio::time::timeout(Duration::from_secs(5), tx.receive())
            .await
            .expect("local timer must produce a response promptly")
            .unwrap();
        let SipMessage::Response(response) = message else {
            panic!("expected timeout response")
        };
        assert_eq!(response.status_code, StatusCode::RequestTimeout);
        assert!(
            response.synthetic,
            "Timer B and C are local outcomes, not received SIP"
        );
        assert_eq!(response.received_from, None);
    }
    Ok(())
}

#[tokio::test]
async fn a_received_response_records_its_source_and_the_request_destination() -> Result<()> {
    let endpoint = super::create_test_endpoint(Some("127.0.0.1:0")).await?;
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let peer_addr = peer.local_addr()?;

    let request = make_request("OPTIONS", peer_addr)?;
    let key = TransactionKey::from_request(&request, TransactionRole::Client)?;
    let mut tx = Transaction::new_client(key, request, endpoint.inner.clone(), None);

    let exchange = async {
        tx.send().await?;
        let mut buf = vec![0u8; 4096];
        let (len, from) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
            .await
            .expect("the request must reach the peer")?;
        let req = crate::sip::Request::try_from(std::str::from_utf8(&buf[..len]).unwrap())?;
        let resp = endpoint
            .inner
            .make_response(&req, StatusCode::OK, None)
            .to_string();
        peer.send_to(resp.as_bytes(), from).await?;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), tx.receive())
                .await
                .expect("the response must reach the transaction")
            {
                Some(SipMessage::Response(resp)) => return Ok::<Response, crate::Error>(resp),
                Some(_) => {}
                None => panic!("transaction ended without a response"),
            }
        }
    };
    let resp = tokio::select! {
        r = exchange => r?,
        _ = endpoint.serve() => panic!("endpoint stopped"),
    };

    assert_eq!(resp.status_code, StatusCode::OK);
    assert!(!resp.synthetic, "a received response is not synthetic");
    let provenance = resp
        .received_from
        .expect("a received response records where it came from");
    assert_eq!(provenance.source, peer_addr);
    assert_eq!(provenance.request_destination, Some(peer_addr));

    // In-process only: not serialized, and a reparse drops it.
    let text = resp.to_string();
    assert!(!text.contains("received_from") && !text.contains("synthetic"));
    let reparsed = Response::try_from(text.as_str())?;
    assert_eq!(reparsed.received_from, None);
    assert!(!reparsed.synthetic);
    Ok(())
}
