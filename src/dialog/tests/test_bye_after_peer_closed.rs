//! A UAC dialog whose TCP connection was closed by the peer (e.g. a carrier
//! idle-closing the flow during a long call) must still be able to send
//! in-dialog requests: BYE has to go out on a new connection instead of being
//! written to the closed one until Timer F.
use crate::dialog::{dialog_layer::DialogLayer, invitation::InviteOption};
use crate::sip::{prelude::HeadersExt, Method, Request, SipMessage, Uri};
use crate::transaction::endpoint::EndpointOption;
use crate::transport::{tcp_listener::TcpListenerConnection, udp::UdpConnection, TransportLayer};
use crate::EndpointBuilder;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::unbounded_channel;
use tokio_util::sync::CancellationToken;

const PEER_TAG: &str = "peer-tag";

/// Read SIP messages (no bodies) off a TCP stream until one is a request
/// with `method`.
async fn recv_request(stream: &mut TcpStream, buf: &mut Vec<u8>, method: Method) -> Request {
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let raw: Vec<u8> = buf.drain(..end + 4).collect();
            let text = String::from_utf8(raw).expect("non utf-8 SIP message");
            if let Ok(SipMessage::Request(req)) = SipMessage::try_from(text.as_str()) {
                if req.method == method {
                    return req;
                }
            }
            continue;
        }
        let mut chunk = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for {method}"))
            .expect("read failed");
        assert!(n > 0, "connection closed while waiting for {method}");
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn response(req: &Request, code: u16, reason: &str, extra: &str) -> String {
    let to = req.to_header().unwrap().value().to_string();
    let to = if to.contains(";tag=") {
        to
    } else {
        format!("{to};tag={PEER_TAG}")
    };
    format!(
        "SIP/2.0 {code} {reason}\r\n\
         Via: {}\r\n\
         From: {}\r\n\
         To: {to}\r\n\
         Call-ID: {}\r\n\
         CSeq: {}\r\n\
         {extra}\
         Content-Length: 0\r\n\r\n",
        req.via_header().unwrap().value(),
        req.from_header().unwrap().value(),
        req.call_id_header().unwrap().value(),
        req.cseq_header().unwrap().value(),
    )
}

#[tokio::test]
async fn test_bye_after_peer_closed_tcp_flow_uses_new_connection() -> crate::Result<()> {
    let token = CancellationToken::new();
    let peer = TcpListener::bind("127.0.0.1:0").await?;
    let peer_addr = peer.local_addr()?;

    let transport_layer = TransportLayer::new(token.child_token());
    let udp = UdpConnection::create_connection(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(token.child_token()),
    )
    .await?;
    transport_layer.add_transport(udp.into());
    let tcp_port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let tcp = TcpListenerConnection::new(format!("127.0.0.1:{tcp_port}").parse()?, None).await?;
    transport_layer.add_transport(tcp.into());
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(transport_layer)
        .with_cancel_token(token.child_token())
        .with_option(EndpointOption {
            t1x64: Duration::from_secs(8),
            ..Default::default()
        })
        .build();
    let endpoint_inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = endpoint_inner.serve().await;
    });
    let dialog_layer = DialogLayer::new(endpoint.inner.clone());

    let (state_sender, _state_receiver) = unbounded_channel();
    let invite_option = InviteOption {
        caller: Uri::try_from("sip:alice@example.com")?,
        callee: Uri::try_from(format!("sip:bob@{peer_addr};transport=tcp").as_str())?,
        contact: Uri::try_from(format!("sip:alice@127.0.0.1:{tcp_port};transport=tcp").as_str())?,
        ..Default::default()
    };
    let invite = tokio::spawn(async move {
        dialog_layer
            .do_invite(invite_option, state_sender)
            .await
            .map(|(dialog, resp)| (dialog, resp.map(|r| r.status_code)))
    });

    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), peer.accept())
        .await
        .expect("timeout waiting for the INVITE connection")?;
    let mut buf = Vec::new();
    let req = recv_request(&mut stream, &mut buf, Method::Invite).await;
    let contact = format!("Contact: <sip:bob@{peer_addr};transport=tcp>\r\n");
    stream
        .write_all(response(&req, 200, "OK", &contact).as_bytes())
        .await?;
    recv_request(&mut stream, &mut buf, Method::Ack).await;
    let (dialog, status) = tokio::time::timeout(Duration::from_secs(2), invite)
        .await
        .expect("do_invite timed out")
        .expect("do_invite panicked")?;
    assert_eq!(status, Some(crate::sip::StatusCode::OK));

    // The peer closes the flow the call was set up on.
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let bye = {
        let dialog = dialog.clone();
        tokio::spawn(async move { dialog.bye().await })
    };
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), peer.accept())
        .await
        .expect("the BYE must be sent on a new TCP connection")?;
    let mut buf = Vec::new();
    let req = recv_request(&mut stream, &mut buf, Method::Bye).await;
    assert_eq!(
        req.call_id_header()?.value(),
        dialog.id().call_id,
        "the BYE must belong to this dialog"
    );
    stream
        .write_all(response(&req, 200, "OK", "").as_bytes())
        .await?;
    tokio::time::timeout(Duration::from_secs(2), bye)
        .await
        .expect("bye timed out")
        .expect("bye panicked")?;
    token.cancel();
    Ok(())
}
