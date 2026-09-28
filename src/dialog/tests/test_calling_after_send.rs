//! `DialogState::Calling` is the "INVITE is on the wire" signal: it is
//! notified only once the INVITE's transport write returned Ok, and never
//! for a send that could not reach the transport.
use super::test_in_dialog_provisional::recv_request;
use crate::dialog::{
    dialog::{DialogState, DialogStateReceiver},
    dialog_layer::DialogLayer,
    invitation::InviteOption,
};
use crate::sip::{Method, Uri};
use crate::transaction::endpoint::EndpointOption;
use crate::transport::{udp::UdpConnection, TransportLayer};
use crate::EndpointBuilder;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::unbounded_channel;
use tokio_util::sync::CancellationToken;

async fn uac(
    token: &CancellationToken,
    callee: &str,
) -> crate::Result<(
    tokio::task::JoinHandle<crate::Result<Option<crate::sip::Response>>>,
    DialogStateReceiver,
)> {
    uac_with_t1(token, callee, Duration::from_millis(20)).await
}

async fn uac_with_t1(
    token: &CancellationToken,
    callee: &str,
    t1: Duration,
) -> crate::Result<(
    tokio::task::JoinHandle<crate::Result<Option<crate::sip::Response>>>,
    DialogStateReceiver,
)> {
    let transport_layer = TransportLayer::new(token.child_token());
    let udp = UdpConnection::create_connection(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(token.child_token()),
    )
    .await?;
    let uac_addr = udp.get_addr().addr.clone();
    transport_layer.add_transport(udp.into());
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(transport_layer)
        .with_cancel_token(token.child_token())
        .with_option(EndpointOption {
            t1,
            t1x64: t1 * 64,
            ..Default::default()
        })
        .build();
    let endpoint_inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = endpoint_inner.serve().await;
    });
    let dialog_layer = DialogLayer::new(endpoint.inner.clone());
    let (state_sender, states) = unbounded_channel();
    let invite_option = InviteOption {
        caller: Uri::try_from("sip:alice@example.com")?,
        callee: Uri::try_from(callee)?,
        contact: Uri::try_from(format!("sip:alice@{uac_addr}").as_str())?,
        ..Default::default()
    };
    let invite = tokio::spawn(async move {
        let _endpoint = endpoint;
        dialog_layer
            .do_invite(invite_option, state_sender)
            .await
            .map(|(_, resp)| resp)
    });
    Ok((invite, states))
}

#[tokio::test]
async fn test_calling_is_notified_once_the_invite_is_written() -> crate::Result<()> {
    let token = CancellationToken::new();
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let callee = format!("sip:bob@{};transport=udp", peer.local_addr()?);
    let (invite, mut states) = uac(&token, &callee).await?;

    recv_request(&peer, Method::Invite).await;
    let first = tokio::time::timeout(Duration::from_secs(2), states.recv())
        .await
        .expect("Calling must be notified once the INVITE is sent")
        .unwrap();
    assert!(
        matches!(first, DialogState::Calling(_)),
        "the first state is Calling, got {first}"
    );
    invite.abort();
    token.cancel();
    Ok(())
}

#[tokio::test]
async fn test_calling_is_not_notified_when_the_invite_cannot_be_sent() -> crate::Result<()> {
    let token = CancellationToken::new();
    // A TCP target nobody listens on: every connect fails, so the INVITE
    // never reaches the wire and the transaction ends on Timer B.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = closed.local_addr()?.port();
    drop(closed);
    let callee = format!("sip:bob@127.0.0.1:{port};transport=tcp");
    let (invite, mut states) = uac(&token, &callee).await?;

    let resp = tokio::time::timeout(Duration::from_secs(5), invite)
        .await
        .expect("the INVITE must end on Timer B")
        .expect("invite task panicked")?
        .expect("Timer B delivers a local 408");
    assert_eq!(resp.status_code, crate::sip::StatusCode::RequestTimeout);
    assert!(resp.synthetic);
    let mut seen = Vec::new();
    while let Ok(state) = states.try_recv() {
        seen.push(state.to_string());
    }
    assert!(
        !seen.iter().any(|s| s.ends_with("(Calling)")),
        "an INVITE that never reached the wire must not notify Calling, got {seen:?}"
    );
    token.cancel();
    Ok(())
}

/// The first send finds no connection (nothing listens yet); a Timer A
/// retransmission connects and writes the INVITE. `Calling` is notified then,
/// exactly once.
#[tokio::test]
async fn test_calling_is_notified_once_when_a_retransmission_first_writes_the_invite(
) -> crate::Result<()> {
    use tokio::io::AsyncReadExt;
    let token = CancellationToken::new();
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
        probe.local_addr()?.port()
    };
    let callee = format!("sip:bob@127.0.0.1:{port};transport=tcp");
    let (invite, mut states) = uac_with_t1(&token, &callee, Duration::from_millis(200)).await?;

    // The initial connect is refused: nothing is on the wire yet.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        states.try_recv().is_err(),
        "no state may be notified before the INVITE is written"
    );

    // The callee comes up; Timer A retries the lookup and writes the INVITE.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("a Timer A retransmission must connect")?;
    let mut buf = vec![0u8; 4096];
    let len = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("the INVITE must be written")?;
    assert!(std::str::from_utf8(&buf[..len])
        .unwrap()
        .starts_with("INVITE "));

    // Let any further retransmission / notification happen.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut seen = Vec::new();
    while let Ok(state) = states.try_recv() {
        seen.push(state.to_string());
    }
    assert_eq!(
        seen.iter().filter(|s| s.ends_with("(Calling)")).count(),
        1,
        "Calling must be notified exactly once, got {seen:?}"
    );
    invite.abort();
    token.cancel();
    Ok(())
}
