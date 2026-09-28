//! `bye()` notifies `Terminated` with the lifecycle subscribers rely on to
//! finish their teardown: a UAC ends the dialog locally even when its BYE
//! cannot be sent, and a UAS notifies `Terminated` before its BYE's response.
use crate::dialog::{
    dialog::{DialogInner, DialogState, DialogStateReceiver, TerminatedReason},
    invite_dialog::InviteDialog,
    DialogId,
};
use crate::sip::{Method, Request, Response, SipMessage, Uri};
use crate::transaction::endpoint::{Endpoint, TargetLocator};
use crate::transaction::key::TransactionRole;
use crate::transport::{udp::UdpConnection, SipAddr, TransportLayer};
use crate::EndpointBuilder;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::unbounded_channel;
use tokio_util::sync::CancellationToken;

/// A locator that can resolve nothing: every request send fails.
struct FailingLocator;

#[async_trait]
impl TargetLocator for FailingLocator {
    async fn locate(&self, _uri: &Uri) -> crate::Result<SipAddr> {
        Err(crate::Error::Error("no route".to_string()))
    }
}

async fn endpoint(
    token: &CancellationToken,
    locator: Option<Box<dyn TargetLocator>>,
) -> crate::Result<Endpoint> {
    let tl = TransportLayer::new(token.child_token());
    let udp = UdpConnection::create_connection(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(token.child_token()),
    )
    .await?;
    tl.add_transport(udp.into());
    let mut builder = EndpointBuilder::new();
    builder
        .with_user_agent("rsipstack-test")
        .with_transport_layer(tl)
        .with_cancel_token(token.child_token());
    if let Some(locator) = locator {
        builder.with_target_locator(locator);
    }
    let endpoint = builder.build();
    let inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = inner.serve().await;
    });
    Ok(endpoint)
}

/// A confirmed INVITE dialog in `role` whose remote target is `peer`.
fn confirmed_dialog(
    endpoint: &Endpoint,
    role: TransactionRole,
    peer: &str,
) -> crate::Result<(InviteDialog, DialogStateReceiver)> {
    let (uri, to) = match role {
        TransactionRole::Client => (format!("sip:bob@{peer}"), "<sip:bob@example.com>"),
        TransactionRole::Server => ("sip:bob@example.com".to_string(), "<sip:bob@example.com>"),
    };
    let invite = Request::try_from(
        format!(
            "INVITE {uri} SIP/2.0\r\n\
             Via: SIP/2.0/UDP {peer};branch=z9hG4bK-bye-lifecycle\r\n\
             From: <sip:alice@example.com>;tag=alice-tag\r\n\
             To: {to}\r\n\
             Call-ID: bye-lifecycle\r\n\
             CSeq: 1 INVITE\r\n\
             Contact: <sip:alice@{peer}>\r\n\
             Content-Length: 0\r\n\r\n"
        )
        .as_str(),
    )?;
    let id = match role {
        TransactionRole::Client => DialogId {
            call_id: "bye-lifecycle".to_string(),
            local_tag: "alice-tag".to_string(),
            remote_tag: "bob-tag".to_string(),
        },
        TransactionRole::Server => DialogId {
            call_id: "bye-lifecycle".to_string(),
            local_tag: "bob-tag".to_string(),
            remote_tag: "alice-tag".to_string(),
        },
    };
    let (state_sender, states) = unbounded_channel();
    let (tu_sender, _) = unbounded_channel();
    let inner = DialogInner::new(
        role,
        id.clone(),
        invite,
        endpoint.inner.clone(),
        state_sender,
        None,
        Some(Uri::try_from("sip:local@127.0.0.1:5060")?),
        tu_sender,
    )?;
    inner.transition(DialogState::Confirmed(id, Response::default()))?;
    Ok((InviteDialog::from_inner(Arc::new(inner)), states))
}

fn terminated(states: &mut DialogStateReceiver) -> Option<TerminatedReason> {
    let mut reason = None;
    while let Ok(state) = states.try_recv() {
        if let DialogState::Terminated(_, r) = state {
            reason = Some(r);
        }
    }
    reason
}

#[tokio::test]
async fn test_uac_bye_that_cannot_be_sent_still_terminates() -> crate::Result<()> {
    let token = CancellationToken::new();
    let endpoint = endpoint(&token, Some(Box::new(FailingLocator))).await?;
    let (dialog, mut states) =
        confirmed_dialog(&endpoint, TransactionRole::Client, "192.0.2.1:5060")?;

    tokio::time::timeout(Duration::from_secs(2), dialog.bye())
        .await
        .expect("bye must not hang")?;
    assert!(matches!(
        terminated(&mut states),
        Some(TerminatedReason::UacBye)
    ));
    assert!(dialog.state().is_terminated());
    token.cancel();
    Ok(())
}

#[tokio::test]
async fn test_uas_bye_notifies_terminated_before_its_response() -> crate::Result<()> {
    let token = CancellationToken::new();
    let endpoint = endpoint(&token, None).await?;
    // A peer that receives the BYE and never answers it.
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let (dialog, mut states) = confirmed_dialog(
        &endpoint,
        TransactionRole::Server,
        &peer.local_addr()?.to_string(),
    )?;

    let bye = tokio::spawn({
        let dialog = dialog.clone();
        async move { dialog.bye().await }
    });
    let mut buf = vec![0u8; 4096];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
        .await
        .expect("the BYE must be sent")?;
    let msg = SipMessage::try_from(std::str::from_utf8(&buf[..len]).unwrap())?;
    assert!(matches!(msg, SipMessage::Request(ref r) if r.method == Method::Bye));

    let reason = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(DialogState::Terminated(_, r)) = states.recv().await {
                return r;
            }
        }
    })
    .await
    .expect("Terminated must not wait for the BYE's response");
    assert!(matches!(reason, TerminatedReason::UasBye));
    assert!(!bye.is_finished(), "the BYE transaction is still pending");
    bye.abort();
    token.cancel();
    Ok(())
}
