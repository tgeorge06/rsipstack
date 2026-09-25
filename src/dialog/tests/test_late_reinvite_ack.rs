//! The ACK for a 2xx carries the CSeq number of the INVITE it acknowledges
//! and stops that 2xx's retransmissions (RFC 3261 §13.2.2.4, §13.3.1.4).
//! Over UDP the ACK of one re-INVITE can arrive after the next re-INVITE on
//! the same dialog: it must still reach its own transaction, and must not be
//! taken for the ACK of the newer one.
use crate::dialog::{
    dialog::{Dialog, DialogState, DialogStateReceiver},
    dialog_layer::DialogLayer,
    invite_dialog::InviteDialog,
};
use crate::sip::{prelude::HeadersExt, Method, Response, SipMessage, StatusCode};
use crate::transaction::endpoint::EndpointOption;
use crate::transport::{udp::UdpConnection, TransportLayer};
use crate::EndpointBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

const CALL_ID: &str = "late-reinvite-ack-test";
const FROM_TAG: &str = "uac-tag";

/// A raw UAC peer.
struct Peer {
    socket: UdpSocket,
    uas: SocketAddr,
}

impl Peer {
    async fn send_request(&self, method: Method, cseq: u32, branch: &str, to_tag: Option<&str>) {
        let addr = self.socket.local_addr().unwrap();
        let to = match to_tag {
            Some(tag) => format!("<sip:bob@{}>;tag={tag}", self.uas),
            None => format!("<sip:bob@{}>", self.uas),
        };
        let msg = format!(
            "{method} sip:bob@{uas} SIP/2.0\r\n\
             Via: SIP/2.0/UDP {addr};branch={branch}\r\n\
             Max-Forwards: 70\r\n\
             From: <sip:alice@{addr}>;tag={FROM_TAG}\r\n\
             To: {to}\r\n\
             Call-ID: {CALL_ID}\r\n\
             CSeq: {cseq} {method}\r\n\
             Contact: <sip:alice@{addr}>\r\n\
             Content-Length: 0\r\n\r\n",
            uas = self.uas,
        );
        self.socket.send_to(msg.as_bytes(), self.uas).await.unwrap();
    }

    /// Wait for the 2xx to the request with `cseq`, skipping anything else.
    async fn recv_2xx(&self, cseq: u32) -> Response {
        let mut buf = vec![0u8; 4096];
        loop {
            let (len, _) =
                tokio::time::timeout(Duration::from_secs(2), self.socket.recv_from(&mut buf))
                    .await
                    .expect("timeout waiting for a 2xx")
                    .unwrap();
            let text = std::str::from_utf8(&buf[..len]).unwrap();
            if let Ok(SipMessage::Response(resp)) = SipMessage::try_from(text) {
                let seq = resp.cseq_header().unwrap().seq().unwrap();
                if seq == cseq && resp.status_code == StatusCode::OK {
                    return resp;
                }
            }
        }
    }

    /// Count the 2xx (re)transmissions per CSeq received during `window`.
    async fn count_2xx(&self, window: Duration) -> std::collections::HashMap<u32, usize> {
        let mut counts = std::collections::HashMap::new();
        let mut buf = vec![0u8; 4096];
        let deadline = tokio::time::Instant::now() + window;
        while let Ok(Ok((len, _))) =
            tokio::time::timeout_at(deadline, self.socket.recv_from(&mut buf)).await
        {
            let text = std::str::from_utf8(&buf[..len]).unwrap();
            if let Ok(SipMessage::Response(resp)) = SipMessage::try_from(text) {
                if resp.status_code == StatusCode::OK {
                    let seq = resp.cseq_header().unwrap().seq().unwrap();
                    *counts.entry(seq).or_insert(0) += 1;
                }
            }
        }
        counts
    }
}

/// A UAS endpoint (short T1) running the usual incoming-transaction loop.
async fn setup(
    token: &CancellationToken,
) -> crate::Result<(DialogStateReceiver, UnboundedReceiver<InviteDialog>, Peer)> {
    let transport_layer = TransportLayer::new(token.child_token());
    let udp = UdpConnection::create_connection(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(token.child_token()),
    )
    .await?;
    let uas: SocketAddr = udp.get_addr().get_socketaddr()?;
    transport_layer.add_transport(udp.into());
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(transport_layer)
        .with_cancel_token(token.child_token())
        .with_option(EndpointOption {
            t1: Duration::from_millis(100),
            t1x64: Duration::from_millis(64 * 100),
            ..Default::default()
        })
        .build();
    let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
    let mut incoming = endpoint.incoming_transactions()?;
    let endpoint_inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = endpoint_inner.serve().await;
    });

    let (state_sender, states) = unbounded_channel();
    let (dialog_sender, dialogs) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(mut tx) = incoming.recv().await {
            let has_to_tag = tx.original.to_header().unwrap().tag().unwrap().is_some();
            let dialog = if has_to_tag {
                dialog_layer.match_dialog(&tx)
            } else if tx.original.method == Method::Invite {
                let dialog = dialog_layer
                    .get_or_create_server_invite(&tx, state_sender.clone(), None, None)
                    .expect("server dialog");
                dialog_sender.send(dialog.clone()).unwrap();
                Some(Dialog::Invite(dialog))
            } else {
                None
            };
            if let Some(mut dialog) = dialog {
                tokio::spawn(async move {
                    let _ = dialog.handle(&mut tx).await;
                });
            }
        }
    });

    let peer = Peer {
        socket: UdpSocket::bind("127.0.0.1:0").await?,
        uas,
    };
    Ok((states, dialogs, peer))
}

async fn next_state(rx: &mut DialogStateReceiver) -> DialogState {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout waiting for dialog state")
        .expect("state channel closed")
}

/// CSeq numbers of the responses carried by the `Confirmed` states in `rx`.
fn confirmed_cseqs(rx: &mut DialogStateReceiver) -> Vec<u32> {
    let mut seqs = Vec::new();
    while let Ok(state) = rx.try_recv() {
        if let DialogState::Confirmed(_, resp) = state {
            seqs.push(resp.cseq_header().unwrap().seq().unwrap());
        }
    }
    seqs
}

/// Answer the next re-INVITE the application is offered with a 200.
async fn accept_reinvite(states: &mut DialogStateReceiver) {
    loop {
        if let DialogState::Updated(_, _, handle) = next_state(states).await {
            handle.reply(StatusCode::OK).await.unwrap();
            return;
        }
    }
}

#[tokio::test]
async fn test_late_ack_of_previous_reinvite_reaches_its_own_transaction() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token).await?;

    // Establish the call: INVITE (CSeq 1), 200, ACK.
    peer.send_request(Method::Invite, 1, "z9hG4bK-invite-1", None)
        .await;
    let dialog = tokio::time::timeout(Duration::from_secs(2), dialogs.recv())
        .await
        .expect("timeout waiting for the server dialog")
        .unwrap();
    dialog.accept(None, None)?;
    let to_tag = peer
        .recv_2xx(1)
        .await
        .to_header()?
        .tag()?
        .expect("To tag")
        .value()
        .to_string();
    peer.send_request(Method::Ack, 1, "z9hG4bK-ack-1", Some(&to_tag))
        .await;
    while !matches!(next_state(&mut states).await, DialogState::Confirmed(..)) {}

    // re-INVITE CSeq 2 is answered; its ACK is delayed in the network.
    peer.send_request(Method::Invite, 2, "z9hG4bK-invite-2", Some(&to_tag))
        .await;
    accept_reinvite(&mut states).await;
    peer.recv_2xx(2).await;

    // The UAC, having sent that ACK, sends re-INVITE CSeq 3, which is
    // answered too.
    peer.send_request(Method::Invite, 3, "z9hG4bK-invite-3", Some(&to_tag))
        .await;
    accept_reinvite(&mut states).await;
    peer.recv_2xx(3).await;

    // Now the delayed ACK of CSeq 2 arrives.
    peer.send_request(Method::Ack, 2, "z9hG4bK-ack-2", Some(&to_tag))
        .await;
    let confirmed = loop {
        if let DialogState::Confirmed(_, resp) = next_state(&mut states).await {
            break resp.cseq_header()?.seq()?;
        }
    };
    assert_eq!(
        confirmed, 2,
        "the ACK of CSeq 2 must confirm re-INVITE 2, not re-INVITE 3"
    );
    // Let anything already in flight arrive, then watch the retransmissions.
    peer.count_2xx(Duration::from_millis(150)).await;
    let counts = peer.count_2xx(Duration::from_millis(1200)).await;
    assert_eq!(
        counts.get(&2).copied().unwrap_or(0),
        0,
        "the ACK of CSeq 2 must stop the retransmissions of its 200, got {counts:?}"
    );
    assert!(
        counts.get(&3).copied().unwrap_or(0) > 0,
        "the 200 to CSeq 3 is not acknowledged yet and must still be retransmitted, got {counts:?}"
    );
    assert_eq!(
        confirmed_cseqs(&mut states),
        Vec::<u32>::new(),
        "the ACK of CSeq 2 must confirm nothing else"
    );

    // The ACK of CSeq 3 still finds its transaction.
    peer.send_request(Method::Ack, 3, "z9hG4bK-ack-3", Some(&to_tag))
        .await;
    let confirmed = loop {
        if let DialogState::Confirmed(_, resp) = next_state(&mut states).await {
            break resp.cseq_header()?.seq()?;
        }
    };
    assert_eq!(confirmed, 3, "the ACK of CSeq 3 must confirm re-INVITE 3");
    peer.count_2xx(Duration::from_millis(150)).await;
    let counts = peer.count_2xx(Duration::from_millis(1000)).await;
    assert!(
        counts.is_empty(),
        "no 200 may be retransmitted once both are acknowledged, got {counts:?}"
    );
    assert_eq!(confirmed_cseqs(&mut states), Vec::<u32>::new());

    token.cancel();
    Ok(())
}
