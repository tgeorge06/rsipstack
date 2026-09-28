//! RFC 3261 §13.3.1.4: a UAS retransmits its 2xx to an INVITE (T1, doubling)
//! until the ACK arrives. If no ACK arrives within 64*T1, the session SHOULD
//! be ended with a BYE. The dialog must not stay in `WaitAck` forever. The
//! same holds for a re-INVITE (§14.2), on a UAS and on a UAC dialog.
use crate::dialog::{
    dialog::{Dialog, DialogState, DialogStateReceiver, ReinviteAck, TerminatedReason},
    dialog_layer::DialogLayer,
    invite_dialog::InviteDialog,
};
use crate::sip::{prelude::HeadersExt, Method, SipMessage};
use crate::transaction::endpoint::EndpointOption;
use crate::transport::{udp::UdpConnection, TransportLayer};
use crate::EndpointBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

const CALL_ID: &str = "uas-ack-timeout-test";
const FROM_TAG: &str = "uac-tag";
const T1: Duration = Duration::from_millis(20);
// Same T4/T1 ratio as the RFC defaults (5 s / 500 ms).
const T4: Duration = Duration::from_millis(200);
const T1X64: Duration = Duration::from_millis(64 * 20);

/// A raw UAC peer.
struct Peer {
    socket: UdpSocket,
    uas: SocketAddr,
}

impl Peer {
    async fn send(&self, msg: String) {
        self.socket.send_to(msg.as_bytes(), self.uas).await.unwrap();
    }

    async fn send_request(&self, method: Method, cseq: u32, to_tag: Option<&str>) {
        let addr = self.socket.local_addr().unwrap();
        let to = match to_tag {
            Some(tag) => format!("<sip:bob@{}>;tag={tag}", self.uas),
            None => format!("<sip:bob@{}>", self.uas),
        };
        let branch = match method {
            Method::Ack => "z9hG4bK-ack".to_string(),
            _ => format!("z9hG4bK-{method}-{cseq}"),
        };
        self.send(format!(
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
        ))
        .await;
    }

    /// Everything the UAS sends until `deadline`, with arrival times. A BYE
    /// is answered with 200 OK.
    async fn collect(&self, deadline: Instant) -> Vec<(Instant, SipMessage)> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; 4096];
        loop {
            let now = Instant::now();
            if now >= deadline {
                return out;
            }
            let Ok(Ok((len, _))) =
                tokio::time::timeout(deadline - now, self.socket.recv_from(&mut buf)).await
            else {
                return out;
            };
            let text = std::str::from_utf8(&buf[..len]).unwrap();
            let Ok(msg) = SipMessage::try_from(text) else {
                continue;
            };
            if let SipMessage::Request(req) = &msg {
                if req.method == Method::Bye {
                    let resp = format!(
                        "SIP/2.0 200 OK\r\n\
                         Via: {}\r\nFrom: {}\r\nTo: {}\r\nCall-ID: {}\r\nCSeq: {}\r\n\
                         Content-Length: 0\r\n\r\n",
                        req.via_header().unwrap().value(),
                        req.from_header().unwrap().value(),
                        req.to_header().unwrap().value(),
                        req.call_id_header().unwrap().value(),
                        req.cseq_header().unwrap().value(),
                    );
                    self.send(resp).await;
                }
            }
            out.push((Instant::now(), msg));
        }
    }
}

/// A UAS endpoint with short timers, running the usual incoming-transaction
/// loop.
fn short_timers() -> EndpointOption {
    EndpointOption {
        t1: T1,
        t4: T4,
        t1x64: T1X64,
        ..Default::default()
    }
}

async fn setup(
    token: &CancellationToken,
    option: EndpointOption,
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
        .with_option(option)
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

fn is_invite_2xx(msg: &SipMessage) -> bool {
    match msg {
        SipMessage::Response(resp) => {
            resp.status_code.code() == 200
                && resp.cseq_header().unwrap().method().unwrap() == Method::Invite
        }
        _ => false,
    }
}

fn is_2xx_to(msg: &SipMessage, cseq: u32) -> bool {
    is_invite_2xx(msg)
        && matches!(msg, SipMessage::Response(resp) if resp.cseq_header().unwrap().seq().unwrap() == cseq)
}

#[tokio::test]
async fn test_unacked_2xx_is_retransmitted_then_the_session_is_ended() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;

    peer.send_request(Method::Invite, 1, None).await;
    let dialog = tokio::time::timeout(Duration::from_secs(2), dialogs.recv())
        .await
        .expect("timeout waiting for the server dialog")
        .unwrap();
    let accepted = Instant::now();
    dialog.accept(None, None)?;

    // The peer never ACKs.
    let messages = peer.collect(accepted + T1X64 * 2).await;

    let oks: Vec<Instant> = messages
        .iter()
        .filter(|(_, m)| is_invite_2xx(m))
        .map(|(at, _)| *at)
        .collect();
    assert!(
        oks.len() >= 6,
        "the 2xx must be retransmitted until 64*T1, got {} transmissions",
        oks.len()
    );
    let last = *oks.last().unwrap();
    assert!(
        last - accepted > T4 * 2 && last - accepted >= T1X64 / 2,
        "2xx retransmissions must go on until 64*T1, not stop at T4, last one after {:?}",
        last - accepted
    );
    let gaps: Vec<Duration> = oks.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(
        gaps.windows(2).all(|g| g[1] + T1 / 2 >= g[0]),
        "retransmission intervals must not shrink: {gaps:?}"
    );

    // The dialog ends, with a reason the application can tell apart.
    let mut terminated = None;
    while let Ok(state) = states.try_recv() {
        if let DialogState::Terminated(_, reason) = state {
            terminated = Some(reason);
        }
    }
    assert!(
        matches!(terminated, Some(TerminatedReason::Timeout)),
        "a 2xx without ACK for 64*T1 must terminate the dialog with Timeout, got {terminated:?}, state {}",
        dialog.state()
    );

    // The session is ended with a BYE for this dialog, sent no earlier than
    // 64*T1 after the 2xx.
    let ok_to_tag = messages
        .iter()
        .find_map(|(_, m)| match m {
            SipMessage::Response(resp) if is_invite_2xx(m) => Some(
                resp.to_header()
                    .unwrap()
                    .tag()
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_string(),
            ),
            _ => None,
        })
        .unwrap();
    let (bye_at, bye) = messages
        .iter()
        .find_map(|(at, m)| match m {
            SipMessage::Request(req) if req.method == Method::Bye => Some((*at, req)),
            _ => None,
        })
        .expect("the UAS must send a BYE when the ACK never arrives");
    assert_eq!(bye.call_id_header()?.value(), CALL_ID);
    assert_eq!(
        bye.from_header()?.tag()?.unwrap().value(),
        ok_to_tag,
        "the BYE must come from the dialog's local tag"
    );
    assert_eq!(bye.to_header()?.tag()?.unwrap().value(), FROM_TAG);
    assert!(
        bye_at - accepted >= T1X64 - T1,
        "the BYE must not be sent before 64*T1, sent after {:?}",
        bye_at - accepted
    );
    assert!(
        !oks.iter().any(|at| *at > bye_at),
        "no 2xx may be retransmitted after the BYE"
    );

    token.cancel();
    Ok(())
}

#[tokio::test]
async fn test_acked_2xx_stops_retransmitting_and_confirms() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;

    peer.send_request(Method::Invite, 1, None).await;
    let dialog = tokio::time::timeout(Duration::from_secs(2), dialogs.recv())
        .await
        .expect("timeout waiting for the server dialog")
        .unwrap();
    dialog.accept(None, None)?;

    let first = peer.collect(Instant::now() + T1 * 2).await;
    let to_tag = first
        .iter()
        .find_map(|(_, m)| match m {
            SipMessage::Response(resp) if is_invite_2xx(m) => Some(
                resp.to_header()
                    .unwrap()
                    .tag()
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_string(),
            ),
            _ => None,
        })
        .expect("the 2xx");
    peer.send_request(Method::Ack, 1, Some(&to_tag)).await;
    let acked = Instant::now();

    let after = peer.collect(acked + T1X64 * 2).await;
    assert!(
        !after
            .iter()
            .any(|(at, m)| is_invite_2xx(m) && *at > acked + T1 * 4),
        "no 2xx may be retransmitted once the ACK arrived"
    );
    assert!(
        !after.iter().any(|(_, m)| matches!(
            m,
            SipMessage::Request(req) if req.method == Method::Bye
        )),
        "an ACKed call must not be ended"
    );
    let mut seen = Vec::new();
    while let Ok(state) = states.try_recv() {
        seen.push(state.to_string());
    }
    assert!(
        seen.iter().any(|s| s.ends_with("(Confirmed)"))
            && !seen.iter().any(|s| s.ends_with("(Terminated)")),
        "the dialog must be confirmed and stay up, got {seen:?}"
    );
    assert!(dialog.state().is_confirmed());

    token.cancel();
    Ok(())
}

/// The interval between 2xx retransmissions doubles up to T2, then stays
/// there until 64*T1 (RFC 3261 §13.3.1.4, §17.2.1). Checked on the Timer G
/// the server INVITE transaction schedules, not on wall-clock arrival times
/// (which a loaded machine stretches).
#[tokio::test]
async fn test_2xx_retransmission_interval_doubles_up_to_t2() -> crate::Result<()> {
    use crate::transaction::{
        key::{TransactionKey, TransactionRole},
        transaction::{Transaction, TransactionEvent},
        TransactionState, TransactionTimer,
    };
    let t1 = Duration::from_millis(20);
    let t2 = t1 * 4;
    let token = CancellationToken::new();
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(TransportLayer::new(token.child_token()))
        .with_option(EndpointOption {
            t1,
            t2,
            ..short_timers()
        })
        .build();
    // The endpoint is not served: no timer fires on its own.
    let invite = crate::sip::Request::try_from(
        "INVITE sip:bob@127.0.0.1 SIP/2.0\r\n\
         Via: SIP/2.0/UDP 127.0.0.1:5070;branch=z9hG4bK-timer-g\r\n\
         From: <sip:alice@127.0.0.1>;tag=uac-tag\r\n\
         To: <sip:bob@127.0.0.1>\r\n\
         Call-ID: timer-g\r\n\
         CSeq: 1 INVITE\r\n\
         Content-Length: 0\r\n\r\n",
    )?;
    let key = TransactionKey::from_request(&invite, TransactionRole::Server)?;
    let mut tx = Transaction::new_server(key.clone(), invite, endpoint.inner.clone(), None);
    tx.state = TransactionState::Completed;

    let mut scheduled = Vec::new();
    let mut duration = t1;
    for _ in 0..6 {
        tx.tu_sender
            .send(TransactionEvent::Timer(TransactionTimer::TimerG(
                key.clone(),
                duration,
            )))
            .unwrap();
        // `receive` handles the timer, then waits for a message that never
        // comes.
        let _ = tokio::time::timeout(Duration::from_millis(20), tx.receive()).await;
        let next = tx.timer_g.take().expect("Timer G is restarted");
        let Some(TransactionTimer::TimerG(_, next)) = endpoint.inner.timers.cancel(next) else {
            panic!("the restarted timer is Timer G");
        };
        scheduled.push(next);
        duration = next;
    }
    assert_eq!(scheduled, vec![t1 * 2, t2, t2, t2, t2, t2]);
    token.cancel();
    Ok(())
}

/// Establish a call with the raw UAC peer (INVITE CSeq 1, 200, ACK) and
/// answer its re-INVITE CSeq 2 with a 200. Returns the dialog, the To tag and
/// when the re-INVITE was answered.
async fn answer_reinvite(
    states: &mut DialogStateReceiver,
    dialogs: &mut UnboundedReceiver<InviteDialog>,
    peer: &Peer,
) -> crate::Result<(InviteDialog, String, Instant)> {
    peer.send_request(Method::Invite, 1, None).await;
    let dialog = tokio::time::timeout(Duration::from_secs(2), dialogs.recv())
        .await
        .expect("timeout waiting for the server dialog")
        .unwrap();
    dialog.accept(None, None)?;
    let first = peer.collect(Instant::now() + T1 * 2).await;
    let local_tag = first
        .iter()
        .find_map(|(_, m)| match m {
            SipMessage::Response(resp) if is_2xx_to(m, 1) => Some(
                resp.to_header()
                    .unwrap()
                    .tag()
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_string(),
            ),
            _ => None,
        })
        .expect("the 2xx");
    peer.send_request(Method::Ack, 1, Some(&local_tag)).await;
    wait_confirmed(states).await;

    peer.send_request(Method::Invite, 2, Some(&local_tag)).await;
    let handle = loop {
        let state = tokio::time::timeout(Duration::from_secs(2), states.recv())
            .await
            .expect("timeout waiting for the re-INVITE")
            .expect("state channel closed");
        if let DialogState::Updated(_, _, handle) = state {
            break handle;
        }
    };
    let answered = Instant::now();
    handle.reply(crate::sip::StatusCode::OK).await.ok();
    Ok((dialog, local_tag, answered))
}

async fn wait_confirmed(states: &mut DialogStateReceiver) {
    loop {
        let state = tokio::time::timeout(Duration::from_secs(2), states.recv())
            .await
            .expect("timeout waiting for the call to be confirmed")
            .expect("state channel closed");
        if matches!(state, DialogState::Confirmed(..)) {
            return;
        }
    }
}

fn drain(states: &mut DialogStateReceiver) -> Vec<DialogState> {
    let mut seen = Vec::new();
    while let Ok(state) = states.try_recv() {
        seen.push(state);
    }
    seen
}

/// The re-INVITE teardown: its 2xx was retransmitted until 64*T1, then the
/// session ended with a BYE of this dialog and `Terminated(Timeout)`;
/// `ReinviteAck::TimedOut` tells why, and no `Confirmed` (a finished
/// renegotiation) was notified for the re-INVITE.
fn assert_reinvite_teardown(
    messages: &[(Instant, SipMessage)],
    seen: &[DialogState],
    answered: Instant,
    reinvite_cseq: u32,
    is_bye_of_dialog: impl Fn(&crate::sip::Request) -> bool,
) {
    let oks: Vec<Instant> = messages
        .iter()
        .filter(|(_, m)| is_2xx_to(m, reinvite_cseq))
        .map(|(at, _)| *at)
        .collect();
    assert!(
        oks.len() >= 2 && *oks.last().unwrap() - answered >= T1X64 / 2,
        "the re-INVITE 2xx must be retransmitted until 64*T1, got {} transmissions",
        oks.len()
    );
    let (bye_at, bye) = messages
        .iter()
        .find_map(|(at, m)| match m {
            SipMessage::Request(req) if req.method == Method::Bye => Some((*at, req)),
            _ => None,
        })
        .expect("a re-INVITE 2xx without ACK must end the session with a BYE");
    assert!(
        is_bye_of_dialog(bye),
        "the BYE must belong to the dialog: {bye}"
    );
    assert!(
        bye_at - answered >= T1X64 - T1,
        "the BYE must not be sent before 64*T1, sent after {:?}",
        bye_at - answered
    );
    assert!(
        matches!(
            seen.iter().rev().find_map(|s| match s {
                DialogState::Terminated(_, r) => Some(r),
                _ => None,
            }),
            Some(TerminatedReason::Timeout)
        ),
        "the dialog must terminate with Timeout, got {seen:?}"
    );
    assert!(
        !seen.iter().any(|s| matches!(
            s,
            DialogState::Confirmed(_, resp)
                if resp.cseq_header().unwrap().seq().unwrap() == reinvite_cseq
        )),
        "a timed-out re-INVITE must not be reported as Confirmed, got {seen:?}"
    );
}

/// RFC 3261 §13.3.1.4 applies to a re-INVITE too (§14.2): our UAS dialog
/// answers the caller's re-INVITE, the 2xx is never ACKed, and the session is
/// ended with a BYE and `Terminated(Timeout)`.
#[tokio::test]
async fn test_unacked_reinvite_2xx_ends_the_session() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;
    let (dialog, local_tag, answered) = answer_reinvite(&mut states, &mut dialogs, &peer).await?;

    // The peer never ACKs the re-INVITE's 2xx.
    let messages = peer.collect(answered + T1X64 * 2).await;
    let seen = drain(&mut states);
    assert_reinvite_teardown(&messages, &seen, answered, 2, |bye| {
        bye.from_header().unwrap().tag().unwrap().unwrap().value() == local_tag
            && bye.to_header().unwrap().tag().unwrap().unwrap().value() == FROM_TAG
    });
    assert_eq!(
        dialog.take_reinvite_ack(),
        Some(ReinviteAck::TimedOut { cseq: 2 })
    );
    assert!(dialog.state().is_terminated());
    token.cancel();
    Ok(())
}

/// An ACK that does not carry the re-INVITE's CSeq (here, a stale copy of the
/// initial INVITE's) acknowledges nothing: the timeout still ends the session.
#[tokio::test]
async fn test_mismatched_ack_does_not_stop_the_reinvite_teardown() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;
    let (dialog, local_tag, answered) = answer_reinvite(&mut states, &mut dialogs, &peer).await?;

    peer.collect(Instant::now() + T1 * 2).await;
    peer.send_request(Method::Ack, 1, Some(&local_tag)).await;
    let messages = peer.collect(answered + T1X64 * 2).await;
    let seen = drain(&mut states);
    assert_reinvite_teardown(&messages, &seen, answered, 2, |bye| {
        bye.from_header().unwrap().tag().unwrap().unwrap().value() == local_tag
    });
    assert_eq!(
        dialog.take_reinvite_ack(),
        Some(ReinviteAck::TimedOut { cseq: 2 })
    );
    token.cancel();
    Ok(())
}

/// A re-INVITE ACKed in time keeps the call up: `Received` then `Confirmed`,
/// retransmissions stop, no BYE, no termination.
#[tokio::test]
async fn test_acked_reinvite_keeps_the_call_up() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;
    let (dialog, local_tag, answered) = answer_reinvite(&mut states, &mut dialogs, &peer).await?;

    peer.collect(answered + T1 * 2).await;
    peer.send_request(Method::Ack, 2, Some(&local_tag)).await;
    let messages = peer.collect(answered + T1X64 * 2).await;
    assert!(
        !messages.iter().any(|(_, m)| matches!(
            m,
            SipMessage::Request(req) if req.method == Method::Bye
        )),
        "an ACKed re-INVITE must not end the session"
    );
    let seen = drain(&mut states);
    assert!(
        !seen
            .iter()
            .any(|s| matches!(s, DialogState::Terminated(..))),
        "got {seen:?}"
    );
    assert!(seen.iter().any(|s| matches!(
        s,
        DialogState::Confirmed(_, resp) if resp.cseq_header().unwrap().seq().unwrap() == 2
    )));
    assert_eq!(
        dialog.take_reinvite_ack(),
        Some(ReinviteAck::Received {
            cseq: 2,
            body: None
        })
    );
    assert!(dialog.state().is_confirmed());
    token.cancel();
    Ok(())
}

/// The same on our UAC dialog: the callee re-INVITEs us, never ACKs our 2xx,
/// and the session is ended with a BYE and `Terminated(Timeout)`.
#[tokio::test]
async fn test_unacked_reinvite_2xx_ends_the_session_on_a_uac_dialog() -> crate::Result<()> {
    use crate::dialog::invitation::InviteOption;
    let token = CancellationToken::new();
    let transport_layer = TransportLayer::new(token.child_token());
    let udp = UdpConnection::create_connection(
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(token.child_token()),
    )
    .await?;
    let uac: SocketAddr = udp.get_addr().get_socketaddr()?;
    transport_layer.add_transport(udp.into());
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(transport_layer)
        .with_cancel_token(token.child_token())
        .with_option(short_timers())
        .build();
    let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
    let mut incoming = endpoint.incoming_transactions()?;
    let endpoint_inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = endpoint_inner.serve().await;
    });
    let layer = dialog_layer.clone();
    tokio::spawn(async move {
        while let Some(mut tx) = incoming.recv().await {
            if let Some(mut dialog) = layer.match_dialog(&tx) {
                tokio::spawn(async move {
                    let _ = dialog.handle(&mut tx).await;
                });
            }
        }
    });

    // The callee: a raw UAS peer.
    let callee = UdpSocket::bind("127.0.0.1:0").await?;
    let callee_addr = callee.local_addr()?;
    let (state_sender, mut states) = unbounded_channel();
    let option = InviteOption {
        caller: crate::sip::Uri::try_from("sip:alice@example.com")?,
        callee: crate::sip::Uri::try_from(format!("sip:bob@{callee_addr}").as_str())?,
        contact: crate::sip::Uri::try_from(format!("sip:alice@{uac}").as_str())?,
        ..Default::default()
    };
    let layer = dialog_layer.clone();
    let invite = tokio::spawn(async move { layer.do_invite(option, state_sender).await });

    let mut buf = vec![0u8; 4096];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), callee.recv_from(&mut buf))
        .await
        .expect("the INVITE")?;
    let SipMessage::Request(inv) = SipMessage::try_from(std::str::from_utf8(&buf[..len]).unwrap())?
    else {
        panic!("expected the INVITE");
    };
    const CALLEE_TAG: &str = "callee-tag";
    let ok = format!(
        "SIP/2.0 200 OK\r\n\
         Via: {}\r\nFrom: {}\r\nTo: {};tag={CALLEE_TAG}\r\nCall-ID: {}\r\nCSeq: {}\r\n\
         Contact: <sip:bob@{callee_addr}>\r\nContent-Length: 0\r\n\r\n",
        inv.via_header()?.value(),
        inv.from_header()?.value(),
        inv.to_header()?.value(),
        inv.call_id_header()?.value(),
        inv.cseq_header()?.value(),
    );
    callee.send_to(ok.as_bytes(), uac).await?;
    let (dialog, _) = tokio::time::timeout(Duration::from_secs(2), invite)
        .await
        .expect("do_invite")
        .expect("do_invite task")?;
    assert!(dialog.state().is_confirmed());
    let local_tag = inv.from_header()?.tag()?.unwrap().value().to_string();
    let call_id = inv.call_id_header()?.value().to_string();

    // The callee re-INVITEs us; the application answers it.
    let reinvite = format!(
        "INVITE sip:alice@{uac} SIP/2.0\r\n\
         Via: SIP/2.0/UDP {callee_addr};branch=z9hG4bK-callee-reinvite\r\n\
         Max-Forwards: 70\r\n\
         From: <sip:bob@{callee_addr}>;tag={CALLEE_TAG}\r\n\
         To: <sip:alice@example.com>;tag={local_tag}\r\n\
         Call-ID: {call_id}\r\n\
         CSeq: 7 INVITE\r\n\
         Contact: <sip:bob@{callee_addr}>\r\n\
         Content-Length: 0\r\n\r\n"
    );
    callee.send_to(reinvite.as_bytes(), uac).await?;
    let handle = loop {
        let state = tokio::time::timeout(Duration::from_secs(2), states.recv())
            .await
            .expect("timeout waiting for the re-INVITE")
            .expect("state channel closed");
        if let DialogState::Updated(_, _, handle) = state {
            break handle;
        }
    };
    let answered = Instant::now();
    handle.reply(crate::sip::StatusCode::OK).await.ok();

    // The callee never ACKs. Collect what we send, answering the BYE.
    let peer = Peer {
        socket: callee,
        uas: uac,
    };
    let messages = peer.collect(answered + T1X64 * 2).await;
    let seen = drain(&mut states);
    assert_reinvite_teardown(&messages, &seen, answered, 7, |bye| {
        bye.from_header().unwrap().tag().unwrap().unwrap().value() == local_tag
            && bye.to_header().unwrap().tag().unwrap().unwrap().value() == CALLEE_TAG
    });
    assert_eq!(
        dialog.take_reinvite_ack(),
        Some(ReinviteAck::TimedOut { cseq: 7 })
    );
    assert!(dialog.state().is_terminated());
    token.cancel();
    Ok(())
}

/// Read SIP messages (no bodies) from `stream` until `deadline`.
async fn read_tcp_messages(
    stream: &mut tokio::net::TcpStream,
    buf: &mut Vec<u8>,
    deadline: Instant,
) -> Vec<(Instant, SipMessage)> {
    use tokio::io::AsyncReadExt;
    let mut out = Vec::new();
    let mut chunk = vec![0u8; 4096];
    loop {
        while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let text = String::from_utf8_lossy(&buf[..end + 4]).to_string();
            buf.drain(..end + 4);
            if let Ok(msg) = SipMessage::try_from(text.as_str()) {
                out.push((Instant::now(), msg));
            }
        }
        let now = Instant::now();
        if now >= deadline {
            return out;
        }
        match tokio::time::timeout(deadline - now, stream.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => return out,
        }
    }
}

/// RFC 3261 §13.3.1.4: the UAS core retransmits a 2xx on every transport,
/// reliable ones included (the 2xx can be lost at a later UDP hop), until
/// the ACK arrives.
#[tokio::test]
async fn test_2xx_over_tcp_is_retransmitted_until_the_ack() -> crate::Result<()> {
    use crate::transport::tcp_listener::TcpListenerConnection;
    use tokio::io::AsyncWriteExt;
    let token = CancellationToken::new();
    let transport_layer = TransportLayer::new(token.child_token());
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let uas: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let tcp = TcpListenerConnection::new(uas.into(), None).await?;
    transport_layer.add_transport(tcp.into());
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(transport_layer)
        .with_cancel_token(token.child_token())
        .with_option(short_timers())
        .build();
    endpoint.inner.transport_layer.serve_listens().await?;
    let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));
    let mut incoming = endpoint.incoming_transactions()?;
    let endpoint_inner = endpoint.inner.clone();
    tokio::spawn(async move {
        let _ = endpoint_inner.serve().await;
    });
    let (state_sender, mut states) = unbounded_channel();
    let (dialog_sender, mut dialogs) = unbounded_channel();
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

    let mut stream = tokio::net::TcpStream::connect(uas).await?;
    let local = stream.local_addr()?;
    let request = |method: Method, cseq: u32, to_tag: Option<&str>| {
        let to = match to_tag {
            Some(tag) => format!("<sip:bob@{uas}>;tag={tag}"),
            None => format!("<sip:bob@{uas}>"),
        };
        format!(
            "{method} sip:bob@{uas};transport=tcp SIP/2.0\r\n\
             Via: SIP/2.0/TCP {local};branch=z9hG4bK-tcp-{method}-{cseq}\r\n\
             Max-Forwards: 70\r\n\
             From: <sip:alice@{local}>;tag={FROM_TAG}\r\n\
             To: {to}\r\n\
             Call-ID: {CALL_ID}-tcp\r\n\
             CSeq: {cseq} {method}\r\n\
             Contact: <sip:alice@{local};transport=tcp>\r\n\
             Content-Length: 0\r\n\r\n"
        )
    };
    stream
        .write_all(request(Method::Invite, 1, None).as_bytes())
        .await?;
    let dialog = tokio::time::timeout(Duration::from_secs(2), dialogs.recv())
        .await
        .expect("timeout waiting for the server dialog")
        .unwrap();
    dialog.accept(None, None)?;

    // No ACK yet: the 2xx is retransmitted (T1, 2*T1, 4*T1, ...).
    let mut buf = Vec::new();
    let before_ack = read_tcp_messages(&mut stream, &mut buf, Instant::now() + T1 * 12).await;
    let oks: Vec<_> = before_ack.iter().filter(|(_, m)| is_2xx_to(m, 1)).collect();
    assert!(
        oks.len() >= 3,
        "a 2xx over TCP must be retransmitted until the ACK, got {} transmissions",
        oks.len()
    );
    let SipMessage::Response(ok) = &oks[0].1 else {
        unreachable!()
    };
    let to_tag = ok.to_header()?.tag()?.unwrap().value().to_string();

    stream
        .write_all(request(Method::Ack, 1, Some(&to_tag)).as_bytes())
        .await?;
    let acked = Instant::now();
    wait_confirmed(&mut states).await;
    let after_ack = read_tcp_messages(&mut stream, &mut buf, acked + T1 * 30).await;
    assert!(
        !after_ack
            .iter()
            .any(|(at, m)| is_invite_2xx(m) && *at > acked + T1 * 2),
        "the ACK must stop the 2xx retransmissions"
    );
    assert!(
        !after_ack.iter().any(|(_, m)| matches!(
            m,
            SipMessage::Request(req) if req.method == Method::Bye
        )),
        "an ACKed call must not be ended"
    );
    assert!(dialog.state().is_confirmed());
    token.cancel();
    Ok(())
}

/// Two re-INVITE transactions are outstanding. The first (CSeq 2) times out
/// and ends the session; the ACK of the second (CSeq 3) arrives after that.
/// The terminal `TimedOut { cseq: 2 }` must still be what the consumer reads.
#[tokio::test]
async fn test_reinvite_timeout_outcome_is_not_replaced_by_a_later_ack() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (mut states, mut dialogs, peer) = setup(&token, short_timers()).await?;
    let (dialog, local_tag, answered) = answer_reinvite(&mut states, &mut dialogs, &peer).await?;

    // Half-way through re-INVITE 2's wait, the peer sends re-INVITE 3.
    peer.collect(answered + T1X64 / 2).await;
    peer.send_request(Method::Invite, 3, Some(&local_tag)).await;
    let handle = loop {
        let state = tokio::time::timeout(Duration::from_secs(2), states.recv())
            .await
            .expect("timeout waiting for re-INVITE 3")
            .expect("state channel closed");
        if let DialogState::Updated(_, _, handle) = state {
            break handle;
        }
    };
    handle.reply(crate::sip::StatusCode::OK).await.ok();

    // Re-INVITE 2 times out: BYE and Terminated(Timeout).
    let messages = peer.collect(answered + T1X64 + T1 * 10).await;
    assert!(
        messages.iter().any(|(_, m)| matches!(
            m,
            SipMessage::Request(req) if req.method == Method::Bye
        )),
        "re-INVITE 2's missing ACK must end the session"
    );
    // Re-INVITE 3's transaction is still waiting; its ACK arrives now.
    peer.send_request(Method::Ack, 3, Some(&local_tag)).await;
    peer.collect(Instant::now() + T1 * 10).await;

    let seen = drain(&mut states);
    assert!(
        seen.iter()
            .any(|s| matches!(s, DialogState::Terminated(_, TerminatedReason::Timeout))),
        "got {seen:?}"
    );
    assert_eq!(
        dialog.take_reinvite_ack(),
        Some(ReinviteAck::TimedOut { cseq: 2 }),
        "the timeout that ended the session must not be replaced"
    );
    token.cancel();
    Ok(())
}
