//! A UAC that receives an offer in a 2xx must answer in the ACK (RFC 3261
//! §14.2). `set_next_ack_body` arms that answer for the next in-dialog
//! INVITE only, and `last_sent_ack` exposes the ACK the dialog sent.
use super::test_in_dialog_provisional::{establish, recv_request};
use crate::sip::{prelude::HeadersExt, Header, Method, Request};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

const OFFER: &str = "v=0\r\no=uas 1 2 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 4000 RTP/AVP 0\r\n";
const ANSWER: &str = "v=0\r\no=uac 1 2 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 5000 RTP/AVP 0\r\n";

/// Answer an offerless re-INVITE with a 200 carrying `OFFER`.
async fn reply_200_with_offer(peer: &UdpSocket, to: std::net::SocketAddr, req: &Request) {
    let contact = format!("sip:bob@{}", peer.local_addr().unwrap());
    let msg = format!(
        "SIP/2.0 200 OK\r\n\
         Via: {}\r\n\
         From: {}\r\n\
         To: {}\r\n\
         Call-ID: {}\r\n\
         CSeq: {}\r\n\
         Contact: <{contact}>\r\n\
         Content-Type: application/sdp\r\n\
         Content-Length: {}\r\n\r\n{OFFER}",
        req.via_header().unwrap().value(),
        req.from_header().unwrap().value(),
        req.to_header().unwrap().value(),
        req.call_id_header().unwrap().value(),
        req.cseq_header().unwrap().value(),
        OFFER.len(),
    );
    peer.send_to(msg.as_bytes(), to).await.unwrap();
}

fn content_type(req: &Request) -> Option<String> {
    req.headers.iter().find_map(|h| match h {
        Header::ContentType(ct) => Some(ct.value().to_string()),
        _ => None,
    })
}

fn content_length(req: &Request) -> Option<String> {
    req.headers.iter().find_map(|h| match h {
        Header::ContentLength(cl) => Some(cl.value().to_string()),
        _ => None,
    })
}

#[tokio::test]
async fn test_armed_ack_body_answers_the_offer_in_the_reinvite_2xx() -> crate::Result<()> {
    let token = CancellationToken::new();
    let (dialog, _states, peer) = establish(&token).await?;
    assert!(dialog.last_sent_ack().is_none(), "no re-INVITE ACK yet");

    // Offerless re-INVITE: the 200 carries the offer, the ACK the answer.
    dialog.set_next_ack_body(Some(ANSWER.as_bytes().to_vec()));
    let requester = dialog.clone();
    let pending = tokio::spawn(async move { requester.reinvite(None, None).await });
    let (req, uac) = recv_request(&peer, Method::Invite).await;
    assert!(req.body.is_empty(), "the re-INVITE is offerless");
    let reinvite_cseq = req.cseq_header()?.seq()?;
    reply_200_with_offer(&peer, uac, &req).await;
    let (ack, _) = recv_request(&peer, Method::Ack).await;
    let resp = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("re-INVITE did not complete")
        .expect("re-INVITE task panicked")?
        .expect("no final response");
    assert_eq!(resp.body, OFFER.as_bytes());

    assert_eq!(ack.body, ANSWER.as_bytes(), "the ACK carries the answer");
    assert_eq!(content_type(&ack).as_deref(), Some("application/sdp"));
    assert_eq!(
        content_length(&ack).as_deref(),
        Some(ANSWER.len().to_string().as_str())
    );
    assert_eq!(ack.cseq_header()?.seq()?, reinvite_cseq);

    let sent = dialog
        .last_sent_ack()
        .expect("the dialog keeps the ACK it sent");
    assert_eq!(sent.cseq_header()?.seq()?, reinvite_cseq);
    assert_eq!(sent.body, ANSWER.as_bytes());

    // The body is consumed by that one request: the next ACK is bodiless.
    let requester = dialog.clone();
    let pending = tokio::spawn(async move { requester.reinvite(None, None).await });
    let (req, uac) = recv_request(&peer, Method::Invite).await;
    let second_cseq = req.cseq_header()?.seq()?;
    reply_200_with_offer(&peer, uac, &req).await;
    let (ack, _) = recv_request(&peer, Method::Ack).await;
    tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("re-INVITE did not complete")
        .expect("re-INVITE task panicked")?;
    assert!(ack.body.is_empty(), "an unarmed ACK carries no body");
    assert_eq!(
        dialog.last_sent_ack().unwrap().cseq_header()?.seq()?,
        second_cseq,
        "last_sent_ack follows the latest re-INVITE"
    );

    token.cancel();
    Ok(())
}
