//! A 2xx Contact usually names no transport. Adopting it verbatim as the
//! remote target would move a TCP/TLS call's in-dialog requests (BYE,
//! re-INVITE, UPDATE) to UDP; the established `;transport=` is carried over.
use crate::dialog::{dialog::DialogInner, DialogId};
use crate::sip::{Param, Request, Response, Transport, Uri};
use crate::transaction::key::TransactionRole;
use crate::transport::TransportLayer;
use crate::EndpointBuilder;
use tokio::sync::mpsc::unbounded_channel;
use tokio_util::sync::CancellationToken;

fn client_dialog(target: &str) -> crate::Result<DialogInner> {
    let token = CancellationToken::new();
    let endpoint = EndpointBuilder::new()
        .with_user_agent("rsipstack-test")
        .with_transport_layer(TransportLayer::new(token.child_token()))
        .build();
    let invite = Request::try_from(
        format!(
            "INVITE {target} SIP/2.0\r\n\
             Via: SIP/2.0/TCP 127.0.0.1:5060;branch=z9hG4bK-sticky\r\n\
             From: <sip:alice@example.com>;tag=alice-tag\r\n\
             To: <sip:bob@example.com>\r\n\
             Call-ID: sticky-transport\r\n\
             CSeq: 1 INVITE\r\n\
             Contact: <sip:alice@127.0.0.1:5060;transport=tcp>\r\n\
             Content-Length: 0\r\n\r\n"
        )
        .as_str(),
    )?;
    let (state_sender, _) = unbounded_channel();
    let (tu_sender, _) = unbounded_channel();
    DialogInner::new(
        TransactionRole::Client,
        DialogId {
            call_id: "sticky-transport".to_string(),
            local_tag: "alice-tag".to_string(),
            remote_tag: String::new(),
        },
        invite,
        endpoint.inner.clone(),
        state_sender,
        None,
        None,
        tu_sender,
    )
}

fn ok_with_contact(contact: &str) -> crate::Result<Response> {
    Ok(Response::try_from(
        format!(
            "SIP/2.0 200 OK\r\n\
             Via: SIP/2.0/TCP 127.0.0.1:5060;branch=z9hG4bK-sticky\r\n\
             From: <sip:alice@example.com>;tag=alice-tag\r\n\
             To: <sip:bob@example.com>;tag=bob-tag\r\n\
             Call-ID: sticky-transport\r\n\
             CSeq: 1 INVITE\r\n\
             Contact: <{contact}>\r\n\
             Content-Length: 0\r\n\r\n"
        )
        .as_str(),
    )?)
}

fn transport_of(uri: &Uri) -> Option<Transport> {
    uri.params.iter().find_map(|p| match p {
        Param::Transport(t) => Some(*t),
        _ => None,
    })
}

#[test]
fn test_2xx_contact_without_transport_keeps_the_established_transport() -> crate::Result<()> {
    let inner = client_dialog("sip:bob@192.0.2.10:5060;transport=tcp")?;
    inner.adopt_2xx_remote_target(&ok_with_contact("sip:bob@192.0.2.20:5070")?)?;
    let target = inner.remote_uri.lock().clone();
    assert_eq!(target.host_with_port.to_string(), "192.0.2.20:5070");
    assert_eq!(transport_of(&target), Some(Transport::Tcp));
    Ok(())
}

#[test]
fn test_2xx_contact_transport_and_sips_scheme_win() -> crate::Result<()> {
    let inner = client_dialog("sip:bob@192.0.2.10:5060;transport=tcp")?;
    inner.adopt_2xx_remote_target(&ok_with_contact("sip:bob@192.0.2.20:5070;transport=tls")?)?;
    assert_eq!(
        transport_of(&inner.remote_uri.lock()),
        Some(Transport::Tls),
        "a Contact that names its transport is adopted as is"
    );

    let inner = client_dialog("sip:bob@192.0.2.10:5060;transport=tls")?;
    inner.adopt_2xx_remote_target(&ok_with_contact("sips:bob@192.0.2.20:5071")?)?;
    assert_eq!(
        transport_of(&inner.remote_uri.lock()),
        None,
        "a sips Contact keeps TLS through its scheme"
    );
    Ok(())
}

#[test]
fn test_2xx_contact_without_transport_stays_bare_on_udp() -> crate::Result<()> {
    let inner = client_dialog("sip:bob@192.0.2.10:5060")?;
    inner.adopt_2xx_remote_target(&ok_with_contact("sip:bob@192.0.2.20:5070")?)?;
    assert_eq!(transport_of(&inner.remote_uri.lock()), None);
    Ok(())
}
