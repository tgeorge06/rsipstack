# rsipstack for recursivecx

This fork (`tgeorge06/rsipstack`) is upstream `restsend/rsipstack` plus the
patches recursivecx (rcx) still needs. The goal is an empty fork: every patch
below is either offered upstream or removed once rcx stops depending on it.
The row ids (R2, R3, ...) are those of the convergence ledger
(`rsipstack-contrib/convergence-ledger.md`).

Base: upstream **0.7.3** (2dfa0d1). Everything the fork used to carry that
0.7.3 contains is upstream's version now: R1, R4-R11, R13, the R12 teardown
(0.7.1), and R2, R3, R21, R22b, R23 (client lookup), the "BYE ends the dialog
whatever the outcome" part of R24, and R27 (0.7.3). `ReinviteAck` /
`take_reinvite_ack()` (R14) is gone: rcx reads
`InviteDialog::last_remote_ack()` and matches its CSeq. The R23 server-side
role check on `get_or_create_server_invite` was dropped: rcx routes
in-dialog requests through `match_dialog` / `get_dialog`.

The fork has one role-agnostic `InviteDialog` (`Dialog::Invite`), as upstream.
The deprecated `ClientInviteDialog` / `ServerInviteDialog` wrappers carry the
same patches where they apply.

## Patches

### R19: a 1xx to an in-dialog request is not notified as `Early`

- **Where:** `DialogInner::send_dialog_request`, `Provisional` arm.
- **What:** `Early` is applied and notified only while the dialog can still be
  cancelled (Calling / Trying / Early). Upstream (#147, R2) no longer
  regresses a confirmed dialog but still notifies the 1xx as `Early`; the
  fork does not notify it, since rcx subscribers read `Early` as ringing.
- **rcx:** the callee-state handlers that match `Early`; the lease-expiry
  fence.
- **Test:** `dialog::tests::test_in_dialog_provisional`.

### R15: response provenance (`Response::synthetic`, `Response::received_from`)

- **Where:** `sip::message::{Response, ReceivedFrom}`. `synthetic` is `true` on
  the Timer B/C 408s and the local 503 for a failed stream write
  (`Transaction::on_stream_send_failure`). `Endpoint::on_received_message`
  stamps `received_from.source` after the inspectors;
  `Transaction::on_received_response` stamps `request_destination`. Never
  serialized; `None` / `false` after a reparse.
- **rcx:** `callrecord/carrier_response.rs`, `callrecord/diagnostics.rs`,
  `rcx-call/src/sip.rs`. Struct literals must set
  `synthetic: false, received_from: None` (and `wire_reason: None`, R28).
- **Tests:** `transaction::tests::test_response_provenance`,
  `test_stream_reconnect::test_send_failure_on_stream_is_reported_at_once`.

### R16: the UAC's 2xx ACK body (`set_next_ack_body`, `last_sent_ack`)

- **Where:** `Transaction.ack_body` / `sent_ack` and `send_ack`;
  `DialogInner.next_ack_body` / `last_sent_ack` and `send_dialog_request`;
  `InviteDialog::set_next_ack_body` / `last_sent_ack` (also on
  `ClientInviteDialog`).
- **What:** the armed body goes into the next in-dialog INVITE's 2xx ACK, with
  `Content-Type: application/sdp`. It survives a 401/407 retry: only a 2xx ACK
  consumes it.
- **rcx:** `tests/helpers/rtc_media_ua.rs` only (offerless re-INVITE answered
  in the ACK).
- **Test:** `dialog::tests::test_uac_ack_body`.

### R17: `Calling` is notified once the INVITE is written

- **Where:** `Transaction::on_first_write`; `InviteDialog::process_invite` and
  `ClientInviteDialog::process_invite` register it before the send.
- **What:** `Calling` is notified exactly once, on the INVITE's first
  successful transport write: the first `send()` or a Timer A
  retransmission. An INVITE that never reaches the wire notifies nothing.
- **rcx:** queue delivery `ringing` (`sip_session/queue/delivery.rs`,
  `queue/dialing.rs`), `sip_session/dialing.rs`, RWI `CallRinging` and
  `park::dial_ring::note_invite_sent` (`rwi/processor/originate.rs`),
  `rwi/transfer.rs`.
- **Test:** `dialog::tests::test_calling_after_send`.

### R18: a 2xx Contact keeps the established non-UDP transport

- **Where:** `DialogInner::adopt_2xx_remote_target`, called by
  `InviteDialog::update_remote_target_from_2xx` and
  `ClientInviteDialog::process_invite`.
- **What:** when the remote target has a non-UDP `;transport=` and the 2xx
  Contact names none, the parameter is carried onto the adopted target (a
  `sips` Contact keeps TLS through its scheme).
- **rcx:** in-dialog BYE / re-INVITE / UPDATE on TCP and TLS trunk legs.
- **Test:** `dialog::tests::test_sticky_transport`.

### R20: `DialogLayer::take_dialog`

- **What:** remove and return the dialog in one map operation, so exactly one
  of several tasks owns its teardown.
- **rcx:** `call/app/lease_revocation.rs`, `sip_session/callee_answer.rs`,
  `sip_session/setup.rs`, `rwi/processor/originate.rs`,
  `crates/rcx-call/src/sip.rs`.
- **Test:** `dialog_layer::test_take_dialog_hands_the_dialog_to_exactly_one_caller`.

### R22a: raw SIP messages at DEBUG only

- **What:** UDP, stream and WebSocket raw send/receive logs are DEBUG
  (upstream: INFO). The WARNs without the message (R22b) are upstream's.
- **Tests:** `transport::tests::test_raw_message_log_level` (`bench` feature);
  upstream's `test_warn_logs` checks the WebSocket receive log at DEBUG.

### R24: a UAS notifies `Terminated` before sending its BYE

- **Where:** `DialogInner::send_bye`.
- **What:** a UAS notifies `Terminated(UasBye)` before the BYE is sent, so
  subscribers never wait for the BYE's response, and returns the send
  result. Upstream (0.7.3) notifies after the BYE transaction for both roles;
  the UAC side is upstream's (`Terminated(UacBye)` whatever the outcome, the
  error still returned).
- **rcx:** the `Terminated` arms in `sip_session/dialog_events.rs`.
- **Test:** `dialog::tests::test_bye_lifecycle`.

### R25: `CallIdFormat::Random22`

- **What:** the rsipstack 0.5.x Call-ID: 22 random mixed-case alphanumerics,
  `@`, then `callid_suffix`. rcx sets it in `proxy/server.rs`.
- **Test:** `transaction::tests::tests::test_make_call_id_random22`.

### R28: `Response::wire_reason`

- **Where:** `sip::message::Response`, `sip::parser`.
- **What:** a known status code whose Status-Line carries a phrase other than
  the standard one keeps it in `wire_reason` (e.g. `403 Caller Origination
  Number is Invalid`). Set only by the parser; `Display` is unchanged.
- **rcx:** `crates/rcx-callrecord/src/carrier_response.rs` (rcx #1135).
- **Test:** `sip::parser` tests for the custom phrase.

## Upstream behavior rcx observes (not fork patches)

- RFC 6026 `Accepted` state. Server: the 2xx is retransmitted by Timer G
  (T1 doubling to T2, every transport) until the matching ACK, which ends the
  transaction, or Timer L (64*T1). A 2xx that is never ACKed ends the session
  (`Terminated(Timeout)` + BYE), for an INVITE or a re-INVITE. Client: every
  retransmitted or forked 2xx is re-ACKed until Timer M; `do_invite` keeps
  receiving the transaction until then.
- A 2xx ACK is routed to the INVITE transaction with the same CSeq (#170).
- A dropped INVITE sends no CANCEL before a provisional response; it CANCELs
  on the first provisional, or ACKs and BYEs a 2xx (#162, #171).
- A forked 2xx is ACKed with its own To tag and remote target (#172).
- 0.7.3: a BYE ends the dialog whatever its transaction returns (#180); a
  forked 2xx's dialog is BYE'd (#181); an in-dialog REFER returns the dialog
  to `Confirmed` once answered; Timer F ends a non-INVITE client
  transaction in Proceeding (#189); a server in-dialog request with no route
  and no dial-back fails at once (#187); the deprecated `ClientInviteDialog`
  ends the session on a never-ACKed re-INVITE 2xx (#185); a dropped INVITE
  whose dialog was already removed still ends (#183); injectable TLS client
  seam (rustls stays the default).
