# rsipstack for recursivecx

This fork (`tgeorge06/rsipstack`) is upstream `restsend/rsipstack` plus the
patches recursivecx (rcx) still needs. The goal is an empty fork: every patch
below is either offered upstream or removed once rcx stops depending on it.
The row ids (R2, R3, ...) are those of the convergence ledger
(`rsipstack-contrib/convergence-ledger.md`).

Base: upstream **0.7.1** (3286e8c). Everything the fork used to carry that
0.7.1 contains (R1, R4-R11, R13, the R12 teardown) is upstream's version now.
`ReinviteAck` / `take_reinvite_ack()` (R14) is gone: rcx reads
`InviteDialog::last_remote_ack()` and matches its CSeq.

The fork has one role-agnostic `InviteDialog` (`Dialog::Invite`), as upstream.
The deprecated `ClientInviteDialog` / `ServerInviteDialog` wrappers carry the
same patches where they apply.

## Patches

### R2 + R19: a 1xx to an in-dialog request is not `Early`

- **Where:** `DialogInner::send_dialog_request`, `Provisional` arm.
- **What:** `Early` is applied and notified only while the dialog can still be
  cancelled (Calling / Trying / Early). A 1xx to a re-INVITE or UPDATE on a
  confirmed dialog neither regresses it (R2, upstream PR #147) nor notifies
  `Early` (R19), which rcx subscribers read as ringing.
- **rcx:** the callee-state handlers that match `Early`; the lease-expiry
  fence.
- **Test:** `dialog::tests::test_in_dialog_provisional`.

### R3: one notification per applied dialog transition

- **Where:** `DialogInner::transition`.
- **What:** the transition is decided and notified under the state lock.
  After `Terminated`, nothing more is applied or notified, a second
  `Terminated` included; a `WaitAck` ignored after `Confirmed` is not
  notified. Event-only states (`Updated`, `Notify`, `Info`, `Options`,
  `Refer`) are notified whatever the lifecycle state. Upstream PR #148.
- **rcx:** the callee-state `Terminated` arm (`sip_session/dialog_events.rs`).
- **Tests:** `dialog::tests::test_state_after_terminated`, the notification
  tests at the end of `dialog::tests::test_dialog_states`.

### R15: response provenance (`Response::synthetic`, `Response::received_from`)

- **Where:** `sip::message::{Response, ReceivedFrom}`. `synthetic` is `true` on
  the Timer B/C 408s and the local 503 for a failed stream write
  (`Transaction::on_stream_send_failure`). `Endpoint::on_received_message`
  stamps `received_from.source` after the inspectors;
  `Transaction::on_received_response` stamps `request_destination`. Never
  serialized; `None` / `false` after a reparse.
- **rcx:** `callrecord/carrier_response.rs`, `callrecord/diagnostics.rs`,
  `rcx-call/src/sip.rs`. Struct literals must set
  `synthetic: false, received_from: None`.
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

### R21: a 2xx crossing a taken dialog's CANCEL is BYE'd

- **Where:** `DialogGuardForUnconfirmed` (`src/dialog/invitation.rs`): the
  `dialog` / `finished` fields and `watch_taken_dialog`.
- **What:** when another owner took the dialog out of the layer
  (`take_dialog`) and the `do_invite` future is dropped, the guard keeps the
  INVITE transaction: in Trying / Early it watches for a 2xx (up to 64*T1)
  and BYEs it, without a second CANCEL; still in Calling (the owner's hangup
  could send no CANCEL), it abandons the INVITE as upstream does for a dialog
  still in the layer.
- **rcx:** RWI originate `Hangup` arm, `rwi_originate_trunk_e2e_test`.
- **Tests:** `test_cancel_2xx_race::test_taken_dialog_*`.

### R22a + R22b: raw SIP messages at DEBUG only

- **What:** UDP, stream and WebSocket raw send/receive logs are DEBUG
  (upstream: INFO). The WebSocket parse failure and the dialog layer's
  "failed to send request" WARNs carry only the length or the method; the
  message itself is logged at DEBUG.
- **Test:** `transport::tests::test_raw_message_log_level` (`bench` feature).

### R23: role-typed dialog lookups

- **What:** `DialogLayer::get_client_dialog_by_call_id` returns UAC dialogs
  only, and `get_or_create_server_invite` matches existing UAS dialogs only.
  With a transparent Call-ID the inbound (UAS) and outbound (UAC) legs of a
  proxied call share it.
- **rcx:** `rwi/processor/originate.rs` (Hangup, media timeout, transfer, DTMF).
- **Test:** `dialog_layer::test_lookups_keep_uac_and_uas_dialogs_apart`.

### R24: BYE lifecycle per role

- **Where:** `DialogInner::send_bye`, used by `InviteDialog` and both wrappers.
- **What:** a UAS notifies `Terminated(UasBye)` before sending the BYE and
  returns the send result; a UAC notifies `Terminated(UacBye)` after the BYE
  transaction whatever its outcome. Upstream terminates only after an `Ok`
  BYE.
- **rcx:** the `Terminated` arms in `sip_session/dialog_events.rs`.
- **Test:** `dialog::tests::test_bye_lifecycle`.

### R25: `CallIdFormat::Random22`

- **What:** the rsipstack 0.5.x Call-ID: 22 random mixed-case alphanumerics,
  `@`, then `callid_suffix`. rcx sets it in `proxy/server.rs`.
- **Test:** `transaction::tests::tests::test_make_call_id_random22`.

### R27: `Confirmed` carries the 2xx its ACK confirms

- **Where:** `Transaction::cleanup`: a server INVITE transaction keeps
  `last_response` (it still hands a copy to `finished_transactions`).
- **What:** since 0.7.1 the matching ACK ends an `Accepted` server INVITE
  transaction (RFC 6026 §7.1, upstream #169) before the dialog reads
  `tx.last_response` for `DialogState::Confirmed`. Upstream then notifies
  `Confirmed` with `Response::default()` (no CSeq, no headers), for the
  initial INVITE and every re-INVITE. rcx correlates `Confirmed` by the
  response's CSeq (`confirms_initial_invite`, `is_reconfirmation`, the
  re-offer settle).
- **Test:** `dialog::tests::test_late_reinvite_ack` (CSeq of each
  `Confirmed`).

## Upstream 0.7.1 behavior rcx observes (not fork patches)

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
