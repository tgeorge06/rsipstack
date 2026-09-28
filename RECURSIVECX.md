# rsipstack for recursivecx

This fork (`tgeorge06/rsipstack`) replaces recursivecx's `vendor/rsipstack`
(upstream 0.5.16 plus local patches, indexed in that directory's
`RECURSIVECX-PATCHES.md`). This file replaces that index. It has one section
per vendored patch: where the change lives in the fork now, which rcx code
depends on it, and its status:

- **carried**: ported from the vendored hunk on `feat/recursivecx-patches`.
- **in fork**: already on the fork's `main` (an earlier fork PR) before this port.
- **diverges**: present, but behaves differently from the vendored hunk (how is given).

The fork has one role-agnostic `InviteDialog` (`Dialog::Invite`). The old
`ClientInviteDialog` / `ServerInviteDialog` (`Dialog::ClientInvite` /
`Dialog::ServerInvite` in 0.5.16) remain as deprecated wrappers. Accessors
that rcx calls are on `InviteDialog`, and on the matching deprecated wrapper
too.

## 1. Sticky dialog transport on the 2xx Contact (rcx PR #537): carried, small divergence

- **Fork:** `DialogInner::adopt_2xx_remote_target` (`src/dialog/dialog.rs`),
  called by `InviteDialog::update_remote_target_from_2xx` (answered INVITE and
  the BYE of a 2xx after CANCEL) and by the deprecated
  `ClientInviteDialog::process_invite`.
- **What:** when the remote target has a non-UDP `;transport=` and the 2xx
  Contact names none, the parameter is carried onto the adopted target. A
  `sips` Contact keeps TLS through its scheme.
- **Diverges:** only non-UDP transports are carried. The vendor also appended
  `;transport=UDP` to UDP calls, which made no difference on the wire.
- **rcx:** every outbound TCP/TLS trunk leg's in-dialog BYE, re-INVITE and UPDATE.
- **Test:** `dialog::tests::test_sticky_transport`.

## 2. Transport-matched Via for in-dialog requests (rcx PR #537): in fork

- **Fork:** `DialogInner::make_request_with_vias` / `via_addr_for_send_transport`
  (`src/dialog/dialog.rs`), fork PR #5. It also fixes the Via on the
  server-dialog dial-back retry.
- **rcx:** the same TCP/TLS trunk legs as patch 1.
- **Test:** `dialog::tests::test_in_dialog_via`.

## 3. `DialogState::Calling` notified after the transport write: carried

- **Fork:** `InviteDialog::process_invite` and deprecated
  `ClientInviteDialog::process_invite` register `Transaction::on_first_write`
  (`src/transaction/transaction.rs`) before the send.
- **What:** `Calling` is notified exactly once, on the INVITE's first
  successful transport write, whichever send makes it. The fork's
  `Transaction::send` returns `Ok` even when no connection was found (Timer A
  retries the lookup) or a stream write failed (a synthetic 503 follows),
  where the vendor returned `Err`. So the signal comes from the transaction:
  either the first `send()` or a Timer A retransmission. An INVITE that never
  reaches the wire notifies nothing.
- **rcx:** queue delivery `ringing` (`sip_session/queue/delivery.rs`
  `note_queue_invite_on_wire`, `reap_abandoned_offer_evidence`;
  `queue/dialing.rs`), `sip_session/dialing.rs`, RWI `CallRinging` and
  `park::dial_ring::note_invite_sent` (`rwi/processor/originate.rs`),
  `rwi/transfer.rs`.
- **Test:** `dialog::tests::test_calling_after_send` (first send; first write
  by a Timer A retransmission, notified once; never written).

## 4. Re-INVITE ACK correlation and answer surfacing (rcx PR #769): carried, diverges

- **Fork:** `pub enum ReinviteAck { Received { cseq, body }, TimedOut { cseq } }`,
  `DialogInner.reinvite_ack`, `DialogInner::await_reinvite_ack` (`src/dialog/dialog.rs`),
  and `InviteDialog::take_reinvite_ack()` (also on the deprecated `ServerInviteDialog`).
  `InviteDialog::last_remote_ack()` (fork PR #8) is kept and still updated.
- **What:** an ACK is accepted only when its CSeq equals the re-INVITE's; a
  stale or mismatched ACK is ignored and the wait goes on. `Received` is
  stored before `Confirmed` is notified.
- **No ACK within 64*T1: the session ends (RFC 3261 §13.3.1.4, applied to
  re-INVITEs by §14.2).** The re-INVITE takes the same path as an initial
  INVITE (`DialogInner::end_session_without_ack`, fork PR #11), on UAS and
  UAC dialogs alike:
  1. `ReinviteAck::TimedOut { cseq }` is stored, so a consumer can tell why.
  2. `Terminated(Timeout)` is notified.
  3. A BYE is sent. It is built before the notification and sent after it,
     so `Terminated` does not wait for the BYE's response.

  **No `Confirmed` is notified** for the timed-out re-INVITE. The dialog was
  confirmed before and stays so until it terminates, but consumers read a
  `Confirmed` after a re-INVITE as a finished renegotiation, and this one
  never finished.
- **Diverges:**
  - From 0.5.16 and from rcx PR #769, which kept the call up on `TimedOut`.
    The vendor's `TimedOut` was effectively unreachable anyway: its server
    INVITE transaction ended at T4 (5 s), and `cleanup` had already dropped
    `last_response`. In the fork, the 2xx is retransmitted (T1 doubling,
    capped at T2) until 64*T1, about 32 s, and then the call is torn down.
    rcx's "restore the previous negotiation on `TimedOut`" branch in
    `settle_caller_reoffer` no longer runs for a timeout; `Terminated(Timeout)`
    follows instead.
  - The unified handler also runs for client-role dialogs (and the
    deprecated `ClientInviteDialog`'s now shares it). A callee's re-INVITE to
    rcx notifies `Confirmed` after its ACK, or ends the call when no ACK
    comes. The vendor's `ClientInviteDialog` did neither.
- **rcx:** `sip_session/dialog_events.rs` (`Confirmed` arm → `take_reinvite_ack`;
  `Terminated` arm), `sip_session/caller_reoffer.rs` `settle_caller_reoffer`,
  `renegotiation_tests.rs`, `queue_external_ring_accept_e2e_test`.
- **Tests:**
  - `test_ack_answer`
  - `test_late_reinvite_ack`
  - `test_uas_ack_timeout`:
    - `test_unacked_reinvite_2xx_ends_the_session` (UAS)
    - `test_unacked_reinvite_2xx_ends_the_session_on_a_uac_dialog`
    - `test_mismatched_ack_does_not_stop_the_reinvite_teardown`
    - `test_acked_reinvite_keeps_the_call_up`
    - `test_2xx_retransmission_interval_doubles_up_to_t2`

## 5. Client-side 2xx ACK body and last-sent ACK (rcx PR #769): carried

- **Fork:** `Transaction.ack_body` / `Transaction.sent_ack` and `send_ack`
  (`src/transaction/transaction.rs`); `DialogInner.next_ack_body` /
  `last_sent_ack` and `send_dialog_request` (`src/dialog/dialog.rs`);
  `InviteDialog::set_next_ack_body` and `InviteDialog::last_sent_ack` (also on
  the deprecated `ClientInviteDialog`).
- **What:** the armed body goes into the next in-dialog INVITE's 2xx ACK, with
  `Content-Type: application/sdp` and a matching `Content-Length`. It is
  consumed once.
- **Diverges (small):** the armed body survives a 401/407 retry: only a 2xx
  ACK consumes it, and the authenticated retry's 2xx ACK carries it. The
  vendor lost it.
- **rcx:** `tests/helpers/rtc_media_ua.rs` (offerless re-INVITE answered in the
  ACK; a stale ACK re-sent raw).
- **Test:** `dialog::tests::test_uac_ack_body` (including a 401 and a 407 retry).

## 6. A CSeq-mismatched ACK never touches a server INVITE transaction (rcx PR #769): in fork

- **Fork:** `Transaction::on_received_request` (`Completed | Confirmed` + ACK
  arm), fork PR #1. Fork PR #10 also routes a 2xx ACK by dialog and CSeq
  (`waiting_ack_cseq`), so a late ACK reaches its own transaction.
- **rcx:** as patch 4.
- **Tests:** `transaction::tests::test_server_invite_ack`, `dialog::tests::test_late_reinvite_ack`.

## 7. The ACK for a client INVITE's final response logs its send failure (rcx PR #771): carried

- **Fork:** `Transaction::on_received_response` logs a `send_ack` error at
  WARN with the transaction key. `send_ack` itself already logged a failed
  write.
- **rcx:** operational trace only (queue agent leg "ACK then BYE").
- **Test:** none (logging only).

## 8. A 1xx to an in-dialog request never regresses a confirmed dialog to Early (rcx PR #771): carried, small divergence

- **Fork:** `DialogInner::send_dialog_request`, `Provisional` arm (fork PR #3,
  completed here). No transition and no `Early` notification unless the
  dialog can still be cancelled (Calling / Trying / Early).
- **Diverges (small):** the vendor gated on `!is_confirmed()`, so a server
  dialog in `WaitAck` could regress to `Early`. The fork keeps it in `WaitAck`.
  On a confirmed dialog the two behave the same.
- **rcx:** lease-expiry fence (`the_lease_expiry_fence_tears_the_agent_leg_down_while_the_loop_is_parked`)
  and the callee-state handlers that match `Early`.
- **Test:** `dialog::tests::test_in_dialog_provisional`.

## 9. `DialogLayer::take_dialog` (rcx PR #771): carried

- **Fork:** `DialogLayer::take_dialog` (`src/dialog/dialog_layer.rs`).
- **rcx:** `call/app/lease_revocation.rs`, `sip_session/callee_answer.rs`,
  `sip_session/setup.rs`, `rwi/processor/originate.rs` (Hangup arm, media
  timeout, confirmed teardown), `crates/rcx-call/src/sip.rs`.
- **Test:** `dialog_layer::test_take_dialog_hands_the_dialog_to_exactly_one_caller`.

## 10. One notification per applied dialog transition (rcx PR #771): in fork, diverges

- **Fork:** `DialogInner::transition` (fork PR #4). A lifecycle transition
  after `Terminated` is neither applied nor notified. Event-only states
  (`Updated`, `Notify`, `Info`, `Options`, `Refer`) are not gated: they are
  notified whatever the lifecycle state, as in the vendor.
- **Diverges:**
  - The ignored `WaitAck`-after-`Confirmed` transition is no longer notified
    (the vendor notified it). rcx does not depend on that. `WaitAck` only
    comes from `accept()`, and rcx does not accept an already-confirmed dialog.
  - Notification now follows the state change, under the state lock.
  - `Refer` is event-only (fork 333d015): the dialog stays `Confirmed`.
- **rcx:** the callee-state `Terminated` arm (`sip_session/dialog_events.rs`).
- **Test:** `dialog::tests::test_state_after_terminated`.

## 11. A 2xx that crosses our CANCEL is ACKed and BYE'd (ring-timeout PR): in fork + carried

- **Fork:** `DialogGuardForUnconfirmed` (`src/dialog/invitation.rs`), fork
  PRs #6 and #12. The guard's `dialog` / `finished` fields and
  `watch_taken_dialog` (the dialog taken by another owner) are carried here.
  `InviteDialog::bye_2xx_after_cancel` is used for the BYE.
- **Diverges:**
  - Fork PR #12: `cancel()` sends nothing before a provisional (RFC 3261
    §9.1); the vendor CANCELed at once. A dropped guard waits for the first
    provisional and CANCELs then, or ACKs and BYEs a 2xx.
  - As a result, an rcx taker's `hangup()` on a dialog still in `Calling` is
    a no-op. The guard, once dropped, abandons the INVITE itself: it notifies
    `Terminated(UacCancel)` and CANCELs on the first provisional.
- **rcx:** RWI originate `Hangup` arm, `rwi_originate_trunk_e2e_test`
  (`a_200_that_crosses_our_cancel_is_acked_and_byed`,
  `a_200_after_the_cancel_settle_window_is_still_acked_and_byed`).
- **Test:** `dialog::tests::test_cancel_2xx_race`.

## 12. `Response::received_from` (carrier-diagnostics PR): carried

- **Fork:** `sip::message::ReceivedFrom { source, request_destination }` and
  `Response.received_from`. `Endpoint::on_received_message` stamps `source`
  after the inspectors, and `Transaction::on_received_response` stamps
  `request_destination`. Never serialized, and `None` after a reparse.
- **rcx:** `callrecord/carrier_response.rs`, `callrecord/carrier_responder.rs`.
- **Test:** `transaction::tests::test_response_provenance`.

## SIP response provenance (`Response::synthetic`): carried, extended

- **Fork:** `Response.synthetic`, `true` on the Timer B/C 408s and on the
  fork's locally generated 503 for a failed stream write
  (`Transaction::on_stream_send_failure`). Never serialized.
- **rcx:** `callrecord/diagnostics.rs`, `callrecord/carrier_response.rs`.
- **Tests:** `test_response_provenance`, `test_stream_reconnect::test_send_failure_on_stream_is_reported_at_once`.

## 401/407 without credentials returns the challenge: in fork

- **Fork:** `InviteDialog::process_invite` (fork PR #2). The challenge is
  returned as the final response after `Terminated(ProxyAuthRequired)`.
- **rcx:** `sip_session/dialog_events.rs` (`ProxyAuthRequired`).
- **Test:** `dialog::tests::test_invite_auth_challenge`.

## Fork behavior restored to match 0.5.16

These are not vendored patches. The fork had diverged from 0.5.16 in ways
rcx depends on, so the 0.5.16 behavior was put back on this branch.

- **Raw SIP logging at DEBUG only.** UDP, stream and WebSocket raw
  send/receive logs (and the UDP Via-update failure dump) had moved to INFO;
  they are DEBUG again. Two WARN logs that carried a whole message (a
  WebSocket parse failure, the dialog layer's "failed to send request") now
  log only the length or the method at WARN, and the message at DEBUG. That
  is stricter than 0.5.16, which logged both at WARN. Test:
  `transport::tests::test_raw_message_log_level`.
- **Role-typed layer lookups.**
  - `DialogLayer::get_client_dialog_by_call_id` returns UAC dialogs only.
    With the unified `Dialog::Invite` it had also returned the inbound
    (UAS) leg of a transparent-Call-ID call, so a caller could BYE or send
    INFO on the caller's dialog.
  - `get_or_create_server_invite` matches existing UAS dialogs only.
  - `get_dialog`, `match_dialog`, `take_dialog` and `remove_dialog` were
    role-agnostic in 0.5.16 too.
  - rcx callers: `rwi/processor/originate.rs` (Hangup arm, media timeout,
    transfer, DTMF).
  - Test: `dialog_layer::test_lookups_keep_uac_and_uas_dialogs_apart`.
- **BYE lifecycle** (`DialogInner::send_bye`, used by `InviteDialog` and
  both deprecated wrappers).
  - A UAS notifies `Terminated(UasBye)` before sending the BYE and returns
    the send result.
  - A UAC notifies `Terminated(UacBye)` after the BYE transaction, whatever
    its outcome (a failed send is logged, `Ok` returned).
  - Before this, `Terminated` waited for a successful BYE transaction.
  - Still diverges (upstream 0.6.x): `bye()` on a dialog that is not
    confirmed (a UAS may also be in `WaitAck`) and not terminated returns
    `Err`, where 0.5.16 returned `Ok(())` silently.
  - rcx: the `Terminated` arms that empty the callee dialogs and finish
    shutdown (`sip_session/dialog_events.rs`).
  - Test: `dialog::tests::test_bye_lifecycle`.
- **Call-ID shape.** `CallIdFormat::Random22` (`EndpointOption::callid_format`)
  generates the 0.5.x Call-ID exactly: 22 random mixed-case ASCII
  alphanumerics, `@`, then `callid_suffix` (default `restsend.com`). The
  fork's default is `UuidWithSuffix` (32 hex digits). rcx sets `Random22`
  explicitly.

## Other fork behavior rcx will observe (not vendored patches)

- A server INVITE 2xx is retransmitted until 64*T1 (Timer K removed for
  that case, fork PR #11). Following RFC 3261 §13.3.1.4, an INVITE whose 2xx
  is never ACKed ends with `Terminated(Timeout)` + BYE. That covers the
  initial INVITE and, since `fix/rfc3261-reinvite-ack-timeout`, a re-INVITE
  on either role (see patch 4). The call is no longer kept up.
- `Response` has two new public fields. Struct literals must set
  `synthetic: false, received_from: None` (rcx's tests already do).
- `Dialog::ClientInvite` / `Dialog::ServerInvite` are now `Dialog::Invite(InviteDialog)`.
  Code that needs the role checks `InviteDialog::role()`.
