# WhatsApp provider boundaries

Evidence is from the current branch at `cd0642eb5e6dcc0ab46e6c832ce39d5accf50b7a`,
the current source commits named below, and preserved refs listed in the
[migration handoff](../migration/2026-09-12-communicator-migration.md#ref-preservation).
This report records code and plans. It does not choose the pilot's outbound
product route.

## What exists

The shared contract already has provider identity and connection state. A
connection carries `provider`, `identity_id`, `status`, and a capability list;
the provider enum includes WhatsApp, Telegram, Messenger, and LinkedIn
([connection.ts](../../packages/contracts/src/connection.ts#L4-L42),
commit `d688d22a3e4c3da91742658a09fa5dd1840a04a`). The canonical envelope
requires tenant, identity, platform, account, conversation, optional Matrix
room/event IDs, optional remote message ID, timestamps, and a typed payload
([canonical-event.ts](../../packages/contracts/src/canonical-event.ts#L249-L272),
commit `2835a03036e7592fa82232cf3eae4f879faffed5`). Projection payloads cover
messages, edits/deletes, reactions, read/delivery receipts, typing,
attachments, participants, conversation updates, command updates, and bridge
delivery updates ([projection.ts](../../packages/contracts/src/projection.ts#L287-L401),
commit `1b6bb80d3180bad3e99dcf77dd62bcd8904686b0`).

The preserved Matrix gateway is implemented and provider-neutral. Its persisted
room binding stores `tenant_id`, `identity_id`, `connection_id`, `account_id`,
`platform`, `conversation_id`, and the owning Matrix user
([registry.rs](../../services/matrix-gateway/src/registry.rs#L24-L42), commit
`77cf2acafd4da23115ba683331c88e7f1101342b`). The normalizer validates the
active room, derives stable participant/message/relation IDs, copies the
binding tuple into each canonical event, and maps the owner’s Matrix message
to outbound with `delivery_status: sent`; other senders are inbound/unknown
([normalize.rs](../../services/matrix-gateway/src/normalize.rs#L840-L904),
[normalize.rs](../../services/matrix-gateway/src/normalize.rs#L1046-L1170),
commit `34c118a785b1ebb62ba7a15e037163412c250d8d`). This is a receive-only
Matrix-to-Cloudflare courier. The canonical outbound acceptance decision is
tracked in [Decide WhatsApp pilot acceptance](https://github.com/0000-chat/0000-communicator/issues/3).

WhatsApp, Messenger, and Telegram have concrete deployment seams in the
current tree: pinned images and separate internal Compose services
([image locks](../../deploy/images.lock.env#L1-L6),
[compose services](../../compose.yaml#L35-L86)). Each has a protected config,
registration, database/runtime initializer, policy validator, readiness check,
and no published bridge port. WhatsApp's renderer currently authorizes Human,
Agent, and Platform Admin, disables relay/provisioning/backfill, and requires
Matrix encryption ([WhatsApp config](../../scripts/render-whatsapp-config.py#L12-L80),
commit `b699d9da22bd8a995b8a43aac0fa2c107c9ca4d7`). Messenger and Telegram
reuse the same deployment pattern but have provider-specific ports, commands,
portal policy, split portals, and receipt/status settings
([Messenger config](../../scripts/render-messenger-config.py#L24-L80),
[Telegram config](../../scripts/render-telegram-config.py#L33-L81)).

## What is simulated or missing

The control-plane UI's default path is explicitly simulated and performs no
provider action ([environment banner](../../apps/control-plane/src/components/layout/environment-banner.tsx#L3-L19)). Its store fabricates provider
connections, conversations, messages, and accepted `message.send` commands
([mock store](../../apps/control-plane/src/mocks/store.ts#L27-L179)); the MSW
handler returns `202` for those commands ([mock handler](../../apps/control-plane/src/mocks/handlers.ts#L177-L200)). The live Worker currently
registers session, identity, connection, channel, conversation, and message
reads plus internal ingestion, with no product command route
([Worker routes](../../apps/control-plane/worker/app.ts#L141-L171),
[read routes](../../apps/control-plane/worker/routes/read.ts#L79-L153)).

`CommandSchema` only models `message.send` and lifecycle statuses, so a real
provider path still needs command authorization, durable idempotency, Matrix
submission, remote submission, and `command.updated`/
`bridge.delivery.updated` producers ([command.ts](../../packages/contracts/src/command.ts#L4-L35)).
The reviewed Matrix gateway normalizes outbound Matrix events but does not call
WhatsApp, Messenger, Telegram, or LinkedIn APIs. No provider-specific source
adapter was found in the reviewed paths that maps remote events into the
canonical envelope. The reviewed paths store/read provider capability arrays
and status values, but no implementation was found there that updates them
from bridge health or remote login state. These are findings from the reviewed
paths, not an exhaustive repository audit.

## Smallest boundary changes

1. Reuse the existing bridge-to-Matrix-to-gateway normalizer and explicit room
   binding. Add only narrow provider/account mappings, capability/status
   updates, or remote-ID mapping where the reviewed provider evidence requires
   them. Do not add a direct-provider event-ingest framework.
2. Add one authenticated command endpoint and durable command/outbox worker.
   Resolve `(tenant, identity, connection, account, conversation, platform)`
   from the directory, submit through the Matrix portal/bridge, then emit the
   existing command and bridge-delivery events. Do not widen `CommandSchema`
   until a second operation is required.
3. Treat the current WhatsApp/Messenger/Telegram scripts as deployment
   adapters. LinkedIn remains preserved work, not current-tree work: ref
   `refs/migration/source/heads/codex/linkedin-bridge` is
   `9ded5379fc909c7c9d7e06751e1fab252967c8b3`, with service/config/policy,
   health, backup, and restore commits (`672f0e3`, `1df31fc`, `c848c50`,
   `59001e4`). Reuse its operational pattern only after integrating it with
   the shared event and command boundaries.

The main rewrite risk is putting provider-specific IDs, capability quirks, or
remote delivery states into UI/mocks or Matrix room IDs. Keep those at the
existing bridge/gateway boundary and map them to the existing canonical fields.
