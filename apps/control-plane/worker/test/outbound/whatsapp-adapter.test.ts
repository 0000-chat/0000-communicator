import { env as runtimeEnv } from "cloudflare:workers";
import {
  OutboundDispatchPayloadSchema,
  OutboundDispatchSchema,
  type SessionResponse,
} from "@communicator/contracts";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  defaultWhatsAppTextAdapter,
  HttpWhatsAppTextAdapter,
} from "../../outbound/whatsapp-adapter";
import type { OutboundAcceptanceContext } from "../../outbound/acceptance";
import {
  clearDirectory,
  seedAccountAccess,
  seedDirectory,
} from "../support/directory-fixtures";

const env = runtimeEnv as typeof runtimeEnv & { CONTROL_DB: D1Database };
const gatewayUrl = "https://matrix-gateway.internal.example.invalid";
const session: SessionResponse = {
  tenant: { id: "tenant_pilot", slug: "pilot", display_name: "Pilot" },
  principal: { id: "principal_human", type: "human", display_name: "Human" },
  membership: { id: "membership_human", role: "owner" },
  identities: [
    {
      identity_id: "identity_human",
      kind: "human",
      display_name: "Human",
      scopes: ["message.send", "conversation.read"],
    },
  ],
};

const payload = OutboundDispatchPayloadSchema.parse({
  schema_version: 1,
  tenant_id: "tenant_pilot",
  command_id: "command_adapter_test",
  dispatch_id: "dispatch_adapter_test",
  message_id: "message_adapter_test",
  event_id: "event_adapter_test",
  identity_id: "identity_human",
  resource_identity_id: "identity_human",
  account_id: "account_human",
  connection_id: "connection_human_whatsapp",
  conversation_id: "conversation_adapter_test",
  provider: "whatsapp",
  body: "hello from the controlled adapter",
  transaction_id: "transaction_adapter_test",
  request_digest: "a".repeat(64),
  projection_generation: 1,
  dispatch_lease_id: "lease_adapter_test",
  dispatch_lease_expires_at: "2026-09-14T01:00:30.000Z",
  body_digest: "b".repeat(64),
  authority: {
    reservation_id: "reservation_adapter_test",
    membership_id: "membership_human",
    identity_id: "identity_human",
    capability: {
      kind: "account_grant",
      grant_id: "grant_adapter_send",
      authorization_epoch: 1,
    },
  },
  created_at: "2026-09-14T00:00:00.000Z",
});

const dispatch = OutboundDispatchSchema.parse({
  id: payload.dispatch_id,
  tenant_id: payload.tenant_id,
  command_id: payload.command_id,
  message_id: payload.message_id,
  event_id: payload.event_id,
  actor_principal_id: "principal_human",
  actor_identity_id: payload.identity_id,
  resource_identity_id: payload.resource_identity_id,
  account_id: payload.account_id,
  connection_id: payload.connection_id,
  conversation_id: payload.conversation_id,
  idempotency_key: "adapter-idempotency",
  status: "dispatching",
  transaction_id: payload.transaction_id,
  request_digest: payload.request_digest,
  body_digest: payload.body_digest,
  authority: payload.authority,
  dispatch_lease_id: payload.dispatch_lease_id,
  dispatch_lease_expires_at: payload.dispatch_lease_expires_at,
  projection_generation: payload.projection_generation,
  matrix_stage: "unknown",
  bridge_stage: "unknown",
  provider_stage: "unknown",
  chat_paused: false,
  duplicate_risk: false,
  created_at: payload.created_at,
  updated_at: payload.created_at,
});

const context = (binding?: unknown): OutboundAcceptanceContext => ({
  env: {
    ...runtimeEnv,
    CONNECTION_GATEWAY_URL: gatewayUrl,
    CONNECTION_GATEWAY_TOKEN: "adapter-test-secret-123",
    ...(binding === undefined ? {} : { CONNECTION_GATEWAY_VPC: binding }),
  } as Cloudflare.Env,
  authorization: session,
});

const acceptedResponse = (overrides: Record<string, unknown> = {}) =>
  new Response(
    JSON.stringify({
      outcome: "accepted",
      transaction_id: payload.transaction_id,
      request_digest: payload.request_digest,
      account_id: payload.account_id,
      connection_id: payload.connection_id,
      session_generation: "2026-08-29T00:00:00.000Z",
      observed_at: "2026-09-14T00:00:01.000Z",
      evidence: [
        {
          source: "provider",
          status: "accepted",
          evidence_id: "provider_operation_adapter_test",
          provider_operation_id: "provider_operation_adapter_test",
          provider_message_id: "provider_message_adapter_test",
        },
      ],
      ...overrides,
    }),
    { status: 200, headers: { "content-type": "application/json" } },
  );

async function seedAdapterDirectory() {
  await clearDirectory(env.CONTROL_DB);
  await seedDirectory(env.CONTROL_DB);
  await seedAccountAccess(env.CONTROL_DB);
  await env.CONTROL_DB.batch([
    env.CONTROL_DB.prepare(
      "INSERT INTO connection_provider_identities (tenant_id, provider, identity_key, connection_id, link_session_id, created_at) VALUES (?, 'whatsapp', ?, ?, ?, ?)",
    ).bind(
      "tenant_pilot",
      "b".repeat(64),
      "connection_human_whatsapp",
      "link_adapter_test",
      "2026-08-29T00:00:00.000Z",
    ),
    env.CONTROL_DB.prepare(
      "INSERT INTO account_grants (id, tenant_id, membership_id, identity_id, account_id, operation_scope, chat_scope, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'message.send', 'all_chats', 'active', ?, ?)",
    ).bind(
      "grant_adapter_send",
      "tenant_pilot",
      "membership_human",
      "identity_human",
      "account_human",
      "2026-08-29T00:00:00.000Z",
      "2026-08-29T00:00:00.000Z",
    ),
  ]);
}

describe("HttpWhatsAppTextAdapter", () => {
  beforeEach(seedAdapterDirectory);

  it.each([
    ["missing", undefined],
    ["malformed", { fetch: "not-a-function" }],
  ] as const)(
    "does not construct a default adapter with %s binding",
    (_label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        expect(defaultWhatsAppTextAdapter(context(binding))).toBeUndefined();
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("routes the default factory through the private binding", async () => {
    const fetcher = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        expect(input).toBe(`${gatewayUrl}/v1/outbound/text`);
        expect(init?.headers).toMatchObject({
          authorization: "Bearer adapter-test-secret-123",
          "content-type": "application/json",
        });
        const requestBody = JSON.parse(String(init?.body)) as {
          account_id: string;
          connection_id: string;
          transaction_id: string;
          body: string;
        };
        expect(requestBody).toMatchObject({
          account_id: payload.account_id,
          connection_id: payload.connection_id,
          transaction_id: payload.transaction_id,
          body: payload.body,
        });
        return acceptedResponse();
      },
    );
    type TestBinding = {
      fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
    };
    let binding!: TestBinding;
    binding = {
      async fetch(this: TestBinding, input, init) {
        expect(this).toBe(binding);
        return fetcher(input, init);
      },
    };

    const adapter = defaultWhatsAppTextAdapter(context(binding));
    expect(adapter).toBeDefined();
    await expect(adapter!.dispatch(dispatch, payload)).resolves.toMatchObject({
      type: "evidence_batch",
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("routes one saved body to the selected account and preserves transaction idempotency", async () => {
    const requests: Request[] = [];
    const fetcher = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push(new Request(input, init));
        return acceptedResponse();
      },
    );
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toEqual({
      type: "evidence_batch",
      evidences: [
        expect.objectContaining({
          source: "provider",
          status: "accepted",
          provider_operation_id: "provider_operation_adapter_test",
          provider_message_id: "provider_message_adapter_test",
        }),
      ],
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(requests[0]?.url).toBe(`${gatewayUrl}/v1/outbound/text`);
    expect(requests[0]?.headers.get("idempotency-key")).toBe(
      "outbound-transaction_adapter_test",
    );
    const requestBody = (await requests[0]!.clone().json()) as {
      account_id: string;
      connection_id: string;
      transaction_id: string;
      body: string;
    };
    expect(requestBody).toMatchObject({
      account_id: "account_human",
      connection_id: "connection_human_whatsapp",
      transaction_id: "transaction_adapter_test",
      body: payload.body,
      body_digest: payload.body_digest,
      reservation_id: payload.authority?.reservation_id,
      membership_id: payload.authority?.membership_id,
      actor_identity_id: payload.authority?.identity_id,
      capability: payload.authority?.capability,
    });
  });

  it("rejects a payload whose account binding differs from the claimed dispatch", async () => {
    const fetcher = vi.fn(async () => acceptedResponse());
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);
    const mismatchedPayload = OutboundDispatchPayloadSchema.parse({
      ...payload,
      account_id: "account_agent",
    });

    const result = await adapter.dispatch(dispatch, mismatchedPayload);
    expect(result).toEqual({
      type: "failure",
      failure_code: "provider_protocol_error",
      reason: "dispatch_payload_mismatch",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("keeps accepted or delivered outcomes uncertain when stage evidence is absent", async () => {
    const fetcher = vi.fn(async () =>
      acceptedResponse({ evidence: undefined }),
    );
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toMatchObject({
      type: "uncertain",
      reason: "gateway_response_missing_stage_evidence",
    });
  });

  it("turns a malformed successful response into uncertainty after I/O", async () => {
    const fetcher = vi.fn(
      async () =>
        new Response(JSON.stringify({ outcome: "accepted" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toMatchObject({
      type: "uncertain",
      reason: "invalid_gateway_response_after_request",
    });
  });

  it("returns uncertainty on transport failure without retrying", async () => {
    const fetcher = vi.fn(async () => {
      throw new Error("timeout");
    });
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const first = await adapter.dispatch(dispatch, payload);
    const second = await adapter.dispatch(dispatch, payload);
    expect(first).toMatchObject({ type: "uncertain" });
    expect(second).toMatchObject({ type: "uncertain" });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it.each([
    [429, "rate_limited"],
    [403, "session_expired"],
    [404, "provider_rejected"],
  ] as const)("keeps gateway HTTP %s explicit", async (status, code) => {
    const fetcher = vi.fn(async () => new Response("{}", { status }));
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toMatchObject({ type: "failure", failure_code: code });
  });

  it("passes a revoked capability to the private gateway claim boundary", async () => {
    await env.CONTROL_DB.prepare(
      "UPDATE account_grants SET status = 'revoked', revoked_at = ?, updated_at = ? WHERE id = ?",
    )
      .bind(
        "2026-09-14T00:00:00.000Z",
        "2026-09-14T00:00:00.000Z",
        "grant_adapter_send",
      )
      .run();
    const fetcher = vi.fn(
      async () =>
        new Response(JSON.stringify({ reason: "authorization_revoked" }), {
          status: 403,
          headers: { "content-type": "application/json" },
        }),
    );
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toMatchObject({
      type: "failure",
      failure_code: "authorization_revoked",
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("blocks missing send capability and disconnected sessions without failover", async () => {
    await env.CONTROL_DB.batch([
      env.CONTROL_DB.prepare(
        "DELETE FROM connection_capabilities WHERE connection_id = ? AND capability = 'message.send'",
      ).bind("connection_human_whatsapp"),
    ]);
    const fetcher = vi.fn(async () => acceptedResponse());
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);
    const missingCapability = await adapter.dispatch(dispatch, payload);
    expect(missingCapability).toMatchObject({
      type: "failure",
      failure_code: "missing_capability",
    });
    expect(fetcher).not.toHaveBeenCalled();

    await env.CONTROL_DB.prepare(
      "UPDATE connections SET status = 'disconnected' WHERE id = ?",
    )
      .bind("connection_human_whatsapp")
      .run();
    const disconnected = await adapter.dispatch(dispatch, payload);
    expect(disconnected).toMatchObject({
      type: "failure",
      failure_code: "connection_unavailable",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("treats a missing provider identity as an expired session", async () => {
    await env.CONTROL_DB.prepare(
      "DELETE FROM connection_provider_identities WHERE connection_id = ?",
    )
      .bind("connection_human_whatsapp")
      .run();
    const fetcher = vi.fn(async () => acceptedResponse());
    const adapter = new HttpWhatsAppTextAdapter(context(), fetcher);

    const result = await adapter.dispatch(dispatch, payload);
    expect(result).toMatchObject({
      type: "failure",
      failure_code: "session_expired",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });
});
