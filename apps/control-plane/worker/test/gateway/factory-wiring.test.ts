import { describe, expect, it, vi } from "vitest";
import { sha256Hex } from "../../archive/codec";
import {
  attachmentProviderFromEnv,
  type AttachmentProviderInput,
} from "../../attachments/provider";
import {
  defaultContactProvider,
  type ContactProviderInput,
  type ContactRoute,
} from "../../contacts/provider";
import {
  ConnectionGatewayError,
  gatewayFromEnv,
  type GatewayOwner,
} from "../../linking/gateway-client";
import {
  defaultGroupProvider,
  type GroupProviderInput,
} from "../../groups/provider";
import {
  defaultWhatsAppReceiptProvider,
  type ReceiptDispatchPayload,
} from "../../receipts/provider";
import type { GatewayServiceBinding } from "../../gateway/private-fetch";

const gatewayUrl = "https://gateway.example.test";
const gatewayToken = "gateway-secret-0123456789";

type TestBinding = GatewayServiceBinding & { calls: number };

const environment = (binding: unknown): Cloudflare.Env =>
  ({
    CONNECTION_GATEWAY_URL: gatewayUrl,
    CONNECTION_GATEWAY_TOKEN: gatewayToken,
    CONNECTION_GATEWAY_VPC: binding,
  }) as unknown as Cloudflare.Env;

const bindingFor = (
  handler: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>,
): TestBinding => {
  let binding!: TestBinding;
  binding = {
    calls: 0,
    async fetch(this: TestBinding, input, init) {
      expect(this).toBe(binding);
      this.calls += 1;
      return handler(input, init);
    },
  };
  return binding;
};

const bodyOf = (init: RequestInit | undefined): Record<string, unknown> =>
  JSON.parse(String(init?.body)) as Record<string, unknown>;

const route: ContactRoute = {
  tenant_id: "tenant_pilot",
  identity_id: "identity_human",
  account_id: "account_human",
  connection_id: "connection_human_whatsapp",
  provider: "whatsapp",
  session_generation: "2026-09-14T00:00:00.000Z",
  gateway_route_id: "gateway_route_human",
  bridge_instance_id: "bridge-whatsapp",
  matrix_user_id: "@human:example.test",
  matrix_room_namespace: "communicator.0000.gold",
  provider_login_id: "login-primary",
};

const contactInput: ContactProviderInput = {
  route,
  operation_id: "contact_factory_operation",
  idempotency_key: "contact-factory-key",
};

const groupInput: GroupProviderInput = {
  route,
  operation_id: "group_factory_operation",
  conversation_id: "conversation_group_factory",
  idempotency_key: "group-factory-key",
};

const receiptInput: ReceiptDispatchPayload = {
  route: { ...route, provider: "whatsapp" },
  membership_id: "membership_human",
  actor_identity_id: "identity_human",
  reservation_id: "receipt_factory_reservation",
  capability: {
    kind: "account_grant",
    grant_id: "grant_receipt_factory",
    authorization_epoch: 2,
  },
  request_hash: "a".repeat(64),
  operation_id: "receipt_factory_operation",
  operation_created_at: "2026-09-14T00:00:00.000Z",
  conversation_id: "conversation_receipt_factory",
  message_id: "message_receipt_factory",
  matrix_room_id: "!receipt-factory:example.test",
  matrix_event_id: "$receipt-factory:example.test",
  receipt_position: "$receipt-factory:example.test",
};

const gatewayOwner: GatewayOwner = {
  session_id: "session_factory",
  tenant_id: "tenant_pilot",
  actor_principal_id: "principal_human",
  membership_id: "membership_human",
  target_identity_id: "identity_human",
  provider: "whatsapp",
  generation: 1,
};

const attachmentBytes = new TextEncoder().encode("factory attachment");

const attachmentInput = async (): Promise<AttachmentProviderInput> => {
  const sha256 = await sha256Hex(attachmentBytes);
  return {
    tenant_id: "tenant_pilot",
    account_id: "account_human",
    connection_id: "connection_human_whatsapp",
    identity_id: "identity_human",
    conversation_id: "conversation_attachment_factory",
    message_id: "message_attachment_factory",
    attachment_id: "attachment_factory",
    revision: "event_attachment_factory",
    provider: "whatsapp",
    media_key: `media/tenant_pilot/${sha256}`,
    expected_size_bytes: attachmentBytes.byteLength,
    expected_sha256: sha256,
    expected_mime_type: "image/png",
  };
};

const absentBindings = [
  ["missing", undefined],
  ["malformed", { fetch: "not-a-function" }],
] as const;

describe("provider factories use the private gateway binding", () => {
  it("routes the connection factory through the binding", async () => {
    const binding = bindingFor(async (input, init) => {
      expect(input).toBe(`${gatewayUrl}/v1/link-sessions/start`);
      expect(init?.headers).toMatchObject({
        authorization: `Bearer ${gatewayToken}`,
        "content-type": "application/json",
      });
      expect(bodyOf(init)).toMatchObject(gatewayOwner);
      return new Response(
        JSON.stringify({
          gateway_ref: "gateway_factory_ref",
          action: "scan_qr",
          qr: "qr-factory",
          action_expires_at: null,
        }),
        { status: 200 },
      );
    });

    await expect(
      gatewayFromEnv(environment(binding)).start(gatewayOwner),
    ).resolves.toEqual({
      gateway_ref: "gateway_factory_ref",
      action: "scan_qr",
      qr: "qr-factory",
      action_expires_at: null,
    });
    expect(binding.calls).toBe(1);
  });

  it.each(absentBindings)(
    "maps %s connection binding configuration to provider_unavailable",
    (label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        expect(() => gatewayFromEnv(environment(binding))).toThrowError(
          new ConnectionGatewayError("provider_unavailable"),
        );
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("routes the attachment factory through the binding", async () => {
    const input = await attachmentInput();
    const binding = bindingFor(async (request, init) => {
      expect(request).toBe(`${gatewayUrl}/v1/attachments/read`);
      expect(init?.headers).toMatchObject({
        authorization: `Bearer ${gatewayToken}`,
        "content-type": "application/json",
      });
      expect(bodyOf(init)).toMatchObject(input);
      return new Response(
        JSON.stringify({
          status: "available",
          bytes_base64: btoa(String.fromCharCode(...attachmentBytes)),
          mime_type: "image/png",
          size_bytes: attachmentBytes.byteLength,
          sha256: input.expected_sha256,
        }),
        { status: 200 },
      );
    });

    await expect(
      attachmentProviderFromEnv(environment(binding)).read(input),
    ).resolves.toMatchObject({
      status: "available",
      mime_type: "image/png",
      sha256: input.expected_sha256,
    });
    expect(binding.calls).toBe(1);
  });

  it.each(absentBindings)(
    "maps %s attachment binding configuration to unavailable",
    async (_label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        await expect(
          attachmentProviderFromEnv(environment(binding)).read(
            await attachmentInput(),
          ),
        ).resolves.toEqual({
          status: "unavailable",
          reason: "provider_unavailable",
        });
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("routes the contact factory through the binding", async () => {
    const binding = bindingFor(async (request, init) => {
      expect(request).toBe(`${gatewayUrl}/v1/contacts/resolve/%2B15550000001`);
      expect(init?.headers).toMatchObject({
        authorization: `Bearer ${gatewayToken}`,
        "content-type": "application/json",
      });
      expect(bodyOf(init)).toMatchObject({
        operation_id: contactInput.operation_id,
        tenant_id: route.tenant_id,
        account_id: route.account_id,
        route: {
          gateway_route_id: route.gateway_route_id,
          provider_login_id: route.provider_login_id,
        },
      });
      return new Response(
        JSON.stringify({
          id: "contact_alex",
          name: "Alex",
          identifiers: ["+15550000001", "alex@lid"],
        }),
        { status: 200 },
      );
    });

    await expect(
      defaultContactProvider(environment(binding)).resolve(
        contactInput,
        "+15550000001",
      ),
    ).resolves.toMatchObject({
      provider_id: "contact_alex",
      current_lid: "alex@lid",
      display_name: "Alex",
    });
    expect(binding.calls).toBe(1);
  });

  it.each(absentBindings)(
    "maps %s contact binding configuration to unavailable",
    async (_label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        await expect(
          defaultContactProvider(environment(binding)).resolve(
            contactInput,
            "+15550000001",
          ),
        ).rejects.toMatchObject({ code: "unavailable" });
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("routes the group factory through the binding", async () => {
    const binding = bindingFor(async (request, init) => {
      expect(request).toBe(`${gatewayUrl}/v1/groups/create`);
      expect(init?.headers).toMatchObject({
        authorization: `Bearer ${gatewayToken}`,
        "content-type": "application/json",
      });
      expect(bodyOf(init)).toMatchObject({
        operation_id: groupInput.operation_id,
        name: "Alex Room",
        participants: ["provider_alex"],
      });
      return new Response(
        JSON.stringify({
          provider_group_id: "provider_group_alex",
          matrix_room_id: "!group-alex:example.test",
          name: "Alex Room",
          participant_provider_ids: ["provider_alex"],
          evidence: {
            source: "provider",
            evidence_id: "group-factory-evidence",
            observed_at: "2026-09-14T00:00:00.000Z",
            operation_id: groupInput.operation_id,
            account_id: route.account_id,
            connection_id: route.connection_id,
            participant_provider_ids: ["provider_alex"],
            status: "confirmed",
          },
        }),
        { status: 200 },
      );
    });

    await expect(
      defaultGroupProvider(environment(binding)).createGroup(
        groupInput,
        "Alex Room",
        ["provider_alex"],
      ),
    ).resolves.toMatchObject({
      provider_group_id: "provider_group_alex",
      matrix_room_id: "!group-alex:example.test",
      participant_provider_ids: ["provider_alex"],
    });
    expect(binding.calls).toBe(1);
  });

  it.each(absentBindings)(
    "maps %s group binding configuration to unavailable",
    async (_label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        await expect(
          defaultGroupProvider(environment(binding)).createGroup(
            groupInput,
            "Alex Room",
            ["provider_alex"],
          ),
        ).rejects.toMatchObject({ code: "unavailable" });
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it("routes the receipt factory through the binding", async () => {
    const binding = bindingFor(async (request, init) => {
      expect(request).toBe(`${gatewayUrl}/v1/receipts/read`);
      expect(init?.headers).toMatchObject({
        authorization: `Bearer ${gatewayToken}`,
        "content-type": "application/json",
      });
      expect(bodyOf(init)).toMatchObject({
        operation_id: receiptInput.operation_id,
        matrix_event_id: receiptInput.matrix_event_id,
        route,
      });
      return new Response(
        JSON.stringify({
          status: "accepted",
          operation_id: receiptInput.operation_id,
          matrix_stage: "accepted",
          bridge_stage: "unknown",
          provider_stage: "unknown",
          evidence: [
            {
              source: "matrix",
              status: "accepted",
              evidence_id: "matrix:receipt-factory",
            },
          ],
        }),
        { status: 200 },
      );
    });

    await expect(
      defaultWhatsAppReceiptProvider(environment(binding)).dispatch(
        receiptInput,
      ),
    ).resolves.toMatchObject({
      status: "accepted",
      operation_id: receiptInput.operation_id,
    });
    expect(binding.calls).toBe(1);
  });

  it.each(absentBindings)(
    "maps %s receipt binding configuration to unavailable",
    async (_label, binding) => {
      const globalFetch = vi.fn();
      vi.stubGlobal("fetch", globalFetch);
      try {
        await expect(
          defaultWhatsAppReceiptProvider(environment(binding)).dispatch(
            receiptInput,
          ),
        ).rejects.toMatchObject({ code: "unavailable" });
        expect(globalFetch).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );
});
