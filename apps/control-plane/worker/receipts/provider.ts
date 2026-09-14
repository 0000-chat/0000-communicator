import {
  ReceiptEvidenceSchema,
  ReceiptOperationStatusSchema,
  type ReceiptEvidence,
  type ReceiptOperationStatus,
  type OutboundCapability,
} from "@communicator/contracts";
import { z } from "zod";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";

export type ReceiptRoute = {
  tenant_id: string;
  identity_id: string;
  account_id: string;
  connection_id: string;
  provider: "whatsapp";
  session_generation: string;
  gateway_route_id: string;
  bridge_instance_id: string;
  matrix_user_id: string;
  matrix_room_namespace: string;
  provider_login_id: string;
};

export type ReceiptDispatchPayload = {
  route: ReceiptRoute;
  membership_id?: string;
  actor_identity_id?: string;
  reservation_id?: string;
  capability?: OutboundCapability;
  request_hash?: string;
  operation_id: string;
  operation_created_at: string;
  conversation_id: string;
  message_id: string;
  matrix_room_id: string;
  matrix_event_id: string;
  receipt_position: string;
};

export type ReceiptAdapterResult = {
  status: ReceiptOperationStatus;
  matrix_stage: "unknown" | "accepted";
  bridge_stage: "unknown" | "observed";
  provider_stage: "unknown" | "confirmed";
  evidence: ReceiptEvidence[];
  failure_code?:
    | "authorization_revoked"
    | "provider_rejected"
    | "provider_unavailable"
    | "provider_timeout"
    | "provider_protocol_error"
    | "matrix_rejected"
    | undefined;
  failure_reason?: string | undefined;
};

export class ReceiptProviderError extends Error {
  constructor(
    readonly code: "unavailable" | "timeout" | "rejected" | "protocol",
    cause?: unknown,
  ) {
    super(code);
    this.name = "ReceiptProviderError";
    if (cause !== undefined)
      Object.defineProperty(this, "cause", { value: cause });
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

const runtimeEnvironment = (env: Cloudflare.Env) =>
  env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };

const responseSchema = z
  .object({
    status: ReceiptOperationStatusSchema,
    operation_id: z.string().trim().min(1).max(128),
    matrix_stage: z.enum(["unknown", "accepted"]),
    bridge_stage: z.enum(["unknown", "observed"]),
    provider_stage: z.enum(["unknown", "confirmed"]),
    evidence: z.array(ReceiptEvidenceSchema).max(6).default([]),
    failure_code: z
      .enum([
        "provider_rejected",
        "provider_unavailable",
        "provider_timeout",
        "provider_protocol_error",
        "matrix_rejected",
        "authorization_revoked",
      ])
      .optional(),
    failure_reason: z.string().trim().min(1).max(200).optional(),
  })
  .strict();

export interface ReceiptProvider {
  dispatch(input: ReceiptDispatchPayload): Promise<ReceiptAdapterResult>;
}

export class HttpWhatsAppReceiptProvider implements ReceiptProvider {
  private readonly baseUrl: string;
  private readonly sharedSecret: string;

  constructor(
    env: Cloudflare.Env,
    private readonly fetcher: GatewayFetch,
  ) {
    const runtime = runtimeEnvironment(env);
    this.baseUrl = (runtime.CONNECTION_GATEWAY_URL ?? "").replace(/\/$/u, "");
    this.sharedSecret = runtime.CONNECTION_GATEWAY_TOKEN ?? "";
  }

  async dispatch(input: ReceiptDispatchPayload): Promise<ReceiptAdapterResult> {
    if (this.baseUrl.length === 0 || this.sharedSecret.length < 16)
      throw new ReceiptProviderError("unavailable");
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}/v1/receipts/read`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": crypto.randomUUID(),
          "idempotency-key": `receipt-${input.operation_id}`,
        },
        body: JSON.stringify({
          schema_version: 1,
          tenant_id: input.route.tenant_id,
          identity_id: input.route.identity_id,
          account_id: input.route.account_id,
          connection_id: input.route.connection_id,
          membership_id: input.membership_id,
          actor_identity_id: input.actor_identity_id,
          provider: input.route.provider,
          session_generation: input.route.session_generation,
          route: input.route,
          operation_id: input.operation_id,
          reservation_id: input.reservation_id,
          capability: input.capability,
          request_hash: input.request_hash,
          operation_created_at: input.operation_created_at,
          conversation_id: input.conversation_id,
          message_id: input.message_id,
          matrix_room_id: input.matrix_room_id,
          matrix_event_id: input.matrix_event_id,
          receipt_position: input.receipt_position,
        }),
      });
    } catch (error) {
      throw new ReceiptProviderError("unavailable", error);
    }
    if (response.status === 408 || response.status === 504)
      throw new ReceiptProviderError("timeout");
    let value: unknown;
    try {
      value = await response.json();
    } catch (error) {
      throw new ReceiptProviderError("protocol", error);
    }
    const parsed = responseSchema.safeParse(value);
    if (!parsed.success)
      throw new ReceiptProviderError("protocol", parsed.error);
    if (parsed.data.operation_id !== input.operation_id)
      throw new ReceiptProviderError("protocol");
    if (!response.ok && parsed.data.status !== "unknown")
      throw new ReceiptProviderError("rejected");
    return parsed.data;
  }
}

export const defaultWhatsAppReceiptProvider = (
  env: Cloudflare.Env,
): ReceiptProvider => {
  try {
    return new HttpWhatsAppReceiptProvider(env, gatewayFetchFromEnv(env));
  } catch (error) {
    if (error instanceof GatewayTransportConfigError) {
      return {
        dispatch: async () => {
          throw new ReceiptProviderError("unavailable");
        },
      };
    }
    throw error;
  }
};
