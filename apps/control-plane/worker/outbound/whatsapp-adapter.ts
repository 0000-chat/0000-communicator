import {
  OutboundEvidenceInputSchema,
  OutboundDispatchPayloadSchema,
  ProviderSchema,
  TimestampSchema,
  type OutboundDispatch,
  type OutboundDispatchPayload,
} from "@communicator/contracts";
import type {
  OutboundAcceptanceContext,
  OutboundAdapterEvidence,
  OutboundAdapterResult,
} from "./acceptance";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";
import { z } from "zod";

const runtimeEnvironment = (env: Cloudflare.Env) =>
  env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };

const connectionRowSchema = z
  .object({
    tenant_id: z.string().min(1),
    connection_id: z.string().min(1),
    identity_id: z.string().min(1),
    provider: ProviderSchema,
    account_id: z.string().min(1),
    status: z.enum([
      "connected",
      "syncing",
      "ready",
      "attention_required",
      "disconnected",
      "revoked",
      "unlinked",
    ]),
    session_generation: TimestampSchema,
    gateway_route_id: z.string().min(1),
    bridge_instance_id: z.string().min(1),
    matrix_user_id: z.string().min(1),
    matrix_room_namespace: z.string().min(1),
    has_send_capability: z.number().int().min(0).max(1),
    has_provider_identity: z.number().int().min(0).max(1),
  })
  .strict();

const evidenceSchema = z
  .object({
    source: z.enum(["matrix", "bridge", "provider", "refresh"]),
    status: z.enum(["confirmed", "accepted", "delivered", "uncertain"]),
    evidence_id: z.string().trim().min(1).max(200),
    observed_at: TimestampSchema.optional(),
    reason: z.string().trim().min(1).max(200).optional(),
    provider_operation_id: z.string().trim().min(1).max(200).optional(),
    provider_message_id: z.string().trim().min(1).max(200).optional(),
    remote_echo_id: z.string().trim().min(1).max(200).optional(),
  })
  .strict();

const responseSchema = z
  .object({
    outcome: z.enum([
      "accepted",
      "delivered",
      "uncertain",
      "rejected",
      "rate_limited",
      "session_expired",
      "missing_capability",
    ]),
    transaction_id: z.string().min(1),
    request_digest: z.string().regex(/^[0-9a-f]{64}$/),
    account_id: z.string().min(1),
    connection_id: z.string().min(1),
    session_generation: TimestampSchema,
    observed_at: TimestampSchema,
    evidence: z.array(evidenceSchema).max(4).optional(),
    provider_operation_id: z.string().trim().min(1).max(200).optional(),
    provider_message_id: z.string().trim().min(1).max(200).optional(),
    remote_echo_id: z.string().trim().min(1).max(200).optional(),
    reason: z.string().trim().min(1).max(200).optional(),
  })
  .strict();

type ConnectionRow = z.infer<typeof connectionRowSchema>;
type WhatsAppResponse = z.infer<typeof responseSchema>;

const usableConnection = (status: ConnectionRow["status"]): boolean =>
  status === "connected" || status === "syncing" || status === "ready";

const failure = (
  failure_code: Extract<
    OutboundAdapterResult,
    { type: "failure" }
  >["failure_code"],
  reason: string,
): OutboundAdapterResult => ({ type: "failure", failure_code, reason });

const isTimeoutStatus = (status: number): boolean =>
  status === 408 || status === 504;

const isUncertainStatus = (status: number): boolean =>
  status >= 500 && status !== 501;

const parseErrorReason = (value: unknown, fallback: string): string => {
  const parsed = z
    .object({ reason: z.string().trim().min(1).max(200).optional() })
    .passthrough()
    .safeParse(value);
  return parsed.success && parsed.data.reason !== undefined
    ? parsed.data.reason
    : fallback;
};

const parseEvidence = (
  payload: OutboundDispatchPayload,
  response: WhatsAppResponse,
): OutboundAdapterEvidence[] | null => {
  const candidates = response.evidence ?? [];
  const parsed: OutboundAdapterEvidence[] = [];
  for (const candidate of candidates) {
    const result = OutboundEvidenceInputSchema.safeParse({
      schema_version: 1,
      tenant_id: payload.tenant_id,
      command_id: payload.command_id,
      source: candidate.source,
      evidence_id: candidate.evidence_id,
      transaction_id: payload.transaction_id,
      request_digest: payload.request_digest,
      account_id: payload.account_id,
      conversation_id: payload.conversation_id,
      generation: payload.projection_generation,
      status: candidate.status,
      observed_at: candidate.observed_at ?? response.observed_at,
      ...(candidate.provider_operation_id === undefined
        ? {}
        : { provider_operation_id: candidate.provider_operation_id }),
      ...(candidate.provider_message_id === undefined
        ? {}
        : { provider_message_id: candidate.provider_message_id }),
      ...(candidate.remote_echo_id === undefined
        ? {}
        : { remote_echo_id: candidate.remote_echo_id }),
      ...(candidate.reason === undefined ? {} : { reason: candidate.reason }),
    });
    if (!result.success) return null;
    parsed.push(candidate);
  }
  return parsed;
};

/**
 * Provider-neutral outbound boundary for the pinned WhatsApp gateway route.
 * The route is deliberately private and provider-neutral; the gateway owns
 * the WhatsApp client. This worker never talks to a WhatsApp endpoint or
 * chooses a fallback account.
 */
export class HttpWhatsAppTextAdapter {
  private readonly baseUrl: string;
  private readonly sharedSecret: string;

  constructor(
    private readonly context: OutboundAcceptanceContext,
    private readonly fetcher: GatewayFetch,
  ) {
    const env = runtimeEnvironment(context.env);
    this.baseUrl = (env.CONNECTION_GATEWAY_URL ?? "").replace(/\/$/, "");
    this.sharedSecret = env.CONNECTION_GATEWAY_TOKEN ?? "";
  }

  async dispatch(
    dispatch: OutboundDispatch,
    payloadInput: OutboundDispatchPayload,
  ): Promise<OutboundAdapterResult> {
    const payload = OutboundDispatchPayloadSchema.parse(payloadInput);
    if (
      dispatch.tenant_id !== payload.tenant_id ||
      dispatch.command_id !== payload.command_id ||
      dispatch.id !== payload.dispatch_id ||
      dispatch.message_id !== payload.message_id ||
      dispatch.event_id !== payload.event_id ||
      dispatch.actor_identity_id !== payload.identity_id ||
      dispatch.resource_identity_id !== payload.resource_identity_id ||
      dispatch.account_id !== payload.account_id ||
      dispatch.connection_id !== payload.connection_id ||
      dispatch.conversation_id !== payload.conversation_id ||
      dispatch.transaction_id !== payload.transaction_id ||
      dispatch.request_digest !== payload.request_digest ||
      (dispatch.body_digest !== undefined &&
        dispatch.body_digest !== payload.body_digest) ||
      dispatch.projection_generation !== payload.projection_generation
    ) {
      return failure("provider_protocol_error", "dispatch_payload_mismatch");
    }
    if (payload.authority === undefined || payload.body_digest === undefined) {
      return failure("provider_protocol_error", "dispatch_authority_missing");
    }
    if (
      dispatch.authority !== undefined &&
      JSON.stringify(dispatch.authority) !== JSON.stringify(payload.authority)
    ) {
      return failure("provider_protocol_error", "dispatch_authority_mismatch");
    }
    if (payload.provider !== "whatsapp") {
      return failure("account_mismatch", "selected_connection_is_not_whatsapp");
    }
    if (this.baseUrl.length === 0 || this.sharedSecret.length < 16) {
      return failure("provider_unavailable", "whatsapp_gateway_not_configured");
    }

    const connection = await this.readConnection(payload);
    if (connection === null) {
      return failure("account_mismatch", "selected_account_route_not_found");
    }
    if (
      connection.provider !== "whatsapp" ||
      connection.account_id !== payload.account_id
    ) {
      return failure("account_mismatch", "selected_account_route_mismatch");
    }
    if (!usableConnection(connection.status)) {
      return failure("connection_unavailable", connection.status);
    }
    if (connection.has_send_capability !== 1) {
      return failure("missing_capability", "message.send.text");
    }
    if (connection.has_provider_identity !== 1) {
      return failure("session_expired", "provider_identity_missing");
    }
    const currentConnection = await this.readConnection(payload);
    if (currentConnection === null) {
      return failure("account_mismatch", "selected_account_route_not_found");
    }
    if (
      currentConnection.session_generation !== connection.session_generation
    ) {
      return failure("session_expired", "session_generation_changed");
    }
    if (!usableConnection(currentConnection.status)) {
      return failure("connection_unavailable", currentConnection.status);
    }
    if (currentConnection.has_send_capability !== 1) {
      return failure("missing_capability", "message.send.text");
    }
    if (currentConnection.has_provider_identity !== 1) {
      return failure("session_expired", "provider_identity_missing");
    }

    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}/v1/outbound/text`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": crypto.randomUUID(),
          "idempotency-key": `outbound-${payload.transaction_id}`,
        },
        body: JSON.stringify({
          schema_version: 1,
          tenant_id: payload.tenant_id,
          account_id: payload.account_id,
          connection_id: payload.connection_id,
          actor_identity_id: payload.identity_id,
          identity_id: payload.resource_identity_id,
          membership_id: payload.authority.membership_id,
          reservation_id: payload.authority.reservation_id,
          capability: payload.authority.capability,
          provider: payload.provider,
          conversation_id: payload.conversation_id,
          command_id: payload.command_id,
          dispatch_id: payload.dispatch_id,
          message_id: payload.message_id,
          event_id: payload.event_id,
          transaction_id: payload.transaction_id,
          request_digest: payload.request_digest,
          body_digest: payload.body_digest,
          projection_generation: payload.projection_generation,
          session_generation: connection.session_generation,
          route: {
            gateway_route_id: connection.gateway_route_id,
            bridge_instance_id: connection.bridge_instance_id,
            matrix_user_id: connection.matrix_user_id,
            matrix_room_namespace: connection.matrix_room_namespace,
          },
          body: payload.body,
        }),
        signal: AbortSignal.timeout(30_000),
      });
    } catch {
      return {
        type: "uncertain",
        reason: "gateway_request_timeout_or_network_error",
        evidence_id: `uncertain_${payload.transaction_id}`,
      };
    }

    let responseBody: unknown = null;
    try {
      responseBody = await response.clone().json();
    } catch {
      responseBody = null;
    }
    if (!response.ok) {
      const reason = parseErrorReason(
        responseBody,
        `gateway_http_${response.status}`,
      );
      if (
        isTimeoutStatus(response.status) ||
        isUncertainStatus(response.status)
      ) {
        return {
          type: "uncertain",
          reason,
          evidence_id: `uncertain_${payload.transaction_id}`,
        };
      }
      if (response.status === 403 && reason === "authorization_revoked") {
        return failure("authorization_revoked", reason);
      }
      if (response.status === 401 || response.status === 403) {
        return failure("session_expired", reason);
      }
      if (response.status === 404) return failure("provider_rejected", reason);
      if (response.status === 429) return failure("rate_limited", reason);
      return failure("provider_rejected", reason);
    }

    const parsed = responseSchema.safeParse(responseBody);
    if (!parsed.success) {
      return {
        type: "uncertain",
        reason: "invalid_gateway_response_after_request",
        evidence_id: `uncertain_${payload.transaction_id}`,
      };
    }
    const result = parsed.data;
    if (
      result.transaction_id !== payload.transaction_id ||
      result.request_digest !== payload.request_digest ||
      result.account_id !== payload.account_id ||
      result.connection_id !== payload.connection_id ||
      result.session_generation !== connection.session_generation
    ) {
      return {
        type: "uncertain",
        reason: "gateway_scope_or_transaction_mismatch_after_request",
        evidence_id: `uncertain_${payload.transaction_id}`,
      };
    }
    if (result.outcome === "uncertain") {
      return {
        type: "uncertain",
        reason: result.reason ?? "gateway_result_uncertain",
        evidence_id: `uncertain_${payload.transaction_id}`,
      };
    }
    if (result.outcome === "rejected") {
      if (result.reason === "authorization_revoked") {
        return failure("authorization_revoked", result.reason);
      }
      return failure("provider_rejected", result.reason ?? "provider_rejected");
    }
    if (result.outcome === "rate_limited") {
      return failure("rate_limited", result.reason ?? "provider_rate_limited");
    }
    if (result.outcome === "session_expired") {
      return failure(
        "session_expired",
        result.reason ?? "provider_session_expired",
      );
    }
    if (result.outcome === "missing_capability") {
      return failure(
        "missing_capability",
        result.reason ?? "message.send.text",
      );
    }
    const evidences = parseEvidence(payload, result);
    if (evidences === null || evidences.length === 0) {
      return {
        type: "uncertain",
        reason: "gateway_response_missing_stage_evidence",
        evidence_id: `uncertain_${payload.transaction_id}`,
      };
    }
    return { type: "evidence_batch", evidences };
  }

  private async readConnection(
    payload: OutboundDispatchPayload,
  ): Promise<ConnectionRow | null> {
    const database = this.context.env.CONTROL_DB;
    if (database === undefined || typeof database.withSession !== "function") {
      return null;
    }
    const row = await database
      .withSession("first-primary")
      .prepare(
        `SELECT c.tenant_id, c.id AS connection_id, c.identity_id, c.provider,
                ca.account_id, c.status, c.updated_at AS session_generation,
                cr.gateway_route_id, cr.bridge_instance_id, cr.matrix_user_id,
                cr.matrix_room_namespace,
                EXISTS (
                  SELECT 1 FROM connection_capabilities AS cc
                   WHERE cc.tenant_id = c.tenant_id
                     AND cc.connection_id = c.id
                     AND cc.capability = 'message.send'
                ) AS has_send_capability,
                EXISTS (
                  SELECT 1 FROM connection_provider_identities AS pi
                   WHERE pi.tenant_id = c.tenant_id
                     AND pi.connection_id = c.id
                     AND pi.provider = c.provider
                ) AS has_provider_identity
           FROM connections AS c
           JOIN connection_accounts AS ca
             ON ca.connection_id = c.id AND ca.status = 'active'
           JOIN connection_routes AS cr ON cr.connection_id = c.id
          WHERE c.tenant_id = ?
            AND c.id = ?
            AND c.identity_id = ?
            AND ca.account_id = ?
          LIMIT 1`,
      )
      .bind(
        payload.tenant_id,
        payload.connection_id,
        payload.resource_identity_id,
        payload.account_id,
      )
      .first<unknown>();
    const parsed = connectionRowSchema.safeParse(row);
    return parsed.success ? parsed.data : null;
  }
}

export const defaultWhatsAppTextAdapter = (
  context: OutboundAcceptanceContext,
  fetcher?: GatewayFetch,
): HttpWhatsAppTextAdapter | undefined => {
  const env = runtimeEnvironment(context.env);
  if (
    typeof env.CONNECTION_GATEWAY_URL !== "string" ||
    env.CONNECTION_GATEWAY_URL.length === 0 ||
    typeof env.CONNECTION_GATEWAY_TOKEN !== "string" ||
    env.CONNECTION_GATEWAY_TOKEN.length < 16
  ) {
    return undefined;
  }
  if (fetcher !== undefined)
    return new HttpWhatsAppTextAdapter(context, fetcher);
  try {
    return new HttpWhatsAppTextAdapter(
      context,
      gatewayFetchFromEnv(context.env),
    );
  } catch (error) {
    if (error instanceof GatewayTransportConfigError) return undefined;
    throw error;
  }
};
