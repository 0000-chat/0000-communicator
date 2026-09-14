import {
  ContactEvidenceOperationSchema,
  ContactProviderEvidenceSchema,
  TimestampSchema,
  type ContactEvidenceOperation,
  type ContactProviderEvidence,
  type Provider,
} from "@communicator/contracts";
import { z } from "zod";
import type { OutboundCapability } from "../outbound/authority-types";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";

export type ContactRoute = {
  tenant_id: string;
  identity_id: string;
  account_id: string;
  connection_id: string;
  provider: Provider;
  session_generation: string;
  gateway_route_id: string;
  bridge_instance_id: string;
  matrix_user_id: string;
  matrix_room_namespace: string;
  provider_login_id: string;
};

export type ProviderContact = {
  provider_id: string;
  current_lid: string | null;
  display_name: string;
  identifiers: string[];
  evidence: ContactProviderEvidence;
};

export type ProviderDirectChat = ProviderContact & {
  matrix_room_id: string;
  status: "created" | "already_exists";
};

export type ContactProviderInput = {
  route: ContactRoute;
  operation_id: string;
  idempotency_key: string;
  membership_id?: string;
  actor_identity_id?: string;
  reservation_id?: string;
  capability?: OutboundCapability;
  request_hash?: string;
  operation_scope?: "conversation.create";
};

export interface ContactProvider {
  search(
    input: ContactProviderInput,
    query: string,
  ): Promise<ProviderContact[]>;
  resolve(
    input: ContactProviderInput,
    identifier: string,
  ): Promise<ProviderContact>;
  createDirectChat(
    input: ContactProviderInput,
    providerId: string,
    conversationId: string,
  ): Promise<ProviderDirectChat>;
}

export class ContactProviderError extends Error {
  constructor(
    readonly code:
      | "unsupported"
      | "unavailable"
      | "session_expired"
      | "unresolved"
      | "rejected"
      | "uncertain",
    readonly status?: number,
  ) {
    super(code);
    this.name = "ContactProviderError";
  }
}

const providerPayloadSchema = z
  .object({
    id: z.string().trim().min(1).max(512),
    name: z.string().trim().min(1).max(200).optional(),
    identifiers: z.array(z.string().trim().min(1).max(512)).max(32).optional(),
  })
  .passthrough();

const gatewayEvidenceSchema = z
  .object({
    source: z.enum(["bridge", "provider"]).default("bridge"),
    operation: ContactEvidenceOperationSchema,
    evidence_id: z.string().trim().min(1).max(256),
    observed_at: TimestampSchema,
    status: z.enum(["confirmed", "already_exists", "uncertain"]),
    reason: z.string().trim().min(1).max(200).nullable().optional(),
  })
  .passthrough();

const resolvedPayload = (
  value: unknown,
): z.infer<typeof providerPayloadSchema> => {
  const result = providerPayloadSchema.safeParse(value);
  if (!result.success) throw new ContactProviderError("rejected");
  return result.data;
};

const extractLid = (
  id: string,
  identifiers: readonly string[],
): string | null => {
  if (id.endsWith("@lid")) return id;
  return (
    identifiers.find(
      (identifier) =>
        identifier.endsWith("@lid") || identifier.includes("@lid"),
    ) ?? null
  );
};

const evidenceFor = (
  operation: ContactEvidenceOperation,
  providerId: string,
  value: unknown,
  fallbackId: string,
): ContactProviderEvidence => {
  const parsed = gatewayEvidenceSchema.safeParse(value);
  const observedAt = parsed.success
    ? parsed.data.observed_at
    : new Date().toISOString();
  const evidence = ContactProviderEvidenceSchema.safeParse({
    source: parsed.success ? parsed.data.source : "bridge",
    operation,
    evidence_id: parsed.success ? parsed.data.evidence_id : fallbackId,
    observed_at: observedAt,
    provider_id: providerId,
    matrix_room_id: null,
    status: parsed.success ? parsed.data.status : "confirmed",
    reason: parsed.success ? (parsed.data.reason ?? null) : null,
  });
  if (!evidence.success) throw new ContactProviderError("rejected");
  return evidence.data;
};

const mapContact = (
  value: unknown,
  operation: ContactEvidenceOperation,
  fallbackId: string,
): ProviderContact => {
  const parsed = resolvedPayload(value);
  const identifiers = parsed.identifiers ?? [];
  return {
    provider_id: parsed.id,
    current_lid: extractLid(parsed.id, identifiers),
    display_name: parsed.name ?? parsed.id,
    identifiers,
    evidence: evidenceFor(operation, parsed.id, value, fallbackId),
  };
};

const runtimeEnvironment = (env: Cloudflare.Env) =>
  env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };

const pathSegment = (value: string): string => encodeURIComponent(value);

/**
 * Provider-neutral Worker adapter for the private gateway contact routes.
 * Account routing and session generation are supplied by the caller and are
 * included in every request so the gateway cannot silently switch accounts.
 */
export class HttpContactProvider implements ContactProvider {
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

  async search(
    input: ContactProviderInput,
    query: string,
  ): Promise<ProviderContact[]> {
    const value = await this.request("/v1/contacts/search", input, "search", {
      query,
    });
    const parsed = z
      .object({
        results: z.array(z.unknown()).max(100),
        evidence: z.unknown().optional(),
      })
      .passthrough()
      .safeParse(value);
    if (!parsed.success) throw new ContactProviderError("rejected");
    return parsed.data.results.map((result, index) =>
      mapContact(
        result,
        "search",
        `contact_search_${input.operation_id}_${index}`,
      ),
    );
  }

  async resolve(
    input: ContactProviderInput,
    identifier: string,
  ): Promise<ProviderContact> {
    const value = await this.request(
      `/v1/contacts/resolve/${pathSegment(identifier)}`,
      input,
      "resolve",
    );
    return mapContact(
      value,
      "resolve",
      `contact_resolve_${input.operation_id}`,
    );
  }

  async createDirectChat(
    input: ContactProviderInput,
    providerId: string,
    conversationId: string,
  ): Promise<ProviderDirectChat> {
    const value = await this.request(
      "/v1/conversations/direct",
      input,
      "create_dm",
      { provider_id: providerId, conversation_id: conversationId },
    );
    const parsed = z
      .object({
        id: z.string().trim().min(1).max(512),
        name: z.string().trim().min(1).max(200).optional(),
        identifiers: z
          .array(z.string().trim().min(1).max(512))
          .max(32)
          .optional(),
        matrix_room_id: z.string().trim().min(1).max(512),
        status: z.enum(["created", "already_exists"]),
        evidence: z.unknown().optional(),
      })
      .passthrough()
      .safeParse(value);
    if (!parsed.success) throw new ContactProviderError("rejected");
    const identifiers = parsed.data.identifiers ?? [];
    const evidence = ContactProviderEvidenceSchema.safeParse({
      source: "bridge",
      operation: "create_dm",
      evidence_id: `contact_create_${input.operation_id}`,
      observed_at: new Date().toISOString(),
      provider_id: parsed.data.id,
      matrix_room_id: parsed.data.matrix_room_id,
      status: parsed.data.status,
      reason: null,
    });
    if (!evidence.success) throw new ContactProviderError("rejected");
    return {
      provider_id: parsed.data.id,
      current_lid: extractLid(parsed.data.id, identifiers),
      display_name: parsed.data.name ?? parsed.data.id,
      identifiers,
      matrix_room_id: parsed.data.matrix_room_id,
      status: parsed.data.status,
      evidence: evidence.data,
    };
  }

  private async request(
    path: string,
    input: ContactProviderInput,
    operation: ContactEvidenceOperation,
    body?: unknown,
  ): Promise<unknown> {
    if (this.baseUrl.length === 0 || this.sharedSecret.length < 16)
      throw new ContactProviderError("unavailable");
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": crypto.randomUUID(),
          "idempotency-key": `contact-${input.operation_id}-${operation}`,
        },
        body: JSON.stringify({
          schema_version: 1,
          tenant_id: input.route.tenant_id,
          account_id: input.route.account_id,
          connection_id: input.route.connection_id,
          identity_id: input.route.identity_id,
          provider: input.route.provider,
          session_generation: input.route.session_generation,
          route: {
            gateway_route_id: input.route.gateway_route_id,
            bridge_instance_id: input.route.bridge_instance_id,
            matrix_user_id: input.route.matrix_user_id,
            matrix_room_namespace: input.route.matrix_room_namespace,
            provider_login_id: input.route.provider_login_id,
          },
          operation_id: input.operation_id,
          ...(input.membership_id === undefined
            ? {}
            : { membership_id: input.membership_id }),
          ...(input.actor_identity_id === undefined
            ? {}
            : { actor_identity_id: input.actor_identity_id }),
          ...(input.reservation_id === undefined
            ? {}
            : { reservation_id: input.reservation_id }),
          ...(input.capability === undefined
            ? {}
            : { capability: input.capability }),
          ...(input.request_hash === undefined
            ? {}
            : { request_hash: input.request_hash }),
          ...(body === undefined ? {} : body),
        }),
        signal: AbortSignal.timeout(30_000),
      });
    } catch {
      throw new ContactProviderError("unavailable");
    }
    if (response.status === 501)
      throw new ContactProviderError("unsupported", response.status);
    if (response.status === 401 || response.status === 403)
      throw new ContactProviderError("session_expired", response.status);
    if (response.status === 404)
      throw new ContactProviderError("unresolved", response.status);
    if (
      response.status === 408 ||
      response.status === 504 ||
      response.status >= 500
    )
      throw new ContactProviderError("unavailable", response.status);
    if (!response.ok)
      throw new ContactProviderError("rejected", response.status);
    try {
      return await response.json();
    } catch {
      throw new ContactProviderError("rejected", response.status);
    }
  }
}

const unavailableContactOperation = async (): Promise<never> => {
  throw new ContactProviderError("unavailable");
};

export const defaultContactProvider = (
  env: Cloudflare.Env,
): ContactProvider => {
  try {
    return new HttpContactProvider(env, gatewayFetchFromEnv(env));
  } catch (error) {
    if (error instanceof GatewayTransportConfigError) {
      return {
        search: unavailableContactOperation,
        resolve: unavailableContactOperation,
        createDirectChat: unavailableContactOperation,
      };
    }
    throw error;
  }
};
