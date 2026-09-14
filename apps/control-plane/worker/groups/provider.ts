import {
  GroupManagementEvidenceSchema,
  GroupEvidenceSchema,
  type GroupEvidence,
  type GroupManagementEvidence,
  type GroupEvidenceSource,
} from "@communicator/contracts";
import type { OutboundCapability } from "@communicator/contracts";
import { z } from "zod";
import type { ContactRoute } from "../contacts/provider";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";

export type GroupProviderInput = {
  route: ContactRoute;
  operation_id: string;
  conversation_id: string;
  idempotency_key: string;
  membership_id?: string;
  actor_identity_id?: string;
  reservation_id?: string;
  capability?: OutboundCapability;
  request_hash?: string;
  operation_scope?: "group.create" | "group.manage";
};

export type GroupManagementProviderInput = GroupProviderInput & {
  provider_group_id: string;
  matrix_room_id: string;
  expected_revision: string;
  action: "rename" | "add_participants" | "remove_participants";
  operation_created_at: string;
  requested_name: string | null;
  requested_member_provider_ids: readonly string[];
};

export type ProviderGroup = {
  provider_group_id: string;
  matrix_room_id: string;
  name: string;
  participant_provider_ids: string[];
  evidence: GroupEvidence;
};

export type ManagedProviderGroup = {
  provider_group_id: string;
  matrix_room_id: string;
  name: string;
  revision: string;
  member_provider_ids: string[];
  evidence: GroupManagementEvidence;
};

export interface GroupProvider {
  createGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup>;
  /** Read a correlated provider event that may arrive after create timed out. */
  observeGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup | null>;
  /** Perform one bounded, account-scoped refresh after event lookup. */
  refreshGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup | null>;
  renameGroup?: (
    input: GroupManagementProviderInput,
    name: string,
  ) => Promise<ManagedProviderGroup>;
  addGroupParticipants?: (
    input: GroupManagementProviderInput,
    participantProviderIds: readonly string[],
  ) => Promise<ManagedProviderGroup>;
  removeGroupParticipants?: (
    input: GroupManagementProviderInput,
    participantProviderIds: readonly string[],
  ) => Promise<ManagedProviderGroup>;
  observeManagedGroup?: (
    input: GroupManagementProviderInput,
  ) => Promise<ManagedProviderGroup | null>;
  refreshManagedGroup?: (
    input: GroupManagementProviderInput,
  ) => Promise<ManagedProviderGroup | null>;
}

export class GroupProviderError extends Error {
  constructor(
    readonly code:
      | "unsupported"
      | "unavailable"
      | "session_expired"
      | "authorization_revoked"
      | "rejected"
      | "uncertain"
      | "not_found",
    readonly status?: number,
  ) {
    super(code);
    this.name = "GroupProviderError";
  }
}

const runtimeEnvironment = (env: Cloudflare.Env) =>
  env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };

const groupPayloadSchema = z
  .object({
    id: z.string().trim().min(1).max(512).optional(),
    provider_group_id: z.string().trim().min(1).max(512).optional(),
    mxid: z.string().trim().min(1).max(512).optional(),
    matrix_room_id: z.string().trim().min(1).max(512).optional(),
    name: z.string().trim().min(1).max(100),
    participants: z
      .array(z.string().trim().min(1).max(512))
      .max(128)
      .optional(),
    participant_provider_ids: z
      .array(z.string().trim().min(1).max(512))
      .max(128)
      .optional(),
    revision: z.string().trim().min(1).max(128).optional(),
    group_revision: z.string().trim().min(1).max(128).optional(),
    member_provider_ids: z
      .array(z.string().trim().min(1).max(512))
      .max(128)
      .optional(),
    members: z.array(z.string().trim().min(1).max(512)).max(128).optional(),
    evidence: z.unknown().optional(),
  })
  .passthrough();

const gatewayEvidenceSchema = z
  .object({
    source: z.enum(["provider", "bridge", "event", "refresh"]).optional(),
    evidence_id: z.string().trim().min(1).max(256).optional(),
    observed_at: z.string().trim().min(1).max(64).optional(),
    account_id: z.string().trim().min(1).max(128).optional(),
    connection_id: z.string().trim().min(1).max(128).optional(),
    operation_id: z.string().trim().min(1).max(128).optional(),
    participant_provider_ids: z
      .array(z.string().trim().min(1).max(512))
      .max(128)
      .optional(),
    member_provider_ids: z
      .array(z.string().trim().min(1).max(512))
      .max(128)
      .optional(),
    revision: z.string().trim().min(1).max(128).optional(),
    group_revision: z.string().trim().min(1).max(128).optional(),
    name: z.string().trim().min(1).max(100).optional(),
    status: z.enum(["confirmed", "uncertain"]).optional(),
    reason: z.string().trim().min(1).max(200).nullable().optional(),
  })
  .passthrough();

const mapSource = (
  source: z.infer<typeof gatewayEvidenceSchema>["source"],
  fallback: GroupEvidenceSource,
): GroupEvidenceSource => {
  if (source === "event" || source === "refresh" || source === "provider")
    return source;
  return fallback;
};

const mapGroup = (
  value: unknown,
  input: GroupProviderInput,
  requestedName: string,
  fallbackSource: GroupEvidenceSource,
): ProviderGroup => {
  const parsed = groupPayloadSchema.safeParse(value);
  if (!parsed.success) throw new GroupProviderError("rejected");
  const providerGroupId = parsed.data.provider_group_id ?? parsed.data.id;
  const matrixRoomId = parsed.data.matrix_room_id ?? parsed.data.mxid;
  const participants =
    parsed.data.participant_provider_ids ?? parsed.data.participants;
  if (
    providerGroupId === undefined ||
    matrixRoomId === undefined ||
    participants === undefined
  ) {
    throw new GroupProviderError("rejected");
  }
  const gatewayEvidence = gatewayEvidenceSchema.safeParse(parsed.data.evidence);
  if (
    !gatewayEvidence.success ||
    gatewayEvidence.data.source === undefined ||
    gatewayEvidence.data.evidence_id === undefined ||
    gatewayEvidence.data.observed_at === undefined ||
    gatewayEvidence.data.operation_id === undefined ||
    gatewayEvidence.data.account_id === undefined ||
    gatewayEvidence.data.connection_id === undefined ||
    gatewayEvidence.data.participant_provider_ids === undefined ||
    gatewayEvidence.data.status === undefined
  ) {
    throw new GroupProviderError("uncertain");
  }
  const evidence = GroupEvidenceSchema.safeParse({
    source: mapSource(gatewayEvidence.data.source, fallbackSource),
    evidence_id: gatewayEvidence.data.evidence_id,
    observed_at: gatewayEvidence.data.observed_at,
    operation_id: gatewayEvidence.data.operation_id,
    account_id: gatewayEvidence.data.account_id,
    connection_id: gatewayEvidence.data.connection_id,
    provider_group_id: providerGroupId,
    matrix_room_id: matrixRoomId,
    participant_provider_ids: gatewayEvidence.data.participant_provider_ids,
    status: gatewayEvidence.data.status,
    reason: gatewayEvidence.data.reason ?? null,
  });
  if (!evidence.success) throw new GroupProviderError("rejected");
  return {
    provider_group_id: providerGroupId,
    matrix_room_id: matrixRoomId,
    name: parsed.data.name ?? requestedName,
    participant_provider_ids: participants,
    evidence: evidence.data,
  };
};

const mapManagedGroup = (
  value: unknown,
  input: GroupManagementProviderInput,
  fallbackSource: GroupEvidenceSource,
): ManagedProviderGroup => {
  const parsed = groupPayloadSchema.safeParse(value);
  if (!parsed.success) throw new GroupProviderError("rejected");
  const providerGroupId = parsed.data.provider_group_id ?? parsed.data.id;
  const matrixRoomId = parsed.data.matrix_room_id ?? parsed.data.mxid;
  const memberProviderIds =
    parsed.data.member_provider_ids ??
    parsed.data.members ??
    parsed.data.participant_provider_ids ??
    parsed.data.participants;
  const revision = parsed.data.revision ?? parsed.data.group_revision;
  if (
    providerGroupId === undefined ||
    matrixRoomId === undefined ||
    memberProviderIds === undefined ||
    revision === undefined
  ) {
    throw new GroupProviderError("uncertain");
  }
  const gatewayEvidence = gatewayEvidenceSchema.safeParse(parsed.data.evidence);
  if (
    !gatewayEvidence.success ||
    gatewayEvidence.data.source === undefined ||
    gatewayEvidence.data.evidence_id === undefined ||
    gatewayEvidence.data.observed_at === undefined ||
    gatewayEvidence.data.operation_id === undefined ||
    gatewayEvidence.data.account_id === undefined ||
    gatewayEvidence.data.connection_id === undefined ||
    gatewayEvidence.data.status === undefined
  ) {
    throw new GroupProviderError("uncertain");
  }
  const evidence = GroupManagementEvidenceSchema.safeParse({
    source: mapSource(gatewayEvidence.data.source, fallbackSource),
    evidence_id: gatewayEvidence.data.evidence_id,
    observed_at: gatewayEvidence.data.observed_at,
    operation_id: gatewayEvidence.data.operation_id,
    account_id: gatewayEvidence.data.account_id,
    connection_id: gatewayEvidence.data.connection_id,
    provider_group_id: providerGroupId,
    matrix_room_id: matrixRoomId,
    revision: gatewayEvidence.data.revision ?? revision,
    name: gatewayEvidence.data.name ?? parsed.data.name,
    member_provider_ids:
      gatewayEvidence.data.member_provider_ids ?? memberProviderIds,
    status: gatewayEvidence.data.status,
    reason: gatewayEvidence.data.reason ?? null,
    accepted: true,
  });
  if (!evidence.success) throw new GroupProviderError("rejected");
  return {
    provider_group_id: providerGroupId,
    matrix_room_id: matrixRoomId,
    name: parsed.data.name,
    revision,
    member_provider_ids: memberProviderIds,
    evidence: evidence.data,
  };
};

/** Worker adapter for the private, account-bound group gateway routes. */
export class HttpGroupProvider implements GroupProvider {
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

  async createGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup> {
    const value = await this.request("/v1/groups/create", input, {
      name,
      participants: [...participantProviderIds],
    });
    return mapGroup(value, input, name, "provider");
  }

  async observeGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup | null> {
    try {
      const value = await this.request("/v1/groups/event", input, {
        name,
        participants: [...participantProviderIds],
      });
      return mapGroup(value, input, name, "event");
    } catch (error) {
      if (error instanceof GroupProviderError && error.code === "not_found")
        return null;
      throw error;
    }
  }

  async refreshGroup(
    input: GroupProviderInput,
    name: string,
    participantProviderIds: readonly string[],
  ): Promise<ProviderGroup | null> {
    try {
      const value = await this.request("/v1/groups/refresh", input, {
        name,
        participants: [...participantProviderIds],
      });
      return mapGroup(value, input, name, "refresh");
    } catch (error) {
      if (error instanceof GroupProviderError && error.code === "not_found")
        return null;
      throw error;
    }
  }

  async renameGroup(
    input: GroupManagementProviderInput,
    name: string,
  ): Promise<ManagedProviderGroup> {
    const value = await this.request("/v1/groups/rename", input, {
      provider_group_id: input.provider_group_id,
      matrix_room_id: input.matrix_room_id,
      expected_revision: input.expected_revision,
      action: input.action,
      name,
    });
    return mapManagedGroup(value, input, "provider");
  }

  async addGroupParticipants(
    input: GroupManagementProviderInput,
    participantProviderIds: readonly string[],
  ): Promise<ManagedProviderGroup> {
    const value = await this.request("/v1/groups/participants/add", input, {
      provider_group_id: input.provider_group_id,
      matrix_room_id: input.matrix_room_id,
      expected_revision: input.expected_revision,
      action: input.action,
      participant_provider_ids: [...participantProviderIds],
    });
    return mapManagedGroup(value, input, "provider");
  }

  async removeGroupParticipants(
    input: GroupManagementProviderInput,
    participantProviderIds: readonly string[],
  ): Promise<ManagedProviderGroup> {
    const value = await this.request("/v1/groups/participants/remove", input, {
      provider_group_id: input.provider_group_id,
      matrix_room_id: input.matrix_room_id,
      expected_revision: input.expected_revision,
      action: input.action,
      participant_provider_ids: [...participantProviderIds],
    });
    return mapManagedGroup(value, input, "provider");
  }

  async observeManagedGroup(
    input: GroupManagementProviderInput,
  ): Promise<ManagedProviderGroup | null> {
    try {
      const value = await this.request("/v1/groups/management/event", input, {
        provider_group_id: input.provider_group_id,
        matrix_room_id: input.matrix_room_id,
        expected_revision: input.expected_revision,
        action: input.action,
        name: input.requested_name,
        participant_provider_ids: [...input.requested_member_provider_ids],
      });
      return mapManagedGroup(value, input, "event");
    } catch (error) {
      if (error instanceof GroupProviderError && error.code === "not_found")
        return null;
      throw error;
    }
  }

  async refreshManagedGroup(
    input: GroupManagementProviderInput,
  ): Promise<ManagedProviderGroup | null> {
    try {
      const value = await this.request("/v1/groups/management/refresh", input, {
        provider_group_id: input.provider_group_id,
        matrix_room_id: input.matrix_room_id,
        expected_revision: input.expected_revision,
        action: input.action,
        name: input.requested_name,
        participant_provider_ids: [...input.requested_member_provider_ids],
      });
      return mapManagedGroup(value, input, "refresh");
    } catch (error) {
      if (error instanceof GroupProviderError && error.code === "not_found")
        return null;
      throw error;
    }
  }

  private async request(
    path: string,
    input: GroupProviderInput | GroupManagementProviderInput,
    body: Record<string, unknown>,
  ): Promise<unknown> {
    if (this.baseUrl.length === 0 || this.sharedSecret.length < 16)
      throw new GroupProviderError("unavailable");
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": crypto.randomUUID(),
          "idempotency-key": `group-${input.operation_id}`,
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
          ...(input.membership_id === undefined ||
          input.actor_identity_id === undefined ||
          input.reservation_id === undefined ||
          input.capability === undefined ||
          input.request_hash === undefined
            ? {}
            : {
                membership_id: input.membership_id,
                actor_identity_id: input.actor_identity_id,
                reservation_id: input.reservation_id,
                capability: input.capability,
                request_hash: input.request_hash,
              }),
          operation_created_at:
            "operation_created_at" in input
              ? input.operation_created_at
              : undefined,
          conversation_id: input.conversation_id,
          ...body,
        }),
        signal: AbortSignal.timeout(30_000),
      });
    } catch {
      throw new GroupProviderError("unavailable");
    }
    if (response.status === 501)
      throw new GroupProviderError("unsupported", response.status);
    if (response.status === 403) {
      try {
        const value = (await response.clone().json()) as {
          error?: unknown;
        };
        if (value.error === "authorization_revoked")
          throw new GroupProviderError(
            "authorization_revoked",
            response.status,
          );
      } catch (error) {
        if (error instanceof GroupProviderError) throw error;
      }
      throw new GroupProviderError("session_expired", response.status);
    }
    if (response.status === 401)
      throw new GroupProviderError("session_expired", response.status);
    if (response.status === 404)
      throw new GroupProviderError("not_found", response.status);
    if (
      response.status === 408 ||
      response.status === 504 ||
      response.status >= 500
    )
      throw new GroupProviderError("unavailable", response.status);
    if (!response.ok) throw new GroupProviderError("rejected", response.status);
    try {
      return await response.json();
    } catch {
      throw new GroupProviderError("rejected", response.status);
    }
  }
}

const unavailableGroupOperation = async (): Promise<never> => {
  throw new GroupProviderError("unavailable");
};

export const defaultGroupProvider = (env: Cloudflare.Env): GroupProvider => {
  try {
    return new HttpGroupProvider(env, gatewayFetchFromEnv(env));
  } catch (error) {
    if (error instanceof GatewayTransportConfigError) {
      return {
        createGroup: unavailableGroupOperation,
        observeGroup: unavailableGroupOperation,
        refreshGroup: unavailableGroupOperation,
        renameGroup: unavailableGroupOperation,
        addGroupParticipants: unavailableGroupOperation,
        removeGroupParticipants: unavailableGroupOperation,
        observeManagedGroup: unavailableGroupOperation,
        refreshManagedGroup: unavailableGroupOperation,
      };
    }
    throw error;
  }
};
