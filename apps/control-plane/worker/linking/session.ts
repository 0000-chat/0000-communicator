import { z } from "zod";
import {
  CommunicatorIdSchema,
  LinkSessionActionSchema,
  LinkSessionErrorCodeSchema,
  LinkSessionStatusSchema,
  ProviderSchema,
  TimestampSchema,
} from "@communicator/contracts";
import type {
  LinkSessionAction,
  LinkSessionErrorCode,
  LinkSessionStatus,
  Provider,
} from "@communicator/contracts";
import {
  commitLinkedAccount,
  LinkingRepositoryError,
  type LinkingRepositoryErrorCode,
} from "./repository";
import {
  completeConnectionRelink,
  LifecycleRepositoryError,
  markLifecycleReconciliation,
  type LifecycleActor,
} from "./lifecycle-repository";
import { gatewayFetchFromEnv } from "../gateway/private-fetch";

const STATE_KEY = "link-session";
const PRODUCT_TTL_MS = 10 * 60_000;

export type LinkSessionOwner = {
  tenant_id: string;
  actor_principal_id: string;
  membership_id: string;
  target_identity_id: string;
  provider: Provider;
};

export type LinkSessionState = LinkSessionOwner & {
  id: string;
  generation: number;
  status: LinkSessionStatus;
  action: LinkSessionAction;
  expires_at: string;
  action_expires_at: string | null;
  gateway_ref: string | null;
  connection_id: string | null;
  /** Set only for a session relinking an already-established connection. */
  lifecycle_operation_id: string | null;
  /** Raw provider login is gateway-bound and never exposed in LinkSession. */
  provider_login_id: string | null;
  account_id: string | null;
  provider_label: string | null;
  error_code: LinkSessionErrorCode | null;
  request_key_digest: string;
  created_at: string;
  updated_at: string;
};

const ownerSchema = z
  .object({
    tenant_id: CommunicatorIdSchema,
    actor_principal_id: CommunicatorIdSchema,
    membership_id: CommunicatorIdSchema,
    target_identity_id: CommunicatorIdSchema,
    provider: ProviderSchema,
  })
  .strict();

const persistedStateSchema = z
  .object({
    id: CommunicatorIdSchema,
    tenant_id: CommunicatorIdSchema,
    actor_principal_id: CommunicatorIdSchema,
    membership_id: CommunicatorIdSchema,
    target_identity_id: CommunicatorIdSchema,
    provider: ProviderSchema,
    generation: z.number().int().positive(),
    status: LinkSessionStatusSchema,
    action: LinkSessionActionSchema,
    expires_at: TimestampSchema,
    action_expires_at: TimestampSchema.nullable(),
    gateway_ref: z.string().min(1).max(512).nullable(),
    connection_id: CommunicatorIdSchema.nullable(),
    lifecycle_operation_id: CommunicatorIdSchema.nullable().optional(),
    provider_login_id: z.string().trim().min(1).max(512).nullable().optional(),
    account_id: CommunicatorIdSchema.nullable(),
    provider_label: z.string().min(1).max(100).nullable(),
    error_code: LinkSessionErrorCodeSchema.nullable(),
    request_key_digest: z.string().length(64),
    created_at: TimestampSchema,
    updated_at: TimestampSchema,
  })
  // The input can contain an injected runtime field. safeState below rebuilds
  // the exact persisted allowlist before anything reaches DO storage.
  .passthrough();

const providerIdentitySchema = z
  .object({
    user_login_id: z.string().trim().min(1).max(256),
    display_label: z.string().trim().min(1).max(100),
    route: z
      .object({
        gateway_route_id: CommunicatorIdSchema,
        bridge_instance_id: z.string().min(1).max(256),
        matrix_user_id: z.string().min(1).max(256),
        matrix_room_namespace: z.string().min(1).max(256),
      })
      .strict(),
  })
  .strict();

const sessionCommandSchema = z.discriminatedUnion("command", [
  z
    .object({ command: z.literal("create"), state: persistedStateSchema })
    .strict(),
  z.object({ command: z.literal("read") }).strict(),
  z
    .object({
      command: z.literal("begin"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
    })
    .strict(),
  z
    .object({
      command: z.literal("set_gateway"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
      gateway_ref: z.string().min(1).max(512),
      action_expires_at: TimestampSchema.nullable(),
    })
    .strict(),
  z
    .object({
      command: z.literal("set_provider_state"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
      status: z.enum(["awaiting_user", "authenticating", "failed", "expired"]),
      action: LinkSessionActionSchema,
      action_expires_at: TimestampSchema.nullable(),
      error_code: LinkSessionErrorCodeSchema.nullable(),
    })
    .strict(),
  z
    .object({
      command: z.literal("refresh"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
    })
    .strict(),
  z
    .object({
      command: z.literal("invalidate"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
      status: z.enum(["cancelled", "expired"]),
    })
    .strict(),
  z
    .object({
      command: z.literal("finish"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
      status: z.enum([
        "connected",
        "relink_required",
        "reconciliation_required",
      ]),
      error_code: LinkSessionErrorCodeSchema.nullable(),
      connection_id: CommunicatorIdSchema.nullable(),
      account_id: CommunicatorIdSchema.nullable(),
      provider_label: z.string().min(1).max(100).nullable(),
    })
    .strict(),
  z
    .object({
      command: z.literal("commit"),
      owner: ownerSchema,
      generation: z.number().int().positive(),
      provider_identity: providerIdentitySchema,
      occurred_at: TimestampSchema,
    })
    .strict(),
]);

type SessionCommand = z.infer<typeof sessionCommandSchema>;

type CommandResult = {
  state: LinkSessionState;
  previous_gateway_ref?: string | null;
  created: boolean;
  committed?: Awaited<ReturnType<typeof commitLinkedAccount>>;
  commit_error?: LinkingRepositoryErrorCode;
};

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
    },
  });

const now = (): string => new Date().toISOString();

const safeState = (state: LinkSessionState): LinkSessionState => {
  const parsed = persistedStateSchema.parse({
    id: state.id,
    tenant_id: state.tenant_id,
    actor_principal_id: state.actor_principal_id,
    membership_id: state.membership_id,
    target_identity_id: state.target_identity_id,
    provider: state.provider,
    generation: state.generation,
    status: state.status,
    action: state.action,
    expires_at: state.expires_at,
    action_expires_at: state.action_expires_at,
    gateway_ref: state.gateway_ref,
    connection_id: state.connection_id,
    lifecycle_operation_id: state.lifecycle_operation_id,
    provider_login_id: state.provider_login_id,
    account_id: state.account_id,
    provider_label: state.provider_label,
    error_code: state.error_code,
    request_key_digest: state.request_key_digest,
    created_at: state.created_at,
    updated_at: state.updated_at,
  });
  return {
    ...parsed,
    lifecycle_operation_id: parsed.lifecycle_operation_id ?? null,
    provider_login_id: parsed.provider_login_id ?? null,
  } as LinkSessionState;
};

const ownerMatches = (state: LinkSessionState, owner: LinkSessionOwner) =>
  state.tenant_id === owner.tenant_id &&
  state.actor_principal_id === owner.actor_principal_id &&
  state.membership_id === owner.membership_id &&
  state.target_identity_id === owner.target_identity_id &&
  state.provider === owner.provider;

const terminal = (status: LinkSessionStatus): boolean =>
  status === "connected" ||
  status === "expired" ||
  status === "failed" ||
  status === "cancelled" ||
  status === "relink_required" ||
  status === "reconciliation_required";

const ensureCurrent = (
  state: LinkSessionState,
  owner: LinkSessionOwner,
  generation: number,
): void => {
  if (!ownerMatches(state, owner) || state.generation !== generation) {
    throw new SessionCommandError("stale_session", 409);
  }
};

class SessionCommandError extends Error {
  constructor(
    readonly code:
      | "stale_session"
      | "not_found"
      | "terminal_session"
      | "invalid_session",
    readonly status: 404 | 409 | 422,
  ) {
    super(code);
  }
}

const parseSessionCommand = (value: unknown): SessionCommand => {
  const parsed = sessionCommandSchema.safeParse(value);
  if (!parsed.success) throw new SessionCommandError("invalid_session", 422);
  return parsed.data;
};

const identityHashSecret = (env: Cloudflare.Env): string => {
  const runtimeEnv = env as Cloudflare.Env & {
    LINKING_IDENTITY_HMAC_SECRET?: string;
  };
  const value = runtimeEnv.LINKING_IDENTITY_HMAC_SECRET;
  if (value && value.length >= 16) return value;
  if (env.COMMUNICATOR_ENV === "development")
    return "development-linking-hmac-secret";
  throw new SessionCommandError("invalid_session", 422);
};

const expireIfNeeded = async (
  ctx: DurableObjectState,
  state: LinkSessionState,
): Promise<LinkSessionState> => {
  if (!terminal(state.status) && Date.parse(state.expires_at) <= Date.now()) {
    const expired: LinkSessionState = {
      ...state,
      generation: state.generation + 1,
      status: "expired",
      action: "none",
      action_expires_at: null,
      gateway_ref: state.gateway_ref,
      error_code: "expired",
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(expired));
    return expired;
  }
  return state;
};

async function readStoredState(
  ctx: DurableObjectState,
): Promise<LinkSessionState> {
  const raw = await ctx.storage.get<unknown>(STATE_KEY);
  if (!raw) throw new SessionCommandError("not_found", 404);
  const parsed = persistedStateSchema.safeParse(raw);
  if (!parsed.success) throw new SessionCommandError("invalid_session", 422);
  const state = safeState(parsed.data as LinkSessionState);
  // Normalize any pre-existing state written by an older/runtime-injected
  // caller before returning it. This prevents QR/process fields from leaking.
  await ctx.storage.put(STATE_KEY, state);
  return state;
}

async function readState(ctx: DurableObjectState): Promise<LinkSessionState> {
  return expireIfNeeded(ctx, await readStoredState(ctx));
}

async function cancelGatewayReference(
  env: Cloudflare.Env,
  state: LinkSessionState,
): Promise<void> {
  const runtimeEnv = env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };
  if (!state.gateway_ref || !runtimeEnv.CONNECTION_GATEWAY_URL) return;
  try {
    const requestId = crypto.randomUUID();
    await gatewayFetchFromEnv(env)(
      `${runtimeEnv.CONNECTION_GATEWAY_URL}/v1/link-sessions/cancel`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${runtimeEnv.CONNECTION_GATEWAY_TOKEN ?? ""}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": requestId,
          "idempotency-key": `link-${state.id}-${state.generation}-cancel`,
        },
        body: JSON.stringify({
          session_id: state.id,
          tenant_id: state.tenant_id,
          actor_principal_id: state.actor_principal_id,
          membership_id: state.membership_id,
          target_identity_id: state.target_identity_id,
          provider: state.provider,
          generation: state.generation,
          gateway_ref: state.gateway_ref,
        }),
      },
    );
  } catch {
    // The local generation is already terminal. The next cleanup attempt can
    // use the gateway's opaque reference; no provider detail is retained here.
  }
}

async function execute(
  ctx: DurableObjectState,
  env: Cloudflare.Env,
  input: SessionCommand,
): Promise<CommandResult> {
  if (input.command === "create") {
    const rawExisting = await ctx.storage.get<unknown>(STATE_KEY);
    if (rawExisting) {
      const parsedExisting = persistedStateSchema.safeParse(rawExisting);
      if (!parsedExisting.success)
        throw new SessionCommandError("invalid_session", 422);
      const existing = safeState(parsedExisting.data as LinkSessionState);
      await ctx.storage.put(STATE_KEY, existing);
      if (existing.request_key_digest !== input.state.request_key_digest) {
        throw new SessionCommandError("invalid_session", 422);
      }
      return { state: await expireIfNeeded(ctx, existing), created: false };
    }
    const state = safeState({
      ...input.state,
      lifecycle_operation_id: input.state.lifecycle_operation_id ?? null,
      provider_login_id: input.state.provider_login_id ?? null,
    } as LinkSessionState);
    await ctx.storage.put(STATE_KEY, state);
    await ctx.storage.setAlarm(Date.parse(state.expires_at));
    return { state, created: true };
  }

  let state = await readState(ctx);
  if (input.command === "read") return { state, created: false };

  if ("owner" in input) ensureCurrent(state, input.owner, input.generation);

  if (input.command === "begin") {
    if (terminal(state.status))
      throw new SessionCommandError("terminal_session", 409);
    return { state, created: false };
  }

  if (input.command === "set_gateway") {
    if (terminal(state.status))
      throw new SessionCommandError("terminal_session", 409);
    state = {
      ...state,
      status: "awaiting_user",
      action: "scan_qr",
      action_expires_at: input.action_expires_at,
      gateway_ref: input.gateway_ref,
      error_code: null,
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(state));
    return { state, created: false };
  }

  if (input.command === "set_provider_state") {
    if (terminal(state.status))
      throw new SessionCommandError("terminal_session", 409);
    state = {
      ...state,
      status: input.status,
      action: input.action,
      action_expires_at: input.action_expires_at,
      error_code: input.error_code,
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(state));
    return { state, created: false };
  }

  if (input.command === "commit") {
    if (terminal(state.status))
      throw new SessionCommandError("terminal_session", 409);
    const database = (env as Cloudflare.Env & { CONTROL_DB?: D1Database })
      .CONTROL_DB;
    if (!database) throw new SessionCommandError("invalid_session", 422);
    try {
      let committed: Awaited<ReturnType<typeof commitLinkedAccount>>;
      if (state.lifecycle_operation_id && state.connection_id) {
        const relinked = await completeConnectionRelink({
          db: database,
          operation_id: state.lifecycle_operation_id,
          session_id: state.id,
          actor: {
            tenant_id: state.tenant_id,
            actor_principal_id: state.actor_principal_id,
            membership_id: state.membership_id,
            identity_id: state.target_identity_id,
          } satisfies LifecycleActor,
          provider_identity: input.provider_identity,
          identity_hash_secret: identityHashSecret(env),
          occurred_at: input.occurred_at,
        });
        committed = relinked.committed ?? {
          kind: "created",
          account: null,
          connection_id:
            relinked.connection?.connection_id ?? state.connection_id,
          account_id: relinked.connection?.account_id ?? state.account_id,
        };
        state = {
          ...state,
          status: "connected",
          action: "none",
          action_expires_at: null,
          gateway_ref: null,
          error_code: null,
          provider_login_id: input.provider_identity.user_login_id,
          connection_id: committed.connection_id,
          account_id: committed.account_id,
          provider_label:
            committed.account?.display_label ?? state.provider_label,
          updated_at: now(),
        };
      } else {
        committed = await commitLinkedAccount({
          db: database,
          sessionId: state.id,
          tenantId: state.tenant_id,
          actorPrincipalId: state.actor_principal_id,
          membershipId: state.membership_id,
          targetIdentityId: state.target_identity_id,
          provider: state.provider,
          providerIdentity: input.provider_identity,
          identityHashSecret: identityHashSecret(env),
          occurredAt: input.occurred_at,
        });
        state = {
          ...state,
          status:
            committed.kind === "duplicate" ? "relink_required" : "connected",
          action: "none",
          action_expires_at: null,
          gateway_ref: null,
          error_code: committed.kind === "duplicate" ? "relink_required" : null,
          connection_id: committed.connection_id,
          account_id: committed.account_id,
          provider_label: committed.account?.display_label ?? null,
          provider_login_id: input.provider_identity.user_login_id,
          updated_at: now(),
        };
      }
      await ctx.storage.put(STATE_KEY, safeState(state));
      return { state, committed, created: false };
    } catch (error) {
      if (
        !(error instanceof LinkingRepositoryError) &&
        !(error instanceof LifecycleRepositoryError)
      )
        throw error;
      if (state.lifecycle_operation_id) {
        try {
          await markLifecycleReconciliation(
            database,
            state.tenant_id,
            state.lifecycle_operation_id,
            error instanceof LifecycleRepositoryError &&
              error.code === "stale_generation"
              ? "stale_generation"
              : error instanceof LifecycleRepositoryError &&
                  error.code === "provider_identity_mismatch"
                ? "reconciliation_required"
                : "reconciliation_required",
            input.occurred_at,
          );
        } catch {
          // The durable session still records reconciliation_required below.
        }
      }
      state = {
        ...state,
        status: "reconciliation_required",
        action: "none",
        action_expires_at: null,
        gateway_ref: null,
        error_code: "reconciliation_required",
        connection_id: null,
        account_id: null,
        provider_label: null,
        updated_at: now(),
      };
      await ctx.storage.put(STATE_KEY, safeState(state));
      return {
        state,
        commit_error:
          error instanceof LinkingRepositoryError
            ? error.code
            : "link_unavailable",
        created: false,
      };
    }
  }

  if (input.command === "refresh") {
    if (terminal(state.status))
      throw new SessionCommandError("terminal_session", 409);
    const previous = state.gateway_ref;
    state = {
      ...state,
      generation: state.generation + 1,
      status: "created",
      action: "none",
      action_expires_at: null,
      gateway_ref: null,
      error_code: null,
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(state));
    return { state, previous_gateway_ref: previous, created: false };
  }

  if (input.command === "invalidate") {
    if (terminal(state.status)) return { state, created: false };
    const previous = state.gateway_ref;
    state = {
      ...state,
      generation: state.generation + 1,
      status: input.status,
      action: "none",
      action_expires_at: null,
      error_code: input.status === "expired" ? "expired" : "cancelled",
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(state));
    return { state, previous_gateway_ref: previous, created: false };
  }

  if (input.command === "finish") {
    if (terminal(state.status)) return { state, created: false };
    state = {
      ...state,
      status: input.status,
      action: "none",
      action_expires_at: null,
      gateway_ref: null,
      error_code: input.error_code,
      connection_id: input.connection_id,
      account_id: input.account_id,
      provider_label: input.provider_label,
      updated_at: now(),
    };
    await ctx.storage.put(STATE_KEY, safeState(state));
    return { state, created: false };
  }

  throw new SessionCommandError("invalid_session", 422);
}

export class LinkSessionDO {
  constructor(
    private readonly stateCtx: DurableObjectState,
    private readonly runtimeEnv: Cloudflare.Env,
  ) {}

  async fetch(request: Request): Promise<Response> {
    let input: SessionCommand;
    try {
      input = parseSessionCommand(await request.json());
    } catch {
      return json({ error: "invalid_session" }, 400);
    }
    try {
      const outcome = await this.stateCtx.blockConcurrencyWhile(async () => {
        try {
          return {
            ok: true as const,
            result: await execute(this.stateCtx, this.runtimeEnv, input),
          };
        } catch (error) {
          return { ok: false as const, error };
        }
      });
      if (!outcome.ok) throw outcome.error;
      return json(outcome.result);
    } catch (error) {
      if (error instanceof SessionCommandError) {
        return json({ error: error.code }, error.status);
      }
      return json({ error: "session_unavailable" }, 503);
    }
  }

  async alarm(): Promise<void> {
    let cleanup: LinkSessionState | null = null;
    try {
      cleanup = await this.stateCtx.blockConcurrencyWhile(async () => {
        const state = await readStoredState(this.stateCtx);
        if (terminal(state.status)) {
          // An earlier read may have performed the expiry transition before
          // the alarm ran. Its gateway ref belongs to the prior generation.
          return state.status === "expired" && state.gateway_ref
            ? { ...state, generation: Math.max(1, state.generation - 1) }
            : null;
        }
        if (Date.parse(state.expires_at) > Date.now()) {
          await this.stateCtx.storage.setAlarm(Date.parse(state.expires_at));
          return null;
        }
        const expired: LinkSessionState = {
          ...state,
          generation: state.generation + 1,
          status: "expired",
          action: "none",
          action_expires_at: null,
          error_code: "expired",
          updated_at: now(),
        };
        await this.stateCtx.storage.put(STATE_KEY, safeState(expired));
        // The gateway session belongs to the pre-expiry generation. Keep that
        // owner tuple for cleanup; the incremented generation is only for
        // rejecting late provider callbacks.
        return state.gateway_ref ? state : null;
      });
      if (cleanup) await cancelGatewayReference(this.runtimeEnv, cleanup);
    } catch {
      // An absent/invalid session has no cleanup work.
    }
  }
}

export const LINK_SESSION_TTL_MS = PRODUCT_TTL_MS;
