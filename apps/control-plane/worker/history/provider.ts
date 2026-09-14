import {
  HistoryImportFailureCodeSchema,
  ProviderEvidenceSchema,
  ProviderSchema,
  ProjectionEventEnvelopeSchema,
  type HistoryImportFailureCode,
  type Provider,
  type ProviderEvidence,
  type ProjectionEventEnvelope,
} from "@communicator/contracts";
import { canonicalJsonStringify } from "../archive/canonical-json";
import { sha256Hex } from "../archive/codec";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";

export type HistoryImportProviderOwner = {
  tenant_id: string;
  account_id: string;
  connection_id: string;
  identity_id: string;
  provider: Provider;
  import_id: string;
  start_at: string;
  end_at: string;
  max_events: number;
};

export type HistoryProviderRange = {
  start_at: string;
  end_at: string;
  source_cursor: string | null;
};

export type HistoryProviderStartResult = {
  availability: "available" | "blocked" | "unavailable";
  provider_version: string | null;
  proof_source: string;
  provider_evidence: ProviderEvidence;
  source_start_at: string | null;
  source_end_at: string | null;
  ranges: readonly HistoryProviderRange[];
  error_code: HistoryImportFailureCode | null;
};

export type HistoryProviderAdvanceResult = {
  status: "active" | "completed" | "partial" | "failed";
  events: readonly ProjectionEventEnvelope[];
  next_cursor: string | null;
  gap_code: string | null;
  error_code: HistoryImportFailureCode | null;
};

export type HistoryImportProvider = {
  start(owner: HistoryImportProviderOwner): Promise<HistoryProviderStartResult>;
  advance(input: {
    owner: HistoryImportProviderOwner;
    range_id: string;
    source_cursor: string | null;
  }): Promise<HistoryProviderAdvanceResult>;
};

export class HistoryProviderError extends Error {
  constructor(
    readonly code:
      | "runtime_unavailable"
      | "provider_refused"
      | "provider_timeout"
      | "provider_error",
    readonly status?: number,
  ) {
    super(code);
    this.name = "HistoryProviderError";
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const nullableString = (value: unknown): string | null => {
  if (value === null) return null;
  if (isString(value)) return value;
  throw new HistoryProviderError("provider_error");
};

const optionalNullableString = (value: unknown): string | null =>
  value === undefined ? null : nullableString(value);

const parseProviderEvidence = (value: unknown): ProviderEvidence => {
  const parsed = ProviderEvidenceSchema.safeParse(value);
  if (!parsed.success) throw new HistoryProviderError("provider_error");
  return parsed.data;
};

const parseStart = (value: unknown): HistoryProviderStartResult => {
  if (!isRecord(value)) throw new HistoryProviderError("provider_error");
  const availability = value.availability;
  if (
    availability !== "available" &&
    availability !== "blocked" &&
    availability !== "unavailable"
  ) {
    throw new HistoryProviderError("provider_error");
  }
  if (!Array.isArray(value.ranges) || value.ranges.length > 100) {
    throw new HistoryProviderError("provider_error");
  }
  const ranges = value.ranges.map((range): HistoryProviderRange => {
    if (
      !isRecord(range) ||
      !isString(range.start_at) ||
      !isString(range.end_at)
    ) {
      throw new HistoryProviderError("provider_error");
    }
    return {
      start_at: range.start_at,
      end_at: range.end_at,
      source_cursor: nullableString(range.source_cursor),
    };
  });
  const errorCode =
    value.error_code === null || value.error_code === undefined
      ? null
      : HistoryImportFailureCodeSchema.safeParse(value.error_code).success
        ? (value.error_code as HistoryImportFailureCode)
        : (() => {
            throw new HistoryProviderError("provider_error");
          })();
  const providerVersion = optionalNullableString(value.provider_version);
  const proofSource = value.proof_source;
  if (!isString(proofSource)) throw new HistoryProviderError("provider_error");
  const evidence = parseProviderEvidence(value.provider_evidence);
  return {
    availability,
    provider_version: providerVersion,
    proof_source: proofSource,
    provider_evidence: evidence,
    source_start_at: optionalNullableString(value.source_start_at),
    source_end_at: optionalNullableString(value.source_end_at),
    ranges,
    error_code: errorCode,
  };
};

const parseAdvance = (value: unknown): HistoryProviderAdvanceResult => {
  if (!isRecord(value)) throw new HistoryProviderError("provider_error");
  if (
    value.status !== "active" &&
    value.status !== "completed" &&
    value.status !== "partial" &&
    value.status !== "failed"
  ) {
    throw new HistoryProviderError("provider_error");
  }
  if (!Array.isArray(value.events) || value.events.length > 500) {
    throw new HistoryProviderError("provider_error");
  }
  const events: ProjectionEventEnvelope[] = [];
  for (const event of value.events) {
    const parsed = ProjectionEventEnvelopeSchema.safeParse(event);
    if (!parsed.success) throw new HistoryProviderError("provider_error");
    if (parsed.data.event_source !== "backfill") {
      throw new HistoryProviderError("provider_error");
    }
    events.push(parsed.data);
  }
  const errorCode =
    value.error_code === null || value.error_code === undefined
      ? null
      : HistoryImportFailureCodeSchema.safeParse(value.error_code).success
        ? (value.error_code as HistoryImportFailureCode)
        : (() => {
            throw new HistoryProviderError("provider_error");
          })();
  if (!Object.prototype.hasOwnProperty.call(value, "next_cursor"))
    throw new HistoryProviderError("provider_error");
  const nextCursor = nullableString(value.next_cursor);
  if (value.status === "active" && nextCursor === null)
    throw new HistoryProviderError("provider_error");
  if (
    (value.status === "completed" || value.status === "failed") &&
    nextCursor !== null
  )
    throw new HistoryProviderError("provider_error");
  return {
    status: value.status,
    events,
    next_cursor: nextCursor,
    gap_code: optionalNullableString(value.gap_code),
    error_code: errorCode,
  };
};

const validateOwner = (owner: HistoryImportProviderOwner): void => {
  if (
    !isString(owner.tenant_id) ||
    !isString(owner.account_id) ||
    !isString(owner.connection_id) ||
    !isString(owner.identity_id) ||
    !ProviderSchema.safeParse(owner.provider).success ||
    !isString(owner.import_id) ||
    !isString(owner.start_at) ||
    !isString(owner.end_at) ||
    !Number.isSafeInteger(owner.max_events) ||
    owner.max_events < 1
  ) {
    throw new HistoryProviderError("provider_error");
  }
};

/**
 * Private Communicator gateway adapter. These routes are an internal
 * provider-neutral contract; they are not the public mautrix provisioning
 * API. Until the pinned runtime implements them, the adapter reports an
 * unavailable runtime rather than claiming that history import works.
 */
export class HttpHistoryImportProvider implements HistoryImportProvider {
  private readonly baseUrl: string;

  constructor(
    baseUrl: string,
    private readonly sharedSecret: string,
    private readonly fetcher: GatewayFetch,
  ) {
    if (!baseUrl || sharedSecret.length < 16)
      throw new HistoryProviderError("runtime_unavailable");
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(
    path: string,
    body: unknown,
    parse: (value: unknown) => T,
  ): Promise<T> {
    const requestId = crypto.randomUUID();
    const idempotencyMaterial = canonicalJsonStringify({
      path,
      body,
    });
    const idempotencyDigest = await sha256Hex(
      new TextEncoder().encode(idempotencyMaterial),
    );
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": requestId,
          "idempotency-key": `history-${idempotencyDigest}`,
        },
        body: JSON.stringify(body),
      });
    } catch {
      throw new HistoryProviderError("runtime_unavailable");
    }
    if (response.status === 401 || response.status === 403)
      throw new HistoryProviderError("provider_refused", response.status);
    if (response.status === 408 || response.status === 504)
      throw new HistoryProviderError("provider_timeout", response.status);
    if (response.status >= 500)
      throw new HistoryProviderError("runtime_unavailable", response.status);
    if (!response.ok)
      throw new HistoryProviderError("provider_error", response.status);
    let value: unknown;
    try {
      value = await response.json();
    } catch {
      throw new HistoryProviderError("provider_error", response.status);
    }
    return parse(value);
  }

  start(
    owner: HistoryImportProviderOwner,
  ): Promise<HistoryProviderStartResult> {
    validateOwner(owner);
    return this.request("/v1/history-imports/start", owner, parseStart);
  }

  advance(input: {
    owner: HistoryImportProviderOwner;
    range_id: string;
    source_cursor: string | null;
  }): Promise<HistoryProviderAdvanceResult> {
    validateOwner(input.owner);
    if (!isString(input.range_id))
      return Promise.reject(new HistoryProviderError("provider_error"));
    return this.request(
      "/v1/history-imports/advance",
      {
        ...input.owner,
        range_id: input.range_id,
        source_cursor: input.source_cursor,
      },
      parseAdvance,
    );
  }
}

export const historyProviderFromEnv = (
  env: Cloudflare.Env,
): HistoryImportProvider => {
  const runtimeEnv = env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };
  try {
    return new HttpHistoryImportProvider(
      runtimeEnv.CONNECTION_GATEWAY_URL ?? "",
      runtimeEnv.CONNECTION_GATEWAY_TOKEN ?? "",
      gatewayFetchFromEnv(env),
    );
  } catch (error) {
    if (error instanceof GatewayTransportConfigError)
      throw new HistoryProviderError("runtime_unavailable");
    throw error;
  }
};
