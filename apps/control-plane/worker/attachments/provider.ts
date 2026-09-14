import { MAX_ATTACHMENT_BYTES, type Provider } from "@communicator/contracts";
import { canonicalJsonStringify } from "../archive/canonical-json";
import { sha256Hex } from "../archive/codec";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayFetch,
} from "../gateway/private-fetch";

export type AttachmentProviderInput = {
  tenant_id: string;
  account_id: string;
  connection_id: string;
  identity_id: string;
  conversation_id: string;
  message_id: string;
  attachment_id: string;
  revision: string;
  provider: Provider;
  media_key: string;
  expected_size_bytes: number | null;
  expected_sha256: string | null;
  expected_mime_type: string | null;
};

export type AttachmentProviderResult =
  | {
      status: "available";
      bytes: Uint8Array;
      mime_type: string;
      sha256: string;
    }
  | {
      status: "unavailable";
      reason:
        | "expired"
        | "missing"
        | "provider_rejected"
        | "provider_timeout"
        | "provider_unavailable"
        | "malformed";
    };

export type AttachmentProvider = {
  read(input: AttachmentProviderInput): Promise<AttachmentProviderResult>;
};

export class AttachmentProviderError extends Error {
  constructor(
    readonly code:
      | "runtime_unavailable"
      | "provider_refused"
      | "provider_timeout"
      | "provider_error",
    readonly status?: number,
  ) {
    super(code);
    this.name = "AttachmentProviderError";
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const validId = (value: unknown): value is string =>
  typeof value === "string" && /^[a-z]+_[a-z0-9_]+$/u.test(value);

const validateInput = (input: AttachmentProviderInput): void => {
  if (
    !validId(input.tenant_id) ||
    !validId(input.account_id) ||
    !validId(input.connection_id) ||
    !validId(input.identity_id) ||
    !validId(input.conversation_id) ||
    !validId(input.message_id) ||
    !validId(input.attachment_id) ||
    !input.revision ||
    !input.media_key ||
    (input.expected_size_bytes !== null &&
      !Number.isSafeInteger(input.expected_size_bytes)) ||
    (input.expected_size_bytes !== null &&
      (input.expected_size_bytes < 0 ||
        input.expected_size_bytes > MAX_ATTACHMENT_BYTES))
  ) {
    throw new AttachmentProviderError("provider_error");
  }
  if (!/^media\/[a-z]+_[a-z0-9_]+\/[0-9a-f]{64}$/u.test(input.media_key)) {
    throw new AttachmentProviderError("provider_error");
  }
};

const decodeBase64 = (value: string): Uint8Array => {
  if (
    !/^[A-Za-z0-9+/]*={0,2}$/u.test(value) ||
    value.length > Math.ceil((MAX_ATTACHMENT_BYTES / 3) * 4) + 4
  ) {
    throw new AttachmentProviderError("provider_error");
  }
  try {
    const binary = atob(value);
    if (binary.length > MAX_ATTACHMENT_BYTES) {
      throw new AttachmentProviderError("provider_error");
    }
    return Uint8Array.from(binary, (character) => character.charCodeAt(0));
  } catch (error) {
    if (error instanceof AttachmentProviderError) throw error;
    throw new AttachmentProviderError("provider_error");
  }
};

const parseResult = async (
  value: unknown,
): Promise<AttachmentProviderResult> => {
  if (!isRecord(value)) throw new AttachmentProviderError("provider_error");
  if (value.status === "unavailable") {
    const reason = value.reason;
    if (
      reason !== "expired" &&
      reason !== "missing" &&
      reason !== "provider_rejected" &&
      reason !== "provider_timeout" &&
      reason !== "provider_unavailable" &&
      reason !== "malformed"
    ) {
      throw new AttachmentProviderError("provider_error");
    }
    return { status: "unavailable", reason };
  }
  if (
    value.status !== "available" ||
    typeof value.bytes_base64 !== "string" ||
    typeof value.mime_type !== "string" ||
    typeof value.sha256 !== "string" ||
    !Number.isSafeInteger(value.size_bytes)
  ) {
    throw new AttachmentProviderError("provider_error");
  }
  const bytes = decodeBase64(value.bytes_base64);
  if (
    value.size_bytes !== bytes.byteLength ||
    bytes.byteLength > MAX_ATTACHMENT_BYTES
  ) {
    throw new AttachmentProviderError("provider_error");
  }
  if (
    !/^[\w!#$&^.+-]+\/[\w!#$&^.+-]+(?:;[\x20-\x7e]+)?$/u.test(
      value.mime_type,
    ) ||
    !/^[0-9a-f]{64}$/u.test(value.sha256)
  ) {
    throw new AttachmentProviderError("provider_error");
  }
  const computed = await sha256Hex(bytes);
  if (computed !== value.sha256) {
    throw new AttachmentProviderError("provider_error");
  }
  return {
    status: "available",
    bytes,
    mime_type: value.mime_type,
    sha256: value.sha256,
  };
};

/**
 * Private provider boundary. The request contains only stable Communicator
 * ownership IDs, a bounded internal media key, and the current revision. It
 * never accepts or forwards an upstream URL or provider credential.
 */
export class HttpAttachmentProvider implements AttachmentProvider {
  private readonly baseUrl: string;

  constructor(
    baseUrl: string,
    private readonly sharedSecret: string,
    private readonly fetcher: GatewayFetch,
  ) {
    if (!baseUrl || sharedSecret.length < 16) {
      throw new AttachmentProviderError("runtime_unavailable");
    }
    this.baseUrl = baseUrl.replace(/\/$/u, "");
  }

  async read(
    input: AttachmentProviderInput,
  ): Promise<AttachmentProviderResult> {
    validateInput(input);
    const requestId = crypto.randomUUID();
    const idempotencyDigest = await sha256Hex(
      new TextEncoder().encode(canonicalJsonStringify(input)),
    );
    let response: Response;
    try {
      response = await this.fetcher(`${this.baseUrl}/v1/attachments/read`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.sharedSecret}`,
          "content-type": "application/json",
          "cache-control": "no-store",
          "x-request-id": requestId,
          "idempotency-key": `attachment-${idempotencyDigest}`,
        },
        body: JSON.stringify(input),
      });
    } catch {
      return { status: "unavailable", reason: "provider_unavailable" };
    }
    if (response.status === 408 || response.status === 504) {
      return { status: "unavailable", reason: "provider_timeout" };
    }
    if (response.status === 401 || response.status === 403) {
      return { status: "unavailable", reason: "provider_rejected" };
    }
    if (response.status === 404 || response.status === 410) {
      return { status: "unavailable", reason: "missing" };
    }
    if (!response.ok) {
      throw new AttachmentProviderError("provider_error", response.status);
    }
    let body: unknown;
    try {
      const text = await response.text();
      if (text.length > 12_000_000) {
        throw new AttachmentProviderError("provider_error");
      }
      body = JSON.parse(text);
    } catch (error) {
      if (error instanceof AttachmentProviderError) throw error;
      throw new AttachmentProviderError("provider_error");
    }
    return parseResult(body);
  }
}

export const attachmentProviderFromEnv = (
  env: Cloudflare.Env,
): AttachmentProvider => {
  const runtimeEnv = env as Cloudflare.Env & {
    CONNECTION_GATEWAY_URL?: string;
    CONNECTION_GATEWAY_TOKEN?: string;
  };
  try {
    return new HttpAttachmentProvider(
      runtimeEnv.CONNECTION_GATEWAY_URL ?? "",
      runtimeEnv.CONNECTION_GATEWAY_TOKEN ?? "",
      gatewayFetchFromEnv(env),
    );
  } catch (error) {
    if (
      error instanceof AttachmentProviderError ||
      error instanceof GatewayTransportConfigError
    ) {
      return {
        read: async () => ({
          status: "unavailable",
          reason: "provider_unavailable",
        }),
      };
    }
    throw error;
  }
};
