import { describe, expect, it, vi } from "vitest";
import { LinkSessionDO, type LinkSessionState } from "../../linking/session";

const stateFor = (
  status: "awaiting_user" | "expired",
  generation: number,
): LinkSessionState => ({
  id: "link_session_alarm",
  tenant_id: "tenant_pilot",
  actor_principal_id: "principal_human",
  membership_id: "membership_human",
  target_identity_id: "identity_human",
  provider: "whatsapp",
  generation,
  status,
  action: status === "expired" ? "none" : "scan_qr",
  expires_at: "2020-01-01T00:00:00.000Z",
  action_expires_at: null,
  gateway_ref: "opaque-gateway-ref",
  connection_id: null,
  lifecycle_operation_id: null,
  provider_login_id: null,
  account_id: null,
  provider_label: null,
  error_code: status === "expired" ? "expired" : null,
  request_key_digest: "a".repeat(64),
  created_at: "2020-01-01T00:00:00.000Z",
  updated_at: "2020-01-01T00:00:00.000Z",
});

const runAlarm = async (initial: LinkSessionState) => {
  let stored: unknown = initial;
  const storage = {
    get: vi.fn(async () => stored),
    put: vi.fn(async (_key: string, value: unknown) => {
      stored = value;
    }),
    setAlarm: vi.fn(async () => undefined),
  };
  const stateCtx = {
    storage,
    blockConcurrencyWhile: async <T>(callback: () => Promise<T>) => callback(),
  } as unknown as DurableObjectState;
  const gatewayFetch = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response("", { status: 200 }),
  );
  const runtimeEnv = {
    CONNECTION_GATEWAY_URL: "https://gateway.example",
    CONNECTION_GATEWAY_TOKEN: "session-test-secret-123",
    CONNECTION_GATEWAY_VPC: { fetch: gatewayFetch },
  } as unknown as Cloudflare.Env;
  const globalFetch = vi.fn();
  vi.stubGlobal("fetch", globalFetch);
  try {
    await new LinkSessionDO(stateCtx, runtimeEnv).alarm();
    return { gatewayFetch, globalFetch, storage, stored };
  } finally {
    vi.unstubAllGlobals();
  }
};

describe("LinkSessionDO private gateway cleanup", () => {
  it.each([
    ["transitions an expired session", stateFor("awaiting_user", 2), 2],
    ["cleans an already expired session", stateFor("expired", 3), 2],
  ])("%s through the VPC binding", async (_label, initial, generation) => {
    const result = await runAlarm(initial);

    expect(result.gatewayFetch).toHaveBeenCalledTimes(1);
    expect(result.globalFetch).not.toHaveBeenCalled();
    const [url, init] = result.gatewayFetch.mock.calls[0] ?? [];
    expect(url).toBe("https://gateway.example/v1/link-sessions/cancel");
    expect(init?.headers).toMatchObject({
      authorization: "Bearer session-test-secret-123",
    });
    expect(JSON.parse(String(init?.body))).toMatchObject({
      gateway_ref: "opaque-gateway-ref",
      generation,
    });
    if (initial.status === "awaiting_user")
      expect(result.stored).toMatchObject({ status: "expired", generation: 3 });
  });
});
