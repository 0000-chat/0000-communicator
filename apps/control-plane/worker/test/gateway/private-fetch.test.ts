import { describe, expect, it, vi } from "vitest";
import {
  gatewayFetchFromEnv,
  GatewayTransportConfigError,
  type GatewayServiceBinding,
  type GatewayTransportEnv,
} from "../../gateway/private-fetch";

const environment = (binding: unknown): GatewayTransportEnv =>
  ({ CONNECTION_GATEWAY_VPC: binding }) as unknown as GatewayTransportEnv;

describe("private gateway fetch binding", () => {
  it("preserves the binding receiver and forwards the request", async () => {
    const binding: GatewayServiceBinding & { calls: number } = {
      calls: 0,
      async fetch(input, init) {
        this.calls += 1;
        expect(this).toBe(binding);
        expect(input).toBe("http://gateway.internal/v1/ping");
        expect(init).toEqual({ method: "POST" });
        return new Response("ok", { status: 200 });
      },
    };

    const fetcher = gatewayFetchFromEnv(environment(binding));
    await expect(
      fetcher("http://gateway.internal/v1/ping", { method: "POST" }),
    ).resolves.toMatchObject({ status: 200 });
    expect(binding.calls).toBe(1);
  });

  it.each([
    ["missing_binding", environment(undefined)],
    ["invalid_binding", environment(null)],
    ["invalid_binding", environment({ fetch: "not-a-function" })],
  ] as const)("fails closed for %s", (code, env) => {
    const globalFetch = vi.fn();
    vi.stubGlobal("fetch", globalFetch);
    try {
      expect(() => gatewayFetchFromEnv(env)).toThrowError(
        new GatewayTransportConfigError(code),
      );
      expect(globalFetch).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
