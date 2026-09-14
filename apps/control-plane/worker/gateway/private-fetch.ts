export type GatewayFetch = typeof fetch;

export type GatewayServiceBinding = {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
};

export type GatewayTransportEnv = Cloudflare.Env & {
  CONNECTION_GATEWAY_VPC?: GatewayServiceBinding;
};

export class GatewayTransportConfigError extends Error {
  constructor(readonly code: "missing_binding" | "invalid_binding") {
    super(code);
    this.name = "GatewayTransportConfigError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

const isGatewayServiceBinding = (
  value: unknown,
): value is GatewayServiceBinding =>
  (typeof value === "object" || typeof value === "function") &&
  value !== null &&
  "fetch" in value &&
  typeof value.fetch === "function";

export const gatewayFetchFromEnv = (env: GatewayTransportEnv): GatewayFetch => {
  const binding = env.CONNECTION_GATEWAY_VPC;
  if (binding === undefined)
    throw new GatewayTransportConfigError("missing_binding");
  if (!isGatewayServiceBinding(binding))
    throw new GatewayTransportConfigError("invalid_binding");

  return (input, init) => binding.fetch(input, init);
};
