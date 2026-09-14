import { env } from "cloudflare:test";
import type { HistoryImportRange } from "@communicator/contracts";
import { parse as parseJsonc } from "jsonc-parser";
import { beforeEach, describe, expect, it, vi } from "vitest";
import configText from "../../../wrangler.jsonc?raw";
import worker from "../../index";
import { getDetail } from "../../history/repository";
import {
  HistoryProviderError,
  type HistoryImportProvider,
  type HistoryProviderAdvanceResult,
  type HistoryProviderStartResult,
} from "../../history/provider";
import { createHistoryService } from "../../history/service";
import { runHistoryImportTick } from "../../history/runner";
import {
  clearDirectory,
  seedAccountAccess,
  seedDirectory,
} from "../support/directory-fixtures";

const workerEnv = env as typeof env & { CONTROL_DB: D1Database };
const tenantId = "tenant_pilot";
const accountId = "account_human";
const identityId = "identity_human";
const rangeStart = "2026-09-01T00:00:00.000Z";
const firstRangeEnd = "2026-09-02T00:00:00.000Z";
const rangeEnd = "2026-09-03T00:00:00.000Z";

const providerEvidence = {
  provider_version: "v26.08",
  proof_source: "controlled-history-scheduler-test",
  summary: "Controlled scheduler test response",
  observed_at: "2026-09-03T00:00:00.000Z",
};

const startResult = (count: 1 | 2 = 2): HistoryProviderStartResult => ({
  availability: "available",
  provider_version: "v26.08",
  proof_source: "controlled-history-scheduler-test",
  provider_evidence: providerEvidence,
  source_start_at: rangeStart,
  source_end_at: rangeEnd,
  ranges:
    count === 1
      ? [
          {
            start_at: rangeStart,
            end_at: rangeEnd,
            source_cursor: "opaque-single",
          },
        ]
      : [
          {
            start_at: rangeStart,
            end_at: firstRangeEnd,
            source_cursor: "opaque-first",
          },
          {
            start_at: firstRangeEnd,
            end_at: rangeEnd,
            source_cursor: "opaque-second",
          },
        ],
  error_code: null,
});

const completedPage = (): HistoryProviderAdvanceResult => ({
  status: "completed",
  events: [],
  next_cursor: null,
  gap_code: null,
  error_code: null,
});

const providerWith = (
  advance: HistoryImportProvider["advance"],
  count: 1 | 2 = 2,
): HistoryImportProvider => ({
  start: async () => startResult(count),
  advance,
});

const makeClock = (initial = "2026-09-03T00:00:10.000Z") => {
  let timestamp = Date.parse(initial);
  return {
    now: () => new Date(timestamp),
    advance: (milliseconds: number) => {
      timestamp += milliseconds;
    },
  };
};

const startImport = async (
  provider: HistoryImportProvider,
  clock: ReturnType<typeof makeClock>,
  idempotencyKey: string,
) => {
  const service = createHistoryService({
    provider,
    now: clock.now,
    applyEvents: async () => undefined,
  });
  const detail = await service.start({
    env: workerEnv,
    tenantId,
    accountId,
    identityId,
    idempotencyKey,
    startAt: rangeStart,
    endAt: rangeEnd,
    maxEvents: 50,
  });
  return { service, detail };
};

type SchedulerRow = Pick<
  HistoryImportRange,
  "range_id" | "status" | "source_cursor"
> & {
  next_attempt_at: string | null;
  lease_token: string | null;
  lease_until: string | null;
  operation_key: string | null;
};

const readSchedulerRow = async (rangeId: string): Promise<SchedulerRow> => {
  const row = await workerEnv.CONTROL_DB.prepare(
    `SELECT range_id, status, source_cursor, next_attempt_at,
              lease_token, lease_until, operation_key
         FROM history_import_ranges
        WHERE range_id = ?`,
  )
    .bind(rangeId)
    .first<SchedulerRow>();
  if (row === null) throw new Error(`missing range ${rangeId}`);
  return row;
};

beforeEach(async () => {
  await clearDirectory(workerEnv.CONTROL_DB);
  await seedDirectory(workerEnv.CONTROL_DB);
  await seedAccountAccess(workerEnv.CONTROL_DB);
});

describe("background history import runner", () => {
  it("advances legacy NULL wakes without a browser", async () => {
    const clock = makeClock();
    const calls: string[] = [];
    const provider = providerWith(async ({ range_id }) => {
      calls.push(range_id);
      return completedPage();
    });
    const started = await startImport(provider, clock, "scheduler-null-wake");

    await workerEnv.CONTROL_DB.prepare(
      `UPDATE history_import_ranges
            SET next_attempt_at = NULL, lease_token = NULL,
                lease_until = NULL, operation_key = NULL
          WHERE import_id = ?`,
    )
      .bind(started.detail.import.import_id)
      .run();

    const result = await runHistoryImportTick(workerEnv, {
      createProvider: () => provider,
      now: clock.now,
      applyEvents: async () => undefined,
    });
    const detail = await getDetail(
      workerEnv.CONTROL_DB,
      tenantId,
      started.detail.import.import_id,
      accountId,
    );

    expect(result).toEqual({ claimed: 2, advanced: 2 });
    expect(new Set(calls)).toHaveProperty("size", 2);
    expect(detail.import.status).toBe("completed");
    expect(detail.ranges.every((range) => range.status === "completed")).toBe(
      true,
    );
  });

  it("continues a sibling while preserving a failed range's retry wake", async () => {
    const clock = makeClock();
    const calls: string[] = [];
    let siblingPages = 0;
    let retryRangeId = "";
    const provider = providerWith(async ({ range_id }) => {
      calls.push(range_id);
      if (range_id === retryRangeId) {
        throw new HistoryProviderError("runtime_unavailable");
      }
      siblingPages += 1;
      return siblingPages < 3
        ? {
            status: "active",
            events: [],
            next_cursor: `opaque-sibling-${siblingPages}`,
            gap_code: null,
            error_code: null,
          }
        : completedPage();
    });
    const started = await startImport(provider, clock, "scheduler-sibling");
    const first = started.detail.ranges[0];
    expect(first).toBeDefined();
    retryRangeId = first?.range_id ?? "";

    const result = await runHistoryImportTick(workerEnv, {
      createProvider: () => provider,
      now: clock.now,
      maxPagesPerTick: 10,
      applyEvents: async () => undefined,
    });
    const detail = await getDetail(
      workerEnv.CONTROL_DB,
      tenantId,
      started.detail.import.import_id,
      accountId,
    );
    const retryRow = await readSchedulerRow(retryRangeId);

    expect(result.claimed).toBe(4);
    expect(result.advanced).toBe(4);
    expect(siblingPages).toBe(3);
    expect(detail.import.status).toBe("active");
    expect(
      detail.ranges.find((range) => range.range_id === retryRangeId),
    ).toMatchObject({
      status: "active",
      error_code: "runtime_unavailable",
      attempt_count: 1,
    });
    expect(
      detail.ranges.filter((range) => range.status === "completed"),
    ).toHaveLength(1);
    expect(retryRow.next_attempt_at).toBe("2026-09-03T00:01:10.000Z");
    expect(retryRow.lease_token).toBeNull();
    expect(calls).toHaveLength(4);
  });

  it("fences overlapping ticks and manual replay after a lease expires", async () => {
    const clock = makeClock();
    let callCount = 0;
    let enteredFirst!: () => void;
    const firstEntered = new Promise<void>((resolve) => {
      enteredFirst = resolve;
    });
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const provider = providerWith(async () => {
      callCount += 1;
      if (callCount === 1) {
        enteredFirst();
        await firstGate;
      }
      return completedPage();
    }, 1);
    const started = await startImport(provider, clock, "scheduler-fencing");
    const range = started.detail.ranges[0];
    expect(range).toBeDefined();
    if (range === undefined) throw new Error("missing scheduler range");
    const errorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);

    try {
      const firstTick = runHistoryImportTick(workerEnv, {
        createProvider: () => provider,
        now: clock.now,
        maxPagesPerTick: 1,
        leaseDurationMs: 1,
        applyEvents: async () => undefined,
      });
      await firstEntered;

      const manualReplay = await started.service.advance({
        env: workerEnv,
        tenantId,
        importId: started.detail.import.import_id,
        accountId,
        identityId,
        rangeId: range.range_id,
      });
      expect(manualReplay.ranges[0]?.status).toBe("active");
      expect(callCount).toBe(1);

      const overlappingTick = await runHistoryImportTick(workerEnv, {
        createProvider: () => provider,
        now: clock.now,
        maxPagesPerTick: 1,
        applyEvents: async () => undefined,
      });
      expect(overlappingTick).toEqual({ claimed: 0, advanced: 0 });

      clock.advance(10);
      const expiredLease = await readSchedulerRow(range.range_id);
      expect(Date.parse(expiredLease.lease_until ?? "")).toBeLessThanOrEqual(
        Date.parse(clock.now().toISOString()),
      );
      const replacementTick = await runHistoryImportTick(workerEnv, {
        createProvider: () => provider,
        now: clock.now,
        maxPagesPerTick: 1,
        applyEvents: async () => undefined,
      });
      expect(replacementTick).toEqual({ claimed: 1, advanced: 1 });

      releaseFirst();
      const staleTick = await firstTick;
      expect(staleTick).toEqual({ claimed: 1, advanced: 0 });
      expect(callCount).toBe(2);

      const row = await readSchedulerRow(range.range_id);
      expect(row.status).toBe("completed");
      expect(row.lease_token).toBeNull();
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("leaves a durable retry wake after an apply crash and resumes later", async () => {
    const clock = makeClock();
    let applyCount = 0;
    const provider = providerWith(async () => completedPage(), 1);
    const applyEvents = async () => {
      applyCount += 1;
      if (applyCount === 1) throw new Error("projection process lost");
    };
    const started = await startImport(provider, clock, "scheduler-restart");
    const firstTick = await runHistoryImportTick(workerEnv, {
      createProvider: () => provider,
      now: clock.now,
      maxPagesPerTick: 1,
      applyEvents,
    });
    const retryDetail = await getDetail(
      workerEnv.CONTROL_DB,
      tenantId,
      started.detail.import.import_id,
      accountId,
    );
    const retryRange = retryDetail.ranges[0];
    expect(firstTick).toEqual({ claimed: 1, advanced: 1 });
    expect(retryRange).toMatchObject({
      status: "active",
      attempt_count: 1,
      error_code: "provider_error",
    });
    expect(
      (await readSchedulerRow(retryRange?.range_id ?? "")).next_attempt_at,
    ).toBe("2026-09-03T00:01:10.000Z");

    clock.advance(60_000);
    const resumed = await runHistoryImportTick(workerEnv, {
      createProvider: () => provider,
      now: clock.now,
      maxPagesPerTick: 1,
      applyEvents,
    });
    const detail = await getDetail(
      workerEnv.CONTROL_DB,
      tenantId,
      started.detail.import.import_id,
      accountId,
    );
    expect(resumed).toEqual({ claimed: 1, advanced: 1 });
    expect(applyCount).toBe(2);
    expect(detail.import.status).toBe("completed");
  });

  it("runs the exported scheduled handler and declares every cron environment", async () => {
    const config = parseJsonc(configText) as {
      triggers?: { crons?: string[] };
      env?: Record<string, { triggers?: { crons?: string[] } }>;
    };
    expect(config.triggers?.crons).toEqual(["*/1 * * * *"]);
    expect(config.env?.staging?.triggers?.crons).toEqual(["*/1 * * * *"]);
    expect(config.env?.production?.triggers?.crons).toEqual(["*/1 * * * *"]);

    const clock = makeClock();
    const provider = providerWith(async () => completedPage(), 1);
    const started = await startImport(provider, clock, "scheduler-exported");
    const gatewayFetch = vi.fn(
      async () =>
        new Response(JSON.stringify(completedPage()), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const scheduledEnv = Object.assign(Object.create(workerEnv), {
      CONNECTION_GATEWAY_URL: "https://history-gateway.example",
      CONNECTION_GATEWAY_TOKEN: "history-scheduler-secret-123",
      CONNECTION_GATEWAY_VPC: { fetch: gatewayFetch },
    }) as Cloudflare.Env;
    let scheduledPromise: Promise<unknown> | undefined;

    expect(worker.scheduled).toBeTypeOf("function");
    worker.scheduled?.(
      {
        cron: "*/1 * * * *",
        scheduledTime: Date.parse("2026-09-03T00:00:10.000Z"),
        noRetry() {},
      },
      scheduledEnv,
      {
        waitUntil(promise: Promise<unknown>) {
          scheduledPromise = promise;
        },
      } as unknown as ExecutionContext,
    );
    expect(scheduledPromise).toBeDefined();
    await scheduledPromise;

    const detail = await getDetail(
      workerEnv.CONTROL_DB,
      tenantId,
      started.detail.import.import_id,
      accountId,
    );
    expect(gatewayFetch).toHaveBeenCalledTimes(1);
    expect(detail.import.status).toBe("completed");
  });
});
