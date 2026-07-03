import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  decodePiAvailableModelsResponseDataExit,
  parsePiApprovalTitle,
  parsePiModelSlug,
  PI_APPROVAL_EXTENSION_SOURCE,
  PI_APPROVAL_OPTION_ALLOW,
  PI_APPROVAL_OPTION_ALLOW_ALWAYS,
  PI_APPROVAL_OPTION_DENY,
  PI_APPROVAL_TITLE_PREFIX,
  spawnPiRpcSession,
  toPiApprovalSelection,
} from "./piRuntime.ts";

const encoder = new TextEncoder();

function makeHandle(input: {
  readonly stdout?: Stream.Stream<Uint8Array>;
  readonly stderr?: string;
  readonly exitCode?: Effect.Effect<number>;
}) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: (input.exitCode ?? Effect.never).pipe(
      Effect.map((code) => ChildProcessSpawner.ExitCode(code)),
    ),
    isRunning: Effect.succeed(true),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: input.stdout ?? Stream.empty,
    stderr: input.stderr ? Stream.encodeText(Stream.make(input.stderr)) : Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function spawnWithFakeProcess<A, E, R>(
  effect: (input: {
    readonly stdout: Queue.Queue<Uint8Array>;
    readonly exitDeferred: Deferred.Deferred<number>;
  }) => Effect.Effect<A, E, R>,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array>();
      const exitDeferred = yield* Deferred.make<number>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            stdout: Stream.fromQueue(stdout),
            exitCode: Deferred.await(exitDeferred),
          }),
        ),
      );
      return yield* effect({ stdout, exitDeferred }).pipe(
        Effect.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
      );
    }),
  );
}

function responseLine(value: unknown): Uint8Array {
  return encoder.encode(`${JSON.stringify(value)}\n`);
}

describe("parsePiModelSlug", () => {
  it("splits provider and model id on the first slash", () => {
    expect(parsePiModelSlug("anthropic/claude-sonnet-5")).toEqual({
      provider: "anthropic",
      modelId: "claude-sonnet-5",
    });
  });

  it("keeps slashes inside the model id (openrouter-style ids)", () => {
    expect(parsePiModelSlug("openrouter/qwen/qwen3-coder")).toEqual({
      provider: "openrouter",
      modelId: "qwen/qwen3-coder",
    });
  });

  it("rejects slugs without a provider segment", () => {
    expect(parsePiModelSlug("claude-sonnet-5")).toBeNull();
    expect(parsePiModelSlug("/model")).toBeNull();
    expect(parsePiModelSlug("provider/")).toBeNull();
    expect(parsePiModelSlug(undefined)).toBeNull();
  });
});

describe("decodePiAvailableModelsResponseDataExit", () => {
  it("keeps structured models and skips malformed entries", () => {
    const decoded = decodePiAvailableModelsResponseDataExit({
      models: [
        {
          provider: "anthropic",
          id: "claude-sonnet-5",
          name: "Claude Sonnet 5",
          reasoning: true,
          contextWindow: 1_000_000,
          maxTokens: 128_000,
        },
        { provider: "anthropic", name: "missing id" },
        { id: "missing-provider" },
        "noise",
      ],
    });

    expect(Exit.isSuccess(decoded)).toBe(true);
    if (Exit.isSuccess(decoded)) {
      expect(decoded.value.models).toEqual([
        {
          provider: "anthropic",
          id: "claude-sonnet-5",
          name: "Claude Sonnet 5",
          reasoning: true,
          contextWindow: 1_000_000,
          maxTokens: 128_000,
        },
      ]);
    }
  });
});

describe("spawnPiRpcSession", () => {
  it.effect("uses a non-empty fallback detail when Pi returns an empty error string", () =>
    spawnWithFakeProcess(({ stdout }) =>
      Effect.gen(function* () {
        const handle = yield* spawnPiRpcSession({
          binaryPath: "pi",
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const requestFiber = yield* handle
          .request({ type: "do_work" })
          .pipe(Effect.flip, Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Queue.offer(
          stdout,
          responseLine({
            type: "response",
            id: "t3-1",
            command: "do_work",
            success: false,
            error: "",
          }),
        );

        const error = yield* Fiber.join(requestFiber);

        expect(error.detail).toBe("Pi command 'do_work' failed.");
      }),
    ),
  );

  it.effect("fails requests immediately when JSON command encoding fails", () =>
    spawnWithFakeProcess(() =>
      Effect.gen(function* () {
        const handle = yield* spawnPiRpcSession({
          binaryPath: "pi",
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const error = yield* handle.request({ type: "bigint", value: 1n }).pipe(Effect.flip);

        expect(error.detail).toBe("Failed to encode Pi RPC command 'bigint' as JSON.");
      }),
    ),
  );

  it.effect("fails in-flight requests when the Pi process exits before replying", () =>
    spawnWithFakeProcess(({ exitDeferred }) =>
      Effect.gen(function* () {
        const handle = yield* spawnPiRpcSession({
          binaryPath: "pi",
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const requestFiber = yield* handle
          .request({ type: "get_state" })
          .pipe(Effect.flip, Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Deferred.succeed(exitDeferred, 7);

        const error = yield* Fiber.join(requestFiber);

        expect(error.detail).toBe("Pi RPC process exited before replying (exit code 7).");
      }),
    ),
  );
});

describe("parsePiApprovalTitle", () => {
  it("extracts the tool and detail from a marker title", () => {
    const title = `${PI_APPROVAL_TITLE_PREFIX}{"tool":"bash","detail":"rm -rf /tmp/x"}`;
    expect(parsePiApprovalTitle(title)).toEqual({ tool: "bash", detail: "rm -rf /tmp/x" });
  });

  it("keeps the injected extension marker parseable by the adapter", () => {
    expect(PI_APPROVAL_EXTENSION_SOURCE).toContain(PI_APPROVAL_TITLE_PREFIX);
    expect(PI_APPROVAL_EXTENSION_SOURCE).toContain(PI_APPROVAL_OPTION_ALLOW);
    expect(PI_APPROVAL_EXTENSION_SOURCE).toContain(PI_APPROVAL_OPTION_ALLOW_ALWAYS);
    expect(PI_APPROVAL_EXTENSION_SOURCE).toContain(PI_APPROVAL_OPTION_DENY);
    expect(
      parsePiApprovalTitle(`${PI_APPROVAL_TITLE_PREFIX}{"tool":"write","detail":"src/app.ts"}`),
    ).toEqual({ tool: "write", detail: "src/app.ts" });
  });

  it("returns null for regular dialog titles and malformed markers", () => {
    expect(parsePiApprovalTitle("Allow dangerous command?")).toBeNull();
    expect(parsePiApprovalTitle(`${PI_APPROVAL_TITLE_PREFIX}not-json`)).toBeNull();
    expect(parsePiApprovalTitle(undefined)).toBeNull();
  });
});

describe("toPiApprovalSelection", () => {
  it("maps decisions onto the extension's select options", () => {
    expect(toPiApprovalSelection("accept")).toBe("allow");
    expect(toPiApprovalSelection("acceptForSession")).toBe("allow-always");
    expect(toPiApprovalSelection("decline")).toBe("deny");
    expect(toPiApprovalSelection("cancel")).toBeNull();
  });
});
