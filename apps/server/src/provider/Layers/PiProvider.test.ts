import * as NodeAssert from "node:assert/strict";

import { it } from "@effect/vitest";
import { PiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import { beforeEach } from "vite-plus/test";

import {
  PiRuntime,
  PiRuntimeError,
  type PiRpcEvent,
  type PiRpcHandle,
  type PiRuntimeShape,
  type SpawnPiRpcInput,
} from "../piRuntime.ts";
import { buildInitialPiProviderSnapshot, checkPiProviderStatus } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const runtimeMock = {
  state: {
    calls: [] as Array<{ binaryPath: string; args: ReadonlyArray<string> }>,
    spawnInputs: [] as Array<SpawnPiRpcInput>,
    requests: [] as Array<Record<string, unknown>>,
    closeCalls: 0,
    versionResult: { stdout: "pi 0.4.1\n", stderr: "", code: 0 },
    modelsData: {
      models: [
        {
          provider: "anthropic",
          id: "claude-sonnet-5",
          name: "Claude Sonnet 5",
          reasoning: true,
        },
        {
          provider: "anthropic",
          id: "claude-haiku-4-5",
          name: "Claude Haiku 4.5",
          reasoning: false,
        },
        {
          provider: "openai-codex",
          id: "gpt-5-codex",
          name: "GPT-5 Codex",
          reasoning: true,
        },
      ],
    } as unknown,
    versionError: null as PiRuntimeError | null,
    modelsError: null as PiRuntimeError | null,
  },
  reset() {
    this.state.calls.length = 0;
    this.state.spawnInputs.length = 0;
    this.state.requests.length = 0;
    this.state.closeCalls = 0;
    this.state.versionResult = { stdout: "pi 0.4.1\n", stderr: "", code: 0 };
    this.state.modelsData = {
      models: [
        {
          provider: "anthropic",
          id: "claude-sonnet-5",
          name: "Claude Sonnet 5",
          reasoning: true,
        },
        {
          provider: "anthropic",
          id: "claude-haiku-4-5",
          name: "Claude Haiku 4.5",
          reasoning: false,
        },
        {
          provider: "openai-codex",
          id: "gpt-5-codex",
          name: "GPT-5 Codex",
          reasoning: true,
        },
      ],
    };
    this.state.versionError = null;
    this.state.modelsError = null;
  },
};

const PiRuntimeTestDouble: PiRuntimeShape = {
  spawnSession: (input) =>
    Effect.gen(function* () {
      runtimeMock.state.spawnInputs.push(input);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls += 1;
        }),
      );
      const events = yield* Queue.unbounded<PiRpcEvent>();
      const handle: PiRpcHandle = {
        request: (command) =>
          Effect.gen(function* () {
            runtimeMock.state.requests.push(command);
            if (command.type === "get_available_models") {
              if (runtimeMock.state.modelsError) return yield* runtimeMock.state.modelsError;
              return {
                type: "response",
                command: "get_available_models",
                success: true,
                data: runtimeMock.state.modelsData,
              };
            }
            return yield* new PiRuntimeError({
              operation: "request",
              detail: `Unexpected Pi RPC command: ${String(command.type)}`,
            });
          }),
        notify: () => Effect.void,
        events,
        exitCode: Effect.never,
        stderr: Effect.succeed(""),
      };
      return handle;
    }),
  runCommand: (input) =>
    Effect.gen(function* () {
      runtimeMock.state.calls.push({ binaryPath: input.binaryPath, args: input.args });
      const command = input.args[0];
      if (command === "--version") {
        if (runtimeMock.state.versionError) return yield* runtimeMock.state.versionError;
        return runtimeMock.state.versionResult;
      }
      return yield* new PiRuntimeError({
        operation: "runCommand",
        detail: `Unexpected Pi command: ${input.args.join(" ")}`,
      });
    }),
};

const PiProviderTestLayer = Layer.succeed(PiRuntime, PiRuntimeTestDouble);

beforeEach(() => {
  runtimeMock.reset();
});

it.effect(
  "buildInitialPiProviderSnapshot returns a disabled snapshot when settings.enabled is false",
  () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({ enabled: false }));
      NodeAssert.equal(snapshot.enabled, false);
      NodeAssert.equal(snapshot.status, "disabled");
      NodeAssert.equal(snapshot.badgeLabel, "Early Access");
      NodeAssert.match(snapshot.message ?? "", /disabled/);
    }),
);

it.effect("buildInitialPiProviderSnapshot returns a pending snapshot by default", () =>
  Effect.gen(function* () {
    const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({}));
    NodeAssert.equal(snapshot.enabled, true);
    NodeAssert.equal(snapshot.status, "warning");
    NodeAssert.equal(snapshot.badgeLabel, "Early Access");
    NodeAssert.match(snapshot.message ?? "", /not been checked/);
  }),
);

it.effect("buildInitialPiProviderSnapshot includes configured custom models", () =>
  Effect.gen(function* () {
    const snapshot = yield* buildInitialPiProviderSnapshot(
      decodePiSettings({ customModels: ["custom/pi-model"] }),
    );

    NodeAssert.equal(snapshot.status, "warning");
    NodeAssert.ok(snapshot.models.some((model) => model.slug === "custom/pi-model"));
  }),
);

it.layer(PiProviderTestLayer)("checkPiProviderStatus", (it) => {
  it.effect("skips runtime probes when Pi is disabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ enabled: false }));

      NodeAssert.equal(snapshot.enabled, false);
      NodeAssert.equal(snapshot.status, "disabled");
      NodeAssert.deepEqual(runtimeMock.state.calls, []);
    }),
  );

  it.effect("reports a missing binary from the runtime error detail", () =>
    Effect.gen(function* () {
      runtimeMock.state.versionError = new PiRuntimeError({
        operation: "runCommand",
        detail: "spawn pi ENOENT",
      });

      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));

      NodeAssert.equal(snapshot.enabled, true);
      NodeAssert.equal(snapshot.installed, false);
      NodeAssert.equal(snapshot.status, "error");
      NodeAssert.equal(snapshot.message, "Pi CLI (`pi`) is not installed or not on PATH.");
    }),
  );

  it.effect("reports model discovery failures after a successful version probe", () =>
    Effect.gen(function* () {
      runtimeMock.state.modelsError = new PiRuntimeError({
        operation: "runCommand",
        detail: "model list failed",
      });

      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));

      NodeAssert.equal(snapshot.enabled, true);
      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "error");
      NodeAssert.equal(snapshot.version, "0.4.1");
      NodeAssert.equal(
        snapshot.message,
        "Failed to execute Pi CLI health check: model list failed",
      );
      NodeAssert.deepEqual(
        runtimeMock.state.calls.map((call) => call.args),
        [["--version"]],
      );
      NodeAssert.deepEqual(
        runtimeMock.state.spawnInputs.map((input) => input.noSession),
        [true],
      );
      NodeAssert.deepEqual(
        runtimeMock.state.spawnInputs.map((input) => input.noTools),
        [true],
      );
      NodeAssert.deepEqual(
        runtimeMock.state.requests.map((command) => command.type),
        ["get_available_models"],
      );
    }),
  );

  it.effect("does not list models when Pi version output cannot be parsed", () =>
    Effect.gen(function* () {
      runtimeMock.state.versionResult = { stdout: "pi dev build\n", stderr: "", code: 0 };

      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));

      NodeAssert.equal(snapshot.status, "error");
      NodeAssert.equal(snapshot.version, null);
      NodeAssert.equal(
        snapshot.message,
        "Failed to execute Pi CLI health check: Unable to determine Pi version from `pi --version` output.",
      );
      NodeAssert.deepEqual(
        runtimeMock.state.calls.map((call) => call.args),
        [["--version"]],
      );
    }),
  );

  it.effect("discovers models from RPC and exposes mapped thinking capabilities", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));
      const slugs = snapshot.models.map((model) => model.slug);

      NodeAssert.equal(snapshot.enabled, true);
      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "ready");
      NodeAssert.equal(snapshot.version, "0.4.1");
      NodeAssert.equal(snapshot.badgeLabel, "Early Access");
      NodeAssert.deepEqual(slugs, [
        "anthropic/claude-haiku-4-5",
        "anthropic/claude-sonnet-5",
        "openai-codex/gpt-5-codex",
      ]);
      NodeAssert.equal(runtimeMock.state.closeCalls, 1);

      const model = snapshot.models.find((entry) => entry.slug === "anthropic/claude-sonnet-5");
      const thinking = model?.capabilities?.optionDescriptors?.find(
        (descriptor) => descriptor.id === "thinking" && descriptor.type === "select",
      );
      NodeAssert.ok(thinking && thinking.type === "select");
      NodeAssert.equal(thinking.currentValue, "medium");
      NodeAssert.deepEqual(
        thinking.options.map((option) => option.id),
        ["off", "minimal", "low", "medium", "high"],
      );

      const codexModel = snapshot.models.find((entry) => entry.slug === "openai-codex/gpt-5-codex");
      const codexThinking = codexModel?.capabilities?.optionDescriptors?.find(
        (descriptor) => descriptor.id === "thinking" && descriptor.type === "select",
      );
      NodeAssert.ok(codexThinking && codexThinking.type === "select");
      NodeAssert.ok(codexThinking.options.some((option) => option.id === "xhigh"));
    }),
  );

  it.effect("returns a warning when model discovery succeeds with no models", () =>
    Effect.gen(function* () {
      runtimeMock.state.modelsData = { models: [] };

      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));

      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "warning");
      NodeAssert.match(snapshot.message ?? "", /reported no models/);
    }),
  );

  it.effect("keeps custom Pi models in error and success snapshots", () =>
    Effect.gen(function* () {
      runtimeMock.state.versionError = new PiRuntimeError({
        operation: "runCommand",
        detail: "spawn pi ENOENT",
      });
      const errorSnapshot = yield* checkPiProviderStatus(
        decodePiSettings({ binaryPath: "pi", customModels: ["custom/pi-model"] }),
      );
      runtimeMock.state.versionError = null;
      runtimeMock.state.calls.length = 0;

      const successSnapshot = yield* checkPiProviderStatus(
        decodePiSettings({ binaryPath: "pi", customModels: ["custom/pi-model"] }),
      );

      NodeAssert.ok(errorSnapshot.models.some((model) => model.slug === "custom/pi-model"));
      NodeAssert.ok(successSnapshot.models.some((model) => model.slug === "custom/pi-model"));
      NodeAssert.ok(
        successSnapshot.models.some((model) => model.slug === "anthropic/claude-sonnet-5"),
      );
    }),
  );
});
