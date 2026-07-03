import * as NodeAssert from "node:assert/strict";

import { it } from "@effect/vitest";
import { DEFAULT_MODEL_BY_PROVIDER, PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { beforeEach } from "vite-plus/test";

import { PiRuntime, PiRuntimeError, type PiRuntimeShape } from "../piRuntime.ts";
import { buildInitialPiProviderSnapshot, checkPiProviderStatus } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const PROVIDER = ProviderDriverKind.make("pi");
const DEFAULT_PI_MODEL = DEFAULT_MODEL_BY_PROVIDER[PROVIDER];

const runtimeMock = {
  state: {
    calls: [] as Array<{ binaryPath: string; args: ReadonlyArray<string> }>,
    versionResult: { stdout: "pi 0.4.1\n", stderr: "", code: 0 },
    modelsResult: {
      stdout: [
        "provider    model                  context  max-out  thinking  images",
        "anthropic   claude-sonnet-5        1M       128K     yes       yes",
        "anthropic   claude-haiku-4-5       200K     16K      no        yes",
        "",
      ].join("\n"),
      stderr: "",
      code: 0,
    },
    versionError: null as PiRuntimeError | null,
    modelsError: null as PiRuntimeError | null,
  },
  reset() {
    this.state.calls.length = 0;
    this.state.versionResult = { stdout: "pi 0.4.1\n", stderr: "", code: 0 };
    this.state.modelsResult = {
      stdout: [
        "provider    model                  context  max-out  thinking  images",
        "anthropic   claude-sonnet-5        1M       128K     yes       yes",
        "anthropic   claude-haiku-4-5       200K     16K      no        yes",
        "",
      ].join("\n"),
      stderr: "",
      code: 0,
    };
    this.state.versionError = null;
    this.state.modelsError = null;
  },
};

const PiRuntimeTestDouble: PiRuntimeShape = {
  spawnSession: () =>
    Effect.fail(
      new PiRuntimeError({
        operation: "spawnSession",
        detail: "PiRuntimeTestDouble.spawnSession not used in provider tests",
      }),
    ),
  runCommand: (input) =>
    Effect.gen(function* () {
      runtimeMock.state.calls.push({ binaryPath: input.binaryPath, args: input.args });
      const command = input.args[0];
      if (command === "--version") {
        if (runtimeMock.state.versionError) return yield* runtimeMock.state.versionError;
        return runtimeMock.state.versionResult;
      }
      if (command === "--list-models") {
        if (runtimeMock.state.modelsError) return yield* runtimeMock.state.modelsError;
        return runtimeMock.state.modelsResult;
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

  it.effect("discovers models and keeps the configured default Pi model resolvable", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));
      const slugs = snapshot.models.map((model) => model.slug);
      const defaultModel = DEFAULT_PI_MODEL;

      NodeAssert.equal(snapshot.enabled, true);
      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "ready");
      NodeAssert.equal(snapshot.version, "0.4.1");
      NodeAssert.equal(snapshot.badgeLabel, "Early Access");
      NodeAssert.ok(defaultModel);
      NodeAssert.ok(slugs.includes(defaultModel));

      const model = snapshot.models.find((entry) => entry.slug === defaultModel);
      const thinking = model?.capabilities?.optionDescriptors?.find(
        (descriptor) => descriptor.id === "thinking" && descriptor.type === "select",
      );
      NodeAssert.ok(thinking && thinking.type === "select");
      NodeAssert.equal(thinking.currentValue, "medium");
    }),
  );

  it.effect("returns a warning when model discovery succeeds with no models", () =>
    Effect.gen(function* () {
      runtimeMock.state.modelsResult = { stdout: "no table here\n", stderr: "", code: 0 };

      const snapshot = yield* checkPiProviderStatus(decodePiSettings({ binaryPath: "pi" }));

      NodeAssert.equal(snapshot.installed, true);
      NodeAssert.equal(snapshot.status, "warning");
      NodeAssert.match(snapshot.message ?? "", /reported no models/);
    }),
  );
});
