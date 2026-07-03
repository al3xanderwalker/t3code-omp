import * as NodeAssert from "node:assert/strict";

import { it } from "@effect/vitest";
import { PiSettings, ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { beforeEach } from "vite-plus/test";

import {
  PiRuntime,
  PiRuntimeError,
  type PiCommandResult,
  type PiRuntimeShape,
} from "../provider/piRuntime.ts";
import { makePiTextGeneration } from "./PiTextGeneration.ts";
import type * as TextGeneration from "./TextGeneration.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const isTextGenerationError = Schema.is(TextGenerationError);

const DEFAULT_MODEL_SELECTION = {
  instanceId: ProviderInstanceId.make("pi"),
  model: "anthropic/claude-haiku-4-5",
};

const runtimeMock = {
  state: {
    calls: [] as Array<{
      readonly binaryPath: string;
      readonly args: ReadonlyArray<string>;
      readonly cwd: string | undefined;
      readonly stdin: string | undefined;
    }>,
    results: [] as Array<PiCommandResult>,
    error: null as PiRuntimeError | null,
  },
  reset() {
    this.state.calls.length = 0;
    this.state.results.length = 0;
    this.state.error = null;
  },
};

const PiRuntimeTestDouble: PiRuntimeShape = {
  spawnSession: () =>
    Effect.fail(
      new PiRuntimeError({
        operation: "spawnSession",
        detail: "PiRuntimeTestDouble.spawnSession not used in text generation tests",
      }),
    ),
  runCommand: (input) =>
    Effect.gen(function* () {
      runtimeMock.state.calls.push({
        binaryPath: input.binaryPath,
        args: input.args,
        cwd: input.cwd,
        stdin: input.stdin,
      });
      if (runtimeMock.state.error) return yield* runtimeMock.state.error;
      return runtimeMock.state.results.shift() ?? { stdout: "{}", stderr: "", code: 0 };
    }),
};

const PiTextGenerationTestLayer = Layer.succeed(PiRuntime, PiRuntimeTestDouble);

function queueJson(value: unknown) {
  runtimeMock.state.results.push({
    stdout: `Sure.\n${JSON.stringify(value)}\nDone.`,
    stderr: "",
    code: 0,
  });
}

function withPiTextGeneration<A, E, R>(
  effectFn: (textGeneration: TextGeneration.TextGeneration["Service"]) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const textGeneration = yield* makePiTextGeneration(
      decodePiSettings({ binaryPath: "fake-pi" }),
      { T3_TEST_ENV: "1" },
    );
    return yield* effectFn(textGeneration);
  });
}

beforeEach(() => {
  runtimeMock.reset();
});

it.layer(PiTextGenerationTestLayer)("PiTextGeneration", (it) => {
  it.effect("generates commit messages through Pi CLI JSON mode", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        queueJson({
          subject: "Add Pi text generation coverage",
          body: "Exercise every Pi text generation method.",
          branch: "pi-text-generation-coverage",
        });

        const commit = yield* textGeneration.generateCommitMessage({
          cwd: process.cwd(),
          branch: "feature/pi-text-generation",
          stagedSummary: "M apps/server/src/textGeneration/PiTextGeneration.ts",
          stagedPatch: "diff --git a/PiTextGeneration.ts b/PiTextGeneration.ts",
          includeBranch: true,
          modelSelection: DEFAULT_MODEL_SELECTION,
        });

        NodeAssert.deepEqual(commit, {
          subject: "Add Pi text generation coverage",
          body: "Exercise every Pi text generation method.",
          branch: "feature/pi-text-generation-coverage",
        });
        NodeAssert.equal(runtimeMock.state.calls.length, 1);

        const args = runtimeMock.state.calls[0]?.args ?? [];
        NodeAssert.deepEqual(args.slice(0, 14), [
          "--print",
          "--mode",
          "text",
          "--no-session",
          "--no-tools",
          "--no-extensions",
          "--no-skills",
          "--no-prompt-templates",
          "--no-context-files",
          "--thinking",
          "off",
          "--provider",
          "anthropic",
          "--model",
        ]);
        NodeAssert.equal(args[14], "claude-haiku-4-5");
        NodeAssert.equal(args.includes("Staged files:"), false);
        NodeAssert.match(String(runtimeMock.state.calls[0]?.stdin), /Staged files:/);
      }),
    ),
  );

  it.effect("generates PR content through Pi CLI JSON mode", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        queueJson({ title: "Add Pi provider tests", body: "Covers provider and adapter flows." });

        const pr = yield* textGeneration.generatePrContent({
          cwd: process.cwd(),
          baseBranch: "main",
          headBranch: "feature/pi-text-generation",
          commitSummary: "Add Pi tests",
          diffSummary: "Server test additions",
          diffPatch: "diff --git a/PiProvider.test.ts b/PiProvider.test.ts",
          modelSelection: DEFAULT_MODEL_SELECTION,
        });

        NodeAssert.deepEqual(pr, {
          title: "Add Pi provider tests",
          body: "Covers provider and adapter flows.",
        });
        NodeAssert.equal(runtimeMock.state.calls.length, 1);
        NodeAssert.match(String(runtimeMock.state.calls[0]?.stdin), /GitHub pull request content/);
      }),
    ),
  );

  it.effect("generates branch names through Pi CLI JSON mode", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        queueJson({ branch: "pi-provider-tests" });

        const branch = yield* textGeneration.generateBranchName({
          cwd: process.cwd(),
          message: "Add coverage for Pi provider",
          modelSelection: DEFAULT_MODEL_SELECTION,
        });

        NodeAssert.deepEqual(branch, { branch: "pi-provider-tests" });
        NodeAssert.equal(runtimeMock.state.calls.length, 1);
        NodeAssert.match(String(runtimeMock.state.calls[0]?.stdin), /branch names/);
      }),
    ),
  );

  it.effect("generates thread titles through Pi CLI JSON mode", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        queueJson({ title: "Debug Pi provider setup" });

        const title = yield* textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Why is Pi provider setup failing?",
          modelSelection: DEFAULT_MODEL_SELECTION,
        });

        NodeAssert.deepEqual(title, { title: "Debug Pi provider setup" });
        NodeAssert.equal(runtimeMock.state.calls.length, 1);
        NodeAssert.match(String(runtimeMock.state.calls[0]?.stdin), /thread titles/);
      }),
    ),
  );

  it.effect("passes nested provider model slugs to Pi without truncating model ids", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        queueJson({ title: "Review nested model slug" });

        yield* textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Title this",
          modelSelection: {
            instanceId: ProviderInstanceId.make("pi"),
            model: "openrouter/qwen/qwen3-coder",
          },
        });

        const args = runtimeMock.state.calls[0]?.args ?? [];
        NodeAssert.equal(args[12], "openrouter");
        NodeAssert.equal(args[14], "qwen/qwen3-coder");
      }),
    ),
  );

  it.effect("rejects model selections that are not provider/model slugs", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        const error = yield* textGeneration
          .generateThreadTitle({
            cwd: process.cwd(),
            message: "Title this",
            modelSelection: {
              instanceId: ProviderInstanceId.make("pi"),
              model: "claude-haiku-4-5",
            },
          })
          .pipe(Effect.flip);

        NodeAssert.ok(isTextGenerationError(error));
        NodeAssert.equal(error.detail, "Pi model selection must use the 'provider/model' format.");
      }),
    ),
  );

  it.effect("surfaces invalid Pi JSON output as text generation errors", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        runtimeMock.state.results.push({
          stdout: "not json",
          stderr: "",
          code: 0,
        });

        const error = yield* textGeneration
          .generateThreadTitle({
            cwd: process.cwd(),
            message: "Title this",
            modelSelection: DEFAULT_MODEL_SELECTION,
          })
          .pipe(Effect.flip);

        NodeAssert.ok(isTextGenerationError(error));
        NodeAssert.equal(error.detail, "Pi returned invalid structured output.");
      }),
    ),
  );

  it.effect("surfaces empty Pi stdout as text generation errors", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        runtimeMock.state.results.push({
          stdout: " \n",
          stderr: "",
          code: 0,
        });

        const error = yield* textGeneration
          .generateThreadTitle({
            cwd: process.cwd(),
            message: "Title this",
            modelSelection: DEFAULT_MODEL_SELECTION,
          })
          .pipe(Effect.flip);

        NodeAssert.ok(isTextGenerationError(error));
        NodeAssert.equal(error.detail, "Pi returned empty output.");
      }),
    ),
  );

  it.effect("surfaces non-zero Pi exits as text generation errors", () =>
    withPiTextGeneration((textGeneration) =>
      Effect.gen(function* () {
        runtimeMock.state.results.push({
          stdout: "",
          stderr: "Pi auth failed",
          code: 2,
        });

        const error = yield* textGeneration
          .generateBranchName({
            cwd: process.cwd(),
            message: "Make a branch",
            modelSelection: DEFAULT_MODEL_SELECTION,
          })
          .pipe(Effect.flip);

        NodeAssert.ok(isTextGenerationError(error));
        NodeAssert.equal(error.detail, "Pi auth failed");
      }),
    ),
  );
});
