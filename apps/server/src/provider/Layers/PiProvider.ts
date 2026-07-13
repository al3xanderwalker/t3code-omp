import {
  ProviderDriverKind,
  type ModelCapabilities,
  type PiSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { createModelCapabilities, titleCaseSlug } from "@t3tools/shared/model";
import {
  decodePiAvailableModelsResponseDataExit,
  PI_THINKING_LEVELS,
  PiRuntime,
  PiRuntimeError,
  piRuntimeErrorDetail,
  type PiAvailableModel,
} from "../piRuntime.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  type ProviderProbeResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_PRESENTATION = {
  displayName: "Pi",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;

const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const PI_MODEL_DISCOVERY_TIMEOUT_MS = 15_000;
const PI_CODEX_THINKING_LEVELS = [...PI_THINKING_LEVELS, "xhigh"] as const;

function thinkingLabel(level: string): string {
  return level === "xhigh" ? "Extra High" : titleCaseSlug(level);
}

function piThinkingCapabilities(model: PiAvailableModel): ModelCapabilities {
  if (model.reasoning !== true) return DEFAULT_PI_MODEL_CAPABILITIES;
  const provider = model.provider.trim().toLowerCase();
  const id = model.id.trim().toLowerCase();
  const levels =
    provider === "openai-codex" || id.includes("codex")
      ? PI_CODEX_THINKING_LEVELS
      : PI_THINKING_LEVELS;
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "thinking",
        label: "Thinking",
        options: levels.map((level) =>
          level === "medium"
            ? { value: level, label: thinkingLabel(level), isDefault: true }
            : { value: level, label: thinkingLabel(level) },
        ),
      }),
    ],
  });
}

function toServerProviderModels(
  models: ReadonlyArray<PiAvailableModel>,
): Array<ServerProviderModel> {
  const seen = new Set<string>();
  const out: Array<ServerProviderModel> = [];
  for (const model of models) {
    const provider = model.provider.trim();
    const id = model.id.trim();
    if (!provider || !id) continue;
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    out.push({
      slug,
      name: model.name?.trim() || titleCaseSlug(id),
      subProvider: titleCaseSlug(provider),
      isCustom: false,
      capabilities: piThinkingCapabilities(model),
    });
  }
  return out.toSorted((left, right) => left.slug.localeCompare(right.slug));
}

function formatPiProbeError(detail: string): { installed: boolean; message: string } {
  const lower = detail.toLowerCase();
  if (lower.includes("enoent") || lower.includes("notfound") || lower.includes("not found")) {
    return {
      installed: false,
      message: "Pi CLI (`pi`) is not installed or not on PATH.",
    };
  }
  return {
    installed: true,
    message: `Failed to execute Pi CLI health check: ${detail}`,
  };
}

const piSnapshot = (input: {
  readonly piSettings: PiSettings;
  readonly checkedAt: string;
  readonly probe: ProviderProbeResult;
  readonly models?: ReadonlyArray<ServerProviderModel>;
}): ServerProviderDraft =>
  buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: input.piSettings.enabled,
    checkedAt: input.checkedAt,
    models:
      input.models ??
      providerModelsFromSettings(
        [],
        PROVIDER,
        input.piSettings.customModels,
        DEFAULT_PI_MODEL_CAPABILITIES,
      ),
    probe: input.probe,
  });

export const buildInitialPiProviderSnapshot = (
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return piSnapshot({
      piSettings,
      checkedAt,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: piSettings.enabled
          ? "Pi provider status has not been checked in this session yet."
          : "Pi is disabled in T3 Code settings.",
      },
    });
  });

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, PiRuntime> {
  const piRuntime = yield* PiRuntime;
  const resolvedEnvironment = environment ?? process.env;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const failureDetail = (cause: Cause.Cause<unknown>) => piRuntimeErrorDetail(Cause.squash(cause));

  const fallback = (detail: string, version: string | null = null) => {
    const failure = formatPiProbeError(detail);
    return piSnapshot({
      piSettings,
      checkedAt,
      probe: {
        installed: failure.installed,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: failure.message,
      },
    });
  };

  if (!piSettings.enabled) {
    return piSnapshot({
      piSettings: { ...piSettings, enabled: false },
      checkedAt,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionExit = yield* Effect.exit(
    piRuntime.runCommand({
      binaryPath: piSettings.binaryPath,
      args: ["--version"],
      environment: resolvedEnvironment,
    }),
  );
  if (versionExit._tag === "Failure") {
    const detail = failureDetail(versionExit.cause);
    yield* Effect.logWarning(`Pi provider version probe failed: ${detail}`);
    return fallback(detail);
  }
  const version = parseGenericCliVersion(versionExit.value.stdout);
  if (!version) {
    yield* Effect.logWarning("Pi provider version probe returned unparseable output.");
    return fallback("Unable to determine Pi version from `pi --version` output.");
  }

  const modelsExit = yield* Effect.exit(
    Effect.scoped(
      Effect.gen(function* () {
        const rpc = yield* piRuntime.spawnSession({
          binaryPath: piSettings.binaryPath,
          cwd: process.cwd(),
          environment: resolvedEnvironment,
          runtimeMode: "full-access",
          noSession: true,
          noTools: true,
        });
        const response = yield* rpc.request(
          { type: "get_available_models" },
          { timeoutMs: PI_MODEL_DISCOVERY_TIMEOUT_MS },
        );
        const modelsDataExit = decodePiAvailableModelsResponseDataExit(response.data);
        if (Exit.isFailure(modelsDataExit)) {
          return yield* new PiRuntimeError({
            operation: "get_available_models",
            detail: "Pi returned malformed available models data.",
          });
        }
        return modelsDataExit.value.models;
      }),
    ),
  );
  if (modelsExit._tag === "Failure") {
    const detail = failureDetail(modelsExit.cause);
    yield* Effect.logWarning(`Pi provider model probe failed: ${detail}`);
    return fallback(detail, version);
  }

  const piModels = modelsExit.value;
  const discoveredModels = toServerProviderModels(piModels);
  const models = providerModelsFromSettings(
    discoveredModels,
    PROVIDER,
    piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  return piSnapshot({
    piSettings,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: discoveredModels.length > 0 ? "ready" : "warning",
      auth: {
        status: discoveredModels.length > 0 ? "authenticated" : "unknown",
        type: "pi",
      },
      message:
        discoveredModels.length > 0
          ? `Pi reports ${discoveredModels.length} models across its configured providers.`
          : "Pi is available, but Pi reported no models.",
    },
  });
});
