import type { ProviderApprovalDecision, RuntimeMode } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as P from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { parseProviderModelSlug } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as ProcessRunner from "../processRunner.ts";

const decodeJsonStringExit = Schema.decodeUnknownExit(Schema.UnknownFromJsonString);
const encodeJsonStringExit = Schema.encodeUnknownExit(Schema.UnknownFromJsonString);

function encodeJsonLineExit(value: unknown): Exit.Exit<string, unknown> {
  const result = encodeJsonStringExit(value);
  return Exit.isSuccess(result) ? Exit.succeed(result.value) : Exit.fail(result.cause);
}

export function nonEmptyDetail(detail: string | undefined, fallback: string): string {
  const trimmed = detail?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : fallback;
}

const PI_RUNTIME_ERROR_TAG = "PiRuntimeError";
export class PiRuntimeError extends Data.TaggedError(PI_RUNTIME_ERROR_TAG)<{
  readonly operation: string;
  readonly cause?: unknown;
  readonly detail: string;
}> {
  static readonly is = (u: unknown): u is PiRuntimeError => P.isTagged(u, PI_RUNTIME_ERROR_TAG);
}

export function piRuntimeErrorDetail(cause: unknown): string {
  if (PiRuntimeError.is(cause)) return cause.detail;
  if (cause instanceof Error && cause.message.trim().length > 0) return cause.message.trim();
  return String(cause);
}

const DEFAULT_PI_REQUEST_TIMEOUT_MS = 30_000;
const PI_STDERR_TAIL_MAX_CHARS = 8 * 1024;

function appendBoundedText(current: string, chunk: string, maxChars: number): string {
  const next = `${current}${chunk}`;
  return next.length > maxChars ? next.slice(next.length - maxChars) : next;
}

export interface PiCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
}

export const PI_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high"] as const;

export const PI_APPROVAL_TITLE_PREFIX = "T3_APPROVAL ";
export const PI_APPROVAL_PROTOCOL_VERSION = 1;
export const PI_APPROVAL_OPTION_ALLOW = "allow";
export const PI_APPROVAL_OPTION_ALLOW_ALWAYS = "allow-always";
export const PI_APPROVAL_OPTION_DENY = "deny";
export const PI_RUNTIME_MODE_ENV = "T3CODE_PI_RUNTIME_MODE";

export const PI_APPROVAL_EXTENSION_SOURCE = `\
const MODE = process.env["${PI_RUNTIME_MODE_ENV}"] ?? "approval-required";

export default function t3codeApprovals(pi) {
  const alwaysAllowed = new Set();
  pi.on("tool_call", async (event, ctx) => {
    if (MODE === "full-access") return;
    const tool = event.toolName;
    const isEditTool = tool === "edit" || tool === "write";
    const gated = tool === "bash" || (isEditTool && MODE !== "auto-accept-edits");
    if (!gated || alwaysAllowed.has(tool)) return;
    const input = event.input ?? {};
    const detail =
      tool === "bash" ? String(input.command ?? "") : String(input.path ?? input.file_path ?? "");
    const choice = await ctx.ui.select(
      "${PI_APPROVAL_TITLE_PREFIX}" + JSON.stringify({ version: ${PI_APPROVAL_PROTOCOL_VERSION}, tool, detail }),
      ["${PI_APPROVAL_OPTION_ALLOW}", "${PI_APPROVAL_OPTION_ALLOW_ALWAYS}", "${PI_APPROVAL_OPTION_DENY}"],
    );
    if (choice === "${PI_APPROVAL_OPTION_ALLOW_ALWAYS}") {
      alwaysAllowed.add(tool);
      return;
    }
    if (choice === "${PI_APPROVAL_OPTION_ALLOW}") return;
    return {
      block: true,
      reason:
        choice === "${PI_APPROVAL_OPTION_DENY}"
          ? "The user denied this action."
          : "The approval request was cancelled.",
    };
  });
}
`;

export interface PiApprovalRequestPayload {
  readonly tool: string;
  readonly detail: string;
}

export function parsePiApprovalTitle(title: unknown): PiApprovalRequestPayload | null {
  if (typeof title !== "string" || !title.startsWith(PI_APPROVAL_TITLE_PREFIX)) return null;
  const result = decodeJsonStringExit(title.slice(PI_APPROVAL_TITLE_PREFIX.length));
  if (Exit.isFailure(result)) return null;
  const parsed = result.value;
  if (parsed && typeof parsed === "object" && "tool" in parsed) {
    const record = parsed as Record<string, unknown>;
    if (record.version !== PI_APPROVAL_PROTOCOL_VERSION || typeof record.tool !== "string")
      return null;
    return {
      tool: record.tool,
      detail: typeof record.detail === "string" ? record.detail : "",
    };
  }
  return null;
}

export function toPiApprovalSelection(decision: ProviderApprovalDecision): string | null {
  switch (decision) {
    case "accept":
      return PI_APPROVAL_OPTION_ALLOW;
    case "acceptForSession":
      return PI_APPROVAL_OPTION_ALLOW_ALWAYS;
    case "decline":
      return PI_APPROVAL_OPTION_DENY;
    case "cancel":
    default:
      return null;
  }
}

export interface PiRpcResponse {
  readonly type: "response";
  readonly id?: string;
  readonly command: string;
  readonly success: boolean;
  readonly error?: string;
  readonly data?: unknown;
}

const PiRpcResponseWithId = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.String,
  command: Schema.String,
  success: Schema.Boolean,
  error: Schema.optionalKey(Schema.String),
  data: Schema.optionalKey(Schema.Unknown),
});

const PiContentBlock = Schema.Struct({
  type: Schema.String,
  text: Schema.optionalKey(Schema.String),
});
export type PiContentBlock = typeof PiContentBlock.Type;

export const PiMessageContent = Schema.Union([Schema.String, Schema.Array(PiContentBlock)]);
export type PiMessageContent = typeof PiMessageContent.Type;

const PiAssistantMessageEvent = Schema.Struct({
  type: Schema.String,
  delta: Schema.optionalKey(Schema.String),
  contentIndex: Schema.optionalKey(Schema.Number),
});

export const PiToolResult = Schema.Struct({
  content: Schema.optionalKey(PiMessageContent),
});
export type PiToolResult = typeof PiToolResult.Type;

const PiTokenCounts = Schema.Struct({
  total: Schema.optionalKey(Schema.Number),
  input: Schema.optionalKey(Schema.Number),
  cacheRead: Schema.optionalKey(Schema.Number),
  output: Schema.optionalKey(Schema.Number),
});

const PiContextUsage = Schema.Struct({
  tokens: Schema.optionalKey(Schema.Number),
  contextWindow: Schema.optionalKey(Schema.Number),
});

export const PiSessionStats = Schema.Struct({
  tokens: Schema.optionalKey(PiTokenCounts),
  contextUsage: Schema.optionalKey(PiContextUsage),
  toolCalls: Schema.optionalKey(Schema.Number),
});
export type PiSessionStats = typeof PiSessionStats.Type;

export const PiThreadMessage = Schema.Struct({
  role: Schema.String,
  stopReason: Schema.optionalKey(Schema.String),
  content: Schema.optionalKey(PiMessageContent),
});
export type PiThreadMessage = typeof PiThreadMessage.Type;

export const PiStateResponseData = Schema.Struct({
  sessionId: Schema.optionalKey(Schema.String),
});
export type PiStateResponseData = typeof PiStateResponseData.Type;

const PiAvailableModel = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.optionalKey(Schema.String),
  reasoning: Schema.optionalKey(Schema.Boolean),
  contextWindow: Schema.optionalKey(Schema.Number),
  maxTokens: Schema.optionalKey(Schema.Number),
});
export type PiAvailableModel = typeof PiAvailableModel.Type;

const PiAvailableModelsResponseData = Schema.Struct({
  models: Schema.Array(Schema.Unknown),
});
export interface PiAvailableModelsResponseData {
  readonly models: ReadonlyArray<PiAvailableModel>;
}

export const PiMessagesResponseData = Schema.Struct({
  messages: Schema.Array(PiThreadMessage),
});
export type PiMessagesResponseData = typeof PiMessagesResponseData.Type;

const MessageStartEvent = Schema.Struct({
  type: Schema.Literal("message_start"),
});

const MessageUpdateEvent = Schema.Struct({
  type: Schema.Literal("message_update"),
  assistantMessageEvent: PiAssistantMessageEvent,
});

const MessageEndEvent = Schema.Struct({
  type: Schema.Literal("message_end"),
  message: PiThreadMessage,
});

const ToolExecutionEvent = Schema.Struct({
  type: Schema.Literals(["tool_execution_start", "tool_execution_update", "tool_execution_end"]),
  toolCallId: Schema.optionalKey(Schema.String),
  toolName: Schema.optionalKey(Schema.String),
  args: Schema.optionalKey(Schema.Unknown),
  partialResult: Schema.optionalKey(PiToolResult),
  result: Schema.optionalKey(PiToolResult),
  isError: Schema.optionalKey(Schema.Boolean),
});

const AgentEndEvent = Schema.Struct({
  type: Schema.Literal("agent_end"),
});

const ExtensionUiRequestEvent = Schema.Struct({
  type: Schema.Literal("extension_ui_request"),
  id: Schema.optionalKey(Schema.String),
  method: Schema.optionalKey(Schema.String),
  notifyType: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(Schema.String),
  title: Schema.optionalKey(Schema.String),
  options: Schema.optionalKey(Schema.Array(Schema.String)),
});

const CompactionStartEvent = Schema.Struct({
  type: Schema.Literal("compaction_start"),
});

const CompactionEndEvent = Schema.Struct({
  type: Schema.Literal("compaction_end"),
  aborted: Schema.optionalKey(Schema.Boolean),
});

const AutoRetryStartEvent = Schema.Struct({
  type: Schema.Literal("auto_retry_start"),
  attempt: Schema.optionalKey(Schema.Unknown),
});

const ExtensionErrorEvent = Schema.Struct({
  type: Schema.Literal("extension_error"),
  error: Schema.optionalKey(Schema.String),
});

export const PiRpcEvent = Schema.Union([
  MessageStartEvent,
  MessageUpdateEvent,
  MessageEndEvent,
  ToolExecutionEvent,
  AgentEndEvent,
  ExtensionUiRequestEvent,
  CompactionStartEvent,
  CompactionEndEvent,
  AutoRetryStartEvent,
  ExtensionErrorEvent,
]);
export type PiRpcEvent = typeof PiRpcEvent.Type;

export const decodePiSessionStatsExit = Schema.decodeUnknownExit(PiSessionStats);
export const decodePiStateResponseDataExit = Schema.decodeUnknownExit(PiStateResponseData);
export const decodePiMessagesResponseDataExit = Schema.decodeUnknownExit(PiMessagesResponseData);

const decodePiAvailableModelExit = Schema.decodeUnknownExit(PiAvailableModel);
const decodePiAvailableModelsResponseDataShapeExit = Schema.decodeUnknownExit(
  PiAvailableModelsResponseData,
);
const decodePiRpcResponseWithIdExit = Schema.decodeUnknownExit(PiRpcResponseWithId);
const decodePiRpcEventExit = Schema.decodeUnknownExit(PiRpcEvent);

export function decodePiAvailableModelsResponseDataExit(
  value: unknown,
): Exit.Exit<PiAvailableModelsResponseData, unknown> {
  const dataExit = decodePiAvailableModelsResponseDataShapeExit(value);
  if (Exit.isFailure(dataExit)) return Exit.fail(dataExit.cause);
  const models = dataExit.value.models.flatMap((model) => {
    const modelExit = decodePiAvailableModelExit(model);
    return Exit.isSuccess(modelExit) ? [modelExit.value] : [];
  });
  return Exit.succeed({ models });
}

export interface PiRpcHandle {
  readonly request: (
    command: Record<string, unknown>,
    options?: { readonly timeoutMs?: number },
  ) => Effect.Effect<PiRpcResponse, PiRuntimeError>;
  readonly notify: (payload: Record<string, unknown>) => Effect.Effect<void>;
  readonly events: Queue.Dequeue<PiRpcEvent>;
  readonly exitCode: Effect.Effect<number>;
  readonly stderr: Effect.Effect<string>;
}

export interface SpawnPiRpcInput {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly runtimeMode: RuntimeMode;
  readonly sessionName?: string;
  readonly modelSlug?: string;
  readonly thinkingLevel?: string;
  readonly approvalExtensionPath?: string;
  readonly noSession?: boolean;
  readonly noTools?: boolean;
  readonly mcpConfigPath?: string;
  readonly appendSystemPrompt?: string;
}

export const spawnPiRpcSession = (
  input: SpawnPiRpcInput,
): Effect.Effect<
  PiRpcHandle,
  PiRuntimeError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const scope = yield* Scope.Scope;
    const parsedModel = parseProviderModelSlug(input.modelSlug);

    const args = [
      "--mode",
      "rpc",
      ...(input.noSession ? ["--no-session"] : []),
      ...(input.noTools ? ["--no-tools"] : []),
      ...(input.mcpConfigPath ? ["--mcp-config", input.mcpConfigPath] : []),
      ...(input.appendSystemPrompt ? ["--append-system-prompt", input.appendSystemPrompt] : []),
      ...(input.sessionName ? ["--name", input.sessionName] : []),
      ...(parsedModel ? ["--provider", parsedModel.provider, "--model", parsedModel.modelId] : []),
      ...(input.thinkingLevel ? ["--thinking", input.thinkingLevel] : []),
      ...(input.approvalExtensionPath && input.runtimeMode !== "full-access"
        ? ["--extension", input.approvalExtensionPath]
        : []),
    ];
    const environment = {
      ...input.environment,
      [PI_RUNTIME_MODE_ENV]: input.runtimeMode,
    };
    const spawnCommand = yield* resolveSpawnCommand(input.binaryPath, args, {
      env: environment,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new PiRuntimeError({
            operation: "spawnPiRpcSession",
            detail: `Failed to resolve Pi spawn command: ${piRuntimeErrorDetail(cause)}`,
            cause,
          }),
      ),
    );

    const stdinQueue = yield* Queue.unbounded<string>();
    const events = yield* Queue.bounded<PiRpcEvent>(1_024);
    const stderrRef = yield* Ref.make("");
    const closedReasonRef = yield* Ref.make<string | null>(null);
    const pending = new Map<string, Deferred.Deferred<PiRpcResponse, PiRuntimeError>>();
    let requestSequence = 0;

    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          shell: spawnCommand.shell,
          cwd: input.cwd,
          env: environment,
          ...(input.environment === undefined ? { extendEnv: true } : {}),
          stdin: {
            stream: Stream.encodeText(Stream.fromQueue(stdinQueue)),
          },
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.mapError(
          (cause) =>
            new PiRuntimeError({
              operation: "spawnPiRpcSession",
              detail: `Failed to spawn Pi RPC process: ${piRuntimeErrorDetail(cause)}`,
              cause,
            }),
        ),
      );

    const failPending = (detail: string) =>
      Effect.gen(function* () {
        yield* Ref.set(closedReasonRef, detail);
        const inflight = yield* Effect.sync(() => {
          const deferreds = [...pending.values()];
          pending.clear();
          return deferreds;
        });
        yield* Effect.forEach(
          inflight,
          (deferred) =>
            Deferred.fail(deferred, new PiRuntimeError({ operation: "request", detail })).pipe(
              Effect.ignore,
            ),
          { discard: true },
        );
      });

    yield* Scope.addFinalizer(
      scope,
      Effect.gen(function* () {
        yield* failPending("Pi RPC session closed.");
        yield* Queue.shutdown(stdinQueue);
        yield* Queue.shutdown(events);
      }),
    );

    const handleLine = (line: string) =>
      Effect.gen(function* () {
        const trimmed = line.endsWith("\r") ? line.slice(0, -1) : line;
        if (trimmed.length === 0) return;
        const decoded = decodeJsonStringExit(trimmed);
        if (Exit.isFailure(decoded)) {
          yield* Effect.logWarning("Dropped malformed Pi RPC JSON line.");
          return;
        }
        const parsed = decoded.value;
        const parsedType =
          parsed && typeof parsed === "object" && "type" in parsed ? parsed.type : undefined;
        if (typeof parsedType !== "string") {
          yield* Effect.logWarning("Dropped malformed Pi RPC line without a string type.");
          return;
        }
        if (parsedType === "response") {
          const responseExit = decodePiRpcResponseWithIdExit(parsed);
          if (Exit.isFailure(responseExit)) {
            yield* Effect.logWarning("Dropped malformed Pi RPC response.");
            return;
          }
          const response = responseExit.value;
          const deferred = pending.get(response.id);
          if (deferred) {
            pending.delete(response.id);
            yield* Deferred.succeed(deferred, response).pipe(Effect.ignore);
          }
          return;
        }
        const eventExit = decodePiRpcEventExit(parsed);
        if (Exit.isFailure(eventExit)) {
          yield* Effect.logWarning(`Dropped unsupported Pi RPC event '${parsedType}'.`);
          return;
        }
        yield* Queue.offer(events, eventExit.value).pipe(Effect.ignore);
      });

    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.runForEach(handleLine),
      Effect.ignore,
      Effect.forkIn(scope),
    );
    yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Ref.update(stderrRef, (stderr) =>
          appendBoundedText(stderr, chunk, PI_STDERR_TAIL_MAX_CHARS),
        ),
      ),
      Effect.ignore,
      Effect.forkIn(scope),
    );

    const exitCode = child.exitCode.pipe(
      Effect.map(Number),
      Effect.orElseSucceed(() => -1),
    );
    const stderr = Ref.get(stderrRef);

    yield* exitCode.pipe(
      Effect.flatMap((code) =>
        failPending(`Pi RPC process exited before replying (exit code ${code}).`),
      ),
      Effect.ensuring(Queue.shutdown(events).pipe(Effect.ignore)),
      Effect.forkIn(scope),
    );

    const request: PiRpcHandle["request"] = (command, options) => {
      const commandType = String(command.type ?? "request");
      return Effect.gen(function* () {
        requestSequence += 1;
        const id = `t3-${requestSequence}`;
        const encodedExit = encodeJsonLineExit({ ...command, id });
        if (Exit.isFailure(encodedExit)) {
          return yield* new PiRuntimeError({
            operation: commandType,
            detail: `Failed to encode Pi RPC command '${commandType}' as JSON.`,
            cause: encodedExit.cause,
          });
        }
        const closedReason = yield* Ref.get(closedReasonRef);
        if (closedReason !== null) {
          return yield* new PiRuntimeError({
            operation: commandType,
            detail: closedReason,
          });
        }
        const deferred = yield* Deferred.make<PiRpcResponse, PiRuntimeError>();
        pending.set(id, deferred);
        const closedAfterPending = yield* Ref.get(closedReasonRef);
        if (closedAfterPending !== null) {
          pending.delete(id);
          return yield* new PiRuntimeError({
            operation: commandType,
            detail: closedAfterPending,
          });
        }
        yield* Queue.offer(stdinQueue, `${encodedExit.value}\n`);
        const response = yield* Deferred.await(deferred).pipe(
          Effect.timeoutOrElse({
            duration: options?.timeoutMs ?? DEFAULT_PI_REQUEST_TIMEOUT_MS,
            orElse: () =>
              Effect.fail(
                new PiRuntimeError({
                  operation: commandType,
                  detail: `Timed out waiting for Pi response to '${commandType}'.`,
                }),
              ),
          }),
          Effect.ensuring(Effect.sync(() => pending.delete(id))),
        );
        if (!response.success) {
          return yield* new PiRuntimeError({
            operation: commandType,
            detail: nonEmptyDetail(response.error, `Pi command '${response.command}' failed.`),
          });
        }
        return response;
      }).pipe(Effect.withSpan(`pi.${commandType}`));
    };

    const notify: PiRpcHandle["notify"] = (payload) =>
      Effect.gen(function* () {
        const encodedExit = encodeJsonLineExit(payload);
        if (Exit.isFailure(encodedExit)) {
          yield* Effect.logWarning("Dropped non-JSON-encodable Pi RPC notification.");
          return;
        }
        yield* Queue.offer(stdinQueue, `${encodedExit.value}\n`);
      });

    return { request, notify, events, exitCode, stderr } satisfies PiRpcHandle;
  });

export interface PiRuntimeShape {
  readonly runCommand: (input: {
    readonly binaryPath: string;
    readonly args: ReadonlyArray<string>;
    readonly environment?: NodeJS.ProcessEnv;
    readonly cwd?: string;
    readonly stdin?: string;
  }) => Effect.Effect<PiCommandResult, PiRuntimeError>;
  readonly spawnSession: (
    input: SpawnPiRpcInput,
  ) => Effect.Effect<PiRpcHandle, PiRuntimeError, Scope.Scope>;
}

export const makePiRuntime = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const processRunner = yield* ProcessRunner.ProcessRunner;
  const runCommand: PiRuntimeShape["runCommand"] = (input) =>
    processRunner
      .run({
        command: input.binaryPath,
        args: input.args,
        cwd: input.cwd,
        env: input.environment,
        stdin: input.stdin,
      })
      .pipe(
        Effect.map(
          (result): PiCommandResult => ({
            stdout: result.stdout,
            stderr: result.stderr,
            code: Number(result.code ?? -1),
          }),
        ),
        Effect.mapError(
          (cause) =>
            new PiRuntimeError({
              operation: "runCommand",
              detail: `Failed to execute '${input.binaryPath} ${input.args.join(" ")}': ${piRuntimeErrorDetail(cause)}`,
              cause,
            }),
        ),
        Effect.withSpan("pi.runCommand"),
      );
  const spawnSession: PiRuntimeShape["spawnSession"] = (input) =>
    spawnPiRpcSession(input).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

  return {
    runCommand,
    spawnSession,
  } satisfies PiRuntimeShape;
});

export class PiRuntime extends Context.Service<PiRuntime, PiRuntimeShape>()(
  "t3/provider/piRuntime",
) {}

export const PiRuntimeLive = Layer.effect(PiRuntime, makePiRuntime).pipe(
  Layer.provide(ProcessRunner.layer),
);
