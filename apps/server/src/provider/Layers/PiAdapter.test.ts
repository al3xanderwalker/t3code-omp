import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  PiSettings,
  ThreadId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { beforeEach } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import {
  PI_APPROVAL_TITLE_PREFIX,
  PiRuntime,
  PiRuntimeError,
  type PiRpcEvent,
  type PiRpcHandle,
  type PiRuntimeShape,
  type SpawnPiRpcInput,
} from "../piRuntime.ts";
import { makePiAdapter } from "./PiAdapter.ts";

class PiAdapter extends Context.Service<PiAdapter, PiAdapterShape>()(
  "t3/provider/Layers/PiAdapter.test/PiAdapter",
) {}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const decodePiSettings = Schema.decodeSync(PiSettings);

type FakePiHandle = {
  readonly input: SpawnPiRpcInput;
  readonly eventsQueue: Queue.Queue<PiRpcEvent>;
  readonly exitDeferred: Deferred.Deferred<number>;
  readonly handle: PiRpcHandle;
};

const runtimeMock = {
  state: {
    spawnInputs: [] as Array<SpawnPiRpcInput>,
    handles: [] as Array<FakePiHandle>,
    requests: [] as Array<Record<string, unknown>>,
    notifications: [] as Array<Record<string, unknown>>,
    closeCalls: [] as Array<string | undefined>,
    beforeGetStateResponse: [] as Array<Effect.Effect<void>>,
    spawnSignals: [] as Array<Deferred.Deferred<void>>,
    spawnFailures: [] as Array<PiRuntimeError | null | undefined>,
    stderrByHandle: [] as Array<string | undefined>,
    promptError: null as PiRuntimeError | null,
    stateData: { sessionId: "pi-session-1" } as unknown,
    stateDataByHandle: [] as Array<unknown>,
    statsData: {
      contextUsage: { tokens: 42, contextWindow: 1_000 },
      tokens: { input: 10, cacheRead: 2, output: 30, total: 42 },
      toolCalls: 1,
    } as unknown,
    abortError: null as PiRuntimeError | null,
    thinkingError: null as PiRuntimeError | null,
    messagesData: {
      messages: [
        { role: "assistant", content: "Hello" },
        { role: "toolResult", content: [{ type: "text", text: "Tool output" }] },
      ],
    } as unknown,
  },
  reset() {
    this.state.spawnInputs.length = 0;
    this.state.handles.length = 0;
    this.state.requests.length = 0;
    this.state.notifications.length = 0;
    this.state.closeCalls.length = 0;
    this.state.beforeGetStateResponse.length = 0;
    this.state.spawnSignals.length = 0;
    this.state.spawnFailures.length = 0;
    this.state.stderrByHandle.length = 0;
    this.state.promptError = null;
    this.state.stateData = { sessionId: "pi-session-1" };
    this.state.stateDataByHandle.length = 0;
    this.state.statsData = {
      contextUsage: { tokens: 42, contextWindow: 1_000 },
      tokens: { input: 10, cacheRead: 2, output: 30, total: 42 },
      toolCalls: 1,
    };
    this.state.abortError = null;
    this.state.thinkingError = null;
    this.state.messagesData = {
      messages: [
        { role: "assistant", content: "Hello" },
        { role: "toolResult", content: [{ type: "text", text: "Tool output" }] },
      ],
    };
  },
};

const commandType = (command: Record<string, unknown>): string =>
  typeof command.type === "string" ? command.type : "unknown";

const PiRuntimeTestDouble: PiRuntimeShape = {
  runCommand: () =>
    Effect.fail(
      new PiRuntimeError({
        operation: "runCommand",
        detail: "PiRuntimeTestDouble.runCommand not used in adapter tests",
      }),
    ),
  spawnSession: (input) =>
    Effect.gen(function* () {
      const attemptIndex = runtimeMock.state.spawnInputs.length;
      runtimeMock.state.spawnInputs.push(input);
      const spawnFailure = runtimeMock.state.spawnFailures[attemptIndex];
      if (spawnFailure) {
        return yield* spawnFailure;
      }
      const handleIndex = runtimeMock.state.handles.length;
      const eventsQueue = yield* Queue.unbounded<PiRpcEvent>();
      const exitDeferred = yield* Deferred.make<number>();
      const handle: PiRpcHandle = {
        request: (command) =>
          Effect.gen(function* () {
            runtimeMock.state.requests.push(command);
            const type = commandType(command);
            if (type === "prompt" && runtimeMock.state.promptError) {
              return yield* runtimeMock.state.promptError;
            }
            if (type === "abort" && runtimeMock.state.abortError) {
              return yield* runtimeMock.state.abortError;
            }
            if (type === "set_thinking_level" && runtimeMock.state.thinkingError) {
              return yield* runtimeMock.state.thinkingError;
            }
            if (type === "get_state") {
              yield* runtimeMock.state.beforeGetStateResponse[handleIndex] ?? Effect.void;
              const stateData =
                handleIndex in runtimeMock.state.stateDataByHandle
                  ? runtimeMock.state.stateDataByHandle[handleIndex]
                  : runtimeMock.state.stateData;
              return {
                type: "response",
                command: type,
                success: true,
                data: stateData,
              };
            }
            if (type === "get_session_stats") {
              return {
                type: "response",
                command: type,
                success: true,
                data: runtimeMock.state.statsData,
              };
            }
            if (type === "get_messages") {
              return {
                type: "response",
                command: type,
                success: true,
                data: runtimeMock.state.messagesData,
              };
            }
            return { type: "response", command: type, success: true };
          }),
        notify: (payload) =>
          Effect.sync(() => {
            runtimeMock.state.notifications.push(payload);
          }),
        events: eventsQueue,
        exitCode: Deferred.await(exitDeferred),
        stderr: Effect.sync(() => runtimeMock.state.stderrByHandle[handleIndex] ?? ""),
      };
      const fakeHandle = { input, eventsQueue, exitDeferred, handle };
      runtimeMock.state.handles.push(fakeHandle);
      const spawnSignal = runtimeMock.state.spawnSignals[handleIndex];
      if (spawnSignal) {
        yield* Deferred.succeed(spawnSignal, undefined).pipe(Effect.ignore);
      }
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls.push(input.sessionName);
        }),
      );
      return handle;
    }),
};

const piAdapterTestSettings = decodePiSettings({
  binaryPath: "fake-pi",
});

const PiAdapterTestLayer = Layer.effect(PiAdapter, makePiAdapter(piAdapterTestSettings)).pipe(
  Layer.provideMerge(Layer.succeed(PiRuntime, PiRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(NodeServices.layer),
);

const startPiSession = (adapter: PiAdapterShape, threadId: ThreadId) =>
  adapter.startSession({
    provider: ProviderDriverKind.make("pi"),
    threadId,
    runtimeMode: "approval-required",
  });

const attachMcpSession = (threadId: ThreadId, token = "mcp-secret-token") =>
  Effect.sync(() => {
    McpProviderSession.setMcpProviderSession({
      environmentId: EnvironmentId.make("environment-pi-test"),
      threadId,
      providerSessionId: "mcp-session-pi",
      providerInstanceId: ProviderInstanceId.make("pi"),
      endpoint: "http://127.0.0.1:43123/mcp",
      authorizationHeader: `Bearer ${token}`,
    });
  });

beforeEach(() => {
  runtimeMock.reset();
  McpProviderSession.clearAllMcpProviderSessions();
});

it.layer(PiAdapterTestLayer)("PiAdapterLive", (it) => {
  it.effect("starts and stops a Pi RPC session through the runtime service", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-start-stop");

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      const session = yield* startPiSession(adapter, threadId);
      yield* adapter.stopSession(threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(session.provider, "pi");
      NodeAssert.equal(session.threadId, threadId);
      NodeAssert.equal(runtimeMock.state.spawnInputs[0]?.binaryPath, "fake-pi");
      NodeAssert.equal(runtimeMock.state.spawnInputs[0]?.mcpConfigPath, undefined);
      NodeAssert.equal(runtimeMock.state.spawnInputs[0]?.appendSystemPrompt, undefined);
      NodeAssert.deepEqual(runtimeMock.state.requests.map(commandType), ["get_state", "abort"]);
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "session.exited"],
      );
      NodeAssert.deepEqual(runtimeMock.state.closeCalls, [`T3 Code ${threadId}`]);
    }),
  );

  it.effect("keeps one live session when concurrent starts race for the same thread", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-concurrent-start");
      const releaseFirstState = yield* Deferred.make<void>();
      const firstSpawned = yield* Deferred.make<void>();
      runtimeMock.state.beforeGetStateResponse[0] = Deferred.await(releaseFirstState);
      runtimeMock.state.spawnSignals[0] = firstSpawned;

      const firstFiber = yield* startPiSession(adapter, threadId).pipe(Effect.forkChild);
      yield* Deferred.await(firstSpawned);
      const secondSession = yield* startPiSession(adapter, threadId);
      yield* Deferred.succeed(releaseFirstState, undefined);
      const firstSession = yield* Fiber.join(firstFiber);
      const sessions = yield* adapter.listSessions();

      NodeAssert.equal(firstSession, secondSession);
      NodeAssert.equal(sessions.length, 1);
      NodeAssert.equal(sessions[0], secondSession);
      NodeAssert.equal(runtimeMock.state.handles.length, 2);
      NodeAssert.deepEqual(runtimeMock.state.closeCalls, [`T3 Code ${threadId}`]);
    }),
  );

  it.effect("includes Pi stderr tail in startup process errors", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-startup-stderr");
      runtimeMock.state.stateData = "malformed";
      runtimeMock.state.stderrByHandle[0] = "startup crash detail\n";

      const error = yield* startPiSession(adapter, threadId).pipe(Effect.flip);

      NodeAssert.equal(error._tag, "ProviderAdapterProcessError");
      NodeAssert.match(error.detail, /Pi returned malformed state data/);
      NodeAssert.match(error.detail, /stderr: startup crash detail/);
      NodeAssert.deepEqual(runtimeMock.state.closeCalls, [`T3 Code ${threadId}`]);
    }),
  );

  it.effect("passes scoped MCP bridge config and token env, then cleans up on stop", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const fs = yield* FileSystem.FileSystem;
      const threadId = asThreadId("thread-pi-mcp-bridge");
      yield* attachMcpSession(threadId, "bridge-token");

      yield* startPiSession(adapter, threadId);

      const input = runtimeMock.state.spawnInputs[0];
      if (!input?.mcpConfigPath) throw new Error("missing MCP config path");
      NodeAssert.match(input.appendSystemPrompt ?? "", /preview_status/);
      NodeAssert.equal(input.approvalExtensionPath !== undefined, true);
      NodeAssert.equal(input.environment?.T3_MCP_BEARER_TOKEN, "bridge-token");

      const rawConfig = yield* fs.readFileString(input.mcpConfigPath!);
      NodeAssert.match(rawConfig, /http:\/\/127\.0\.0\.1:43123\/mcp/);
      NodeAssert.match(rawConfig, /"bearerTokenEnv":"T3_MCP_BEARER_TOKEN"/);
      NodeAssert.equal(rawConfig.includes("bridge-token"), false);

      yield* adapter.stopSession(threadId);
      NodeAssert.equal(yield* fs.exists(input.mcpConfigPath!), false);
    }),
  );

  it.effect("degrades without MCP when bridge config cannot be written", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const threadId = asThreadId("thread-pi-mcp-config-failure");
      yield* fs.remove(path.join(serverConfig.stateDir, "pi-mcp"), {
        recursive: true,
        force: true,
      });
      yield* fs.writeFileString(path.join(serverConfig.stateDir, "pi-mcp"), "blocked");
      yield* attachMcpSession(threadId);

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(runtimeMock.state.spawnInputs[0]?.mcpConfigPath, undefined);
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "runtime.warning"],
      );
      const warningPayload = events.at(-1)?.payload as { readonly message?: string } | undefined;
      NodeAssert.equal(
        warningPayload?.message,
        "Pi MCP bridge could not be configured; preview browser tools unavailable for this session",
      );
      yield* adapter.stopSession(threadId);
      yield* fs
        .remove(path.join(serverConfig.stateDir, "pi-mcp"), { force: true })
        .pipe(Effect.ignore);
    }),
  );

  it.effect("retries without MCP when the pi-mcp-adapter extension is missing", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const fs = yield* FileSystem.FileSystem;
      const threadId = asThreadId("thread-pi-mcp-missing-extension");
      yield* attachMcpSession(threadId);
      runtimeMock.state.spawnFailures[0] = new PiRuntimeError({
        operation: "spawnPiRpcSession",
        detail: "Unknown option --mcp-config",
      });

      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const firstInput = runtimeMock.state.spawnInputs[0];
      const retryInput = runtimeMock.state.spawnInputs[1];
      if (!firstInput?.mcpConfigPath) throw new Error("missing first MCP config path");
      NodeAssert.match(firstInput.appendSystemPrompt ?? "", /preview_status/);
      NodeAssert.equal(retryInput?.mcpConfigPath, undefined);
      NodeAssert.equal(retryInput?.appendSystemPrompt, undefined);
      NodeAssert.equal(runtimeMock.state.handles.length, 1);
      NodeAssert.equal(yield* fs.exists(firstInput.mcpConfigPath!), false);
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "runtime.warning"],
      );
      const warningPayload = events.at(-1)?.payload as { readonly message?: string } | undefined;
      NodeAssert.match(String(warningPayload?.message ?? ""), /pi install npm:pi-mcp-adapter/);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("emits a transport error and removes the session when the Pi process exits", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-unexpected-exit");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* startPiSession(adapter, threadId);
      const handle = runtimeMock.state.handles[0];
      if (!handle) throw new Error("missing fake Pi handle");
      runtimeMock.state.stderrByHandle[0] = "adapter crash detail\n";
      yield* Deferred.succeed(handle.exitDeferred, 23);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "runtime.error", "session.exited"],
      );
      NodeAssert.deepEqual(events.at(-2)?.payload, {
        message: "Pi process exited unexpectedly (23). stderr: adapter crash detail",
        class: "transport_error",
      });
      NodeAssert.deepEqual(events.at(-1)?.payload, {
        reason: "Pi process exited unexpectedly (23). stderr: adapter crash detail",
        recoverable: false,
        exitKind: "error",
      });
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
      NodeAssert.deepEqual(runtimeMock.state.closeCalls, [`T3 Code ${threadId}`]);
    }),
  );

  it.effect("switches model options and steers a running turn into the same turn id", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-steer");
      yield* startPiSession(adapter, threadId);

      const modelSelection = createModelSelection(
        ProviderInstanceId.make("pi"),
        "anthropic/claude-sonnet-5",
        [{ id: "thinking", value: "high" }],
      );
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "Run the first task",
        modelSelection,
      });
      const steered = yield* adapter.sendTurn({
        threadId,
        input: "Update that request",
        modelSelection,
      });

      NodeAssert.equal(String(steered.turnId), String(turn.turnId));
      NodeAssert.deepEqual(runtimeMock.state.requests.map(commandType), [
        "get_state",
        "set_model",
        "set_thinking_level",
        "prompt",
        "prompt",
      ]);
      NodeAssert.deepEqual(runtimeMock.state.requests.at(-1), {
        type: "prompt",
        message: "Update that request",
        streamingBehavior: "steer",
      });
    }),
  );

  it.effect("interrupts a running turn and returns the session to ready", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-interrupt");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* startPiSession(adapter, threadId);
      const turn = yield* adapter.sendTurn({ threadId, input: "Start a long task" });
      yield* adapter.interruptTurn(threadId);
      const session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "turn.started", "turn.aborted"],
      );
      NodeAssert.equal(String(events.at(-1)?.turnId), String(turn.turnId));
      NodeAssert.deepEqual(events.at(-1)?.payload, { reason: "Interrupted by user." });
      NodeAssert.equal(session?.status, "ready");
      NodeAssert.equal(session?.activeTurnId, undefined);
      NodeAssert.equal(commandType(runtimeMock.state.requests.at(-1) ?? {}), "abort");
    }),
  );

  it.effect("returns the session to ready even when Pi abort fails", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-interrupt-abort-failure");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* startPiSession(adapter, threadId);
      const turn = yield* adapter.sendTurn({ threadId, input: "Start a long task" });
      runtimeMock.state.abortError = new PiRuntimeError({
        operation: "abort",
        detail: "abort timed out",
      });

      yield* adapter.interruptTurn(threadId);
      const session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "turn.started", "turn.aborted"],
      );
      NodeAssert.equal(String(events.at(-1)?.turnId), String(turn.turnId));
      NodeAssert.equal(session?.status, "ready");
      NodeAssert.equal(session?.activeTurnId, undefined);
    }),
  );

  it.effect("does not cache a model switch when thinking setup fails", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-model-thinking-failure");
      yield* startPiSession(adapter, threadId);
      const modelSelection = createModelSelection(
        ProviderInstanceId.make("pi"),
        "anthropic/claude-sonnet-5",
        [{ id: "thinking", value: "high" }],
      );

      runtimeMock.state.thinkingError = new PiRuntimeError({
        operation: "set_thinking_level",
        detail: "thinking unavailable",
      });
      const firstError = yield* adapter
        .sendTurn({ threadId, input: "first", modelSelection })
        .pipe(Effect.flip);
      const failedSession = (yield* adapter.listSessions()).find(
        (entry) => entry.threadId === threadId,
      );
      runtimeMock.state.thinkingError = null;
      yield* adapter.sendTurn({ threadId, input: "retry", modelSelection });

      NodeAssert.equal(firstError._tag, "ProviderAdapterRequestError");
      NodeAssert.equal(failedSession?.model, undefined);
      NodeAssert.deepEqual(runtimeMock.state.requests.map(commandType), [
        "get_state",
        "set_model",
        "set_thinking_level",
        "set_model",
        "set_thinking_level",
        "prompt",
      ]);
    }),
  );

  it.effect("translates Pi events and resolves approval requests", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-events");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(11),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);
      yield* adapter.sendTurn({ threadId, input: "Do the work" });
      const handle = runtimeMock.state.handles[0];
      if (!handle) throw new Error("missing fake Pi handle");

      yield* Queue.offer(handle.eventsQueue, { type: "message_start" });
      yield* Queue.offer(handle.eventsQueue, {
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "Hello", contentIndex: 0 },
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "message_end",
        message: { role: "assistant", content: [{ type: "text", text: "Hello" }] },
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "tool_execution_start",
        toolCallId: "tool-1",
        toolName: "bash",
        args: { command: "printf hi" },
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "tool_execution_end",
        toolCallId: "tool-1",
        toolName: "bash",
        result: { content: [{ type: "text", text: "hi" }] },
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "extension_ui_request",
        id: "approval-1",
        method: "select",
        title: `${PI_APPROVAL_TITLE_PREFIX}{"tool":"bash","detail":"printf hi"}`,
        options: ["allow", "allow-always", "deny"],
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* adapter.respondToRequest(threadId, ApprovalRequestId.make("approval-1"), "accept");
      yield* Queue.offer(handle.eventsQueue, { type: "agent_end" });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        [
          "session.started",
          "thread.started",
          "turn.started",
          "content.delta",
          "item.completed",
          "item.started",
          "item.completed",
          "request.opened",
          "request.resolved",
          "turn.completed",
          "thread.token-usage.updated",
        ],
      );
      NodeAssert.deepEqual(
        [...new Set(events.map((event) => event.providerInstanceId))],
        [ProviderInstanceId.make("pi")],
      );
      NodeAssert.deepEqual(runtimeMock.state.notifications, [
        { type: "extension_ui_response", id: "approval-1", value: "allow" },
      ]);
    }),
  );

  it.effect("classifies MCP tools and gives missing Pi tool ids unique fallback item ids", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-tool-fallback-ids");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(5),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);
      yield* adapter.sendTurn({ threadId, input: "Use tools" });
      const handle = runtimeMock.state.handles[0];
      if (!handle) throw new Error("missing fake Pi handle");

      yield* Queue.offer(handle.eventsQueue, { type: "message_start" });
      yield* Queue.offer(handle.eventsQueue, {
        type: "tool_execution_start",
        toolName: "mcp",
        args: { tool: "preview_status" },
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "tool_execution_start",
        toolName: "multiedit",
        args: { path: "src/app.ts" },
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      const toolEvents = events.filter((event) => event.type === "item.started");
      NodeAssert.equal(toolEvents.length, 2);
      NodeAssert.notEqual(String(toolEvents[0]?.itemId), String(toolEvents[1]?.itemId));
      NodeAssert.equal(
        (toolEvents[0]?.payload as { itemType?: string } | undefined)?.itemType,
        "mcp_tool_call",
      );
      NodeAssert.equal(
        (toolEvents[1]?.payload as { itemType?: string } | undefined)?.itemType,
        "file_change",
      );
    }),
  );

  it.effect("round-trips regular Pi select and confirm dialogs through user input", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-user-input");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(6),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);
      const handle = runtimeMock.state.handles[0];
      if (!handle) throw new Error("missing fake Pi handle");

      yield* Queue.offer(handle.eventsQueue, {
        type: "extension_ui_request",
        id: "select-1",
        method: "select",
        title: "Choose deployment target",
        options: ["Staging", "Production"],
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("select-1"), {
        "select-1": "Staging",
      });

      yield* Queue.offer(handle.eventsQueue, {
        type: "extension_ui_request",
        id: "confirm-1",
        method: "confirm",
        title: "Continue?",
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("confirm-1"), {
        "confirm-1": "No",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        [
          "session.started",
          "thread.started",
          "user-input.requested",
          "user-input.resolved",
          "user-input.requested",
          "user-input.resolved",
        ],
      );
      NodeAssert.deepEqual(events[2]?.payload, {
        questions: [
          {
            id: "select-1",
            header: "Pi",
            question: "Choose deployment target",
            options: [
              { label: "Staging", description: "Staging" },
              { label: "Production", description: "Production" },
            ],
            multiSelect: false,
          },
        ],
      });
      NodeAssert.deepEqual(events[4]?.payload, {
        questions: [
          {
            id: "confirm-1",
            header: "Pi",
            question: "Continue?",
            options: [
              { label: "Yes", description: "Confirm" },
              { label: "No", description: "Decline" },
            ],
            multiSelect: false,
          },
        ],
      });
      NodeAssert.deepEqual(runtimeMock.state.notifications, [
        { type: "extension_ui_response", id: "select-1", value: "Staging" },
        { type: "extension_ui_response", id: "confirm-1", confirmed: false },
      ]);
    }),
  );

  it.effect("cancels unsupported Pi input and editor dialogs", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-unsupported-dialog");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);
      const handle = runtimeMock.state.handles[0];
      if (!handle) throw new Error("missing fake Pi handle");

      yield* Queue.offer(handle.eventsQueue, {
        type: "extension_ui_request",
        id: "input-1",
        method: "input",
        title: "Enter a token",
      });
      yield* Queue.offer(handle.eventsQueue, {
        type: "extension_ui_request",
        id: "editor-1",
        method: "editor",
        title: "Edit generated config",
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "runtime.warning", "runtime.warning"],
      );
      NodeAssert.deepEqual(runtimeMock.state.notifications, [
        { type: "extension_ui_response", id: "input-1", cancelled: true },
        { type: "extension_ui_response", id: "editor-1", cancelled: true },
      ]);
      NodeAssert.deepEqual(events[2]?.payload, {
        message: "Cancelled unsupported Pi extension input dialog: Enter a token",
      });
      NodeAssert.deepEqual(events[3]?.payload, {
        message: "Cancelled unsupported Pi extension editor dialog: Edit generated config",
      });
    }),
  );

  it.effect("rolls back an initial prompt failure but keeps an active turn on steer failure", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const initialFailureThreadId = asThreadId("thread-pi-initial-failure");
      yield* startPiSession(adapter, initialFailureThreadId);

      runtimeMock.state.promptError = new PiRuntimeError({
        operation: "prompt",
        detail: "prompt failed",
      });
      const initialError = yield* adapter
        .sendTurn({ threadId: initialFailureThreadId, input: "fail" })
        .pipe(Effect.flip);
      const initialSession = (yield* adapter.listSessions()).find(
        (session) => session.threadId === initialFailureThreadId,
      );
      NodeAssert.equal(initialError._tag, "ProviderAdapterRequestError");
      NodeAssert.equal(initialSession?.status, "ready");
      NodeAssert.equal(initialSession?.activeTurnId, undefined);
      NodeAssert.equal(initialSession?.lastError, "prompt failed");

      runtimeMock.state.promptError = null;
      const steerFailureThreadId = asThreadId("thread-pi-steer-failure");
      yield* startPiSession(adapter, steerFailureThreadId);
      const turn = yield* adapter.sendTurn({ threadId: steerFailureThreadId, input: "start" });
      runtimeMock.state.promptError = new PiRuntimeError({
        operation: "prompt",
        detail: "steer failed",
      });
      const steerError = yield* adapter
        .sendTurn({ threadId: steerFailureThreadId, input: "steer" })
        .pipe(Effect.flip);
      const steerSession = (yield* adapter.listSessions()).find(
        (session) => session.threadId === steerFailureThreadId,
      );
      NodeAssert.equal(steerError._tag, "ProviderAdapterRequestError");
      NodeAssert.equal(steerSession?.status, "running");
      NodeAssert.equal(String(steerSession?.activeTurnId), String(turn.turnId));
    }),
  );

  it.effect("emits a non-empty turn abort reason for empty Pi prompt errors", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-empty-prompt-error");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(4),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* startPiSession(adapter, threadId);
      runtimeMock.state.promptError = new PiRuntimeError({
        operation: "prompt",
        detail: "",
      });

      const error = yield* adapter.sendTurn({ threadId, input: "fail" }).pipe(Effect.flip);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(error._tag, "ProviderAdapterRequestError");
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "turn.started", "turn.aborted"],
      );
      NodeAssert.deepEqual(events.at(-1)?.payload, { reason: "Pi prompt request failed." });
    }),
  );

  it.effect("reconstructs readThread snapshots with synthetic snapshot turn ids", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-read-thread");
      runtimeMock.state.messagesData = {
        messages: [
          { role: "user", content: "Run the checks" },
          { role: "assistant", content: "I'll inspect the repo." },
          { role: "toolResult", content: [{ type: "text", text: "Tool output" }] },
          { role: "assistant", content: "Done." },
          { role: "user", content: "Now summarize" },
          { role: "assistant", content: "Summary." },
        ],
      };
      yield* startPiSession(adapter, threadId);

      const snapshot = yield* adapter.readThread(threadId);

      NodeAssert.equal(snapshot.turns.length, 2);
      NodeAssert.equal(String(snapshot.turns[0]?.id), "pi-snapshot-turn-0");
      NodeAssert.deepEqual(snapshot.turns[0]?.items, [
        { role: "user", content: "Run the checks" },
        { role: "assistant", content: "I'll inspect the repo." },
        { role: "toolResult", content: [{ type: "text", text: "Tool output" }] },
        { role: "assistant", content: "Done." },
      ]);
      NodeAssert.equal(String(snapshot.turns[1]?.id), "pi-snapshot-turn-1");
      NodeAssert.deepEqual(snapshot.turns[1]?.items, [
        { role: "user", content: "Now summarize" },
        { role: "assistant", content: "Summary." },
      ]);
    }),
  );

  it.effect("rejects malformed Pi message history snapshots", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-malformed-history");
      runtimeMock.state.messagesData = { messages: "not an array" };
      yield* startPiSession(adapter, threadId);

      const error = yield* adapter.readThread(threadId).pipe(Effect.flip);

      NodeAssert.equal(error._tag, "ProviderAdapterRequestError");
      NodeAssert.equal(error.method, "get_messages");
      NodeAssert.equal(error.detail, "Pi returned malformed message history.");
    }),
  );
});
