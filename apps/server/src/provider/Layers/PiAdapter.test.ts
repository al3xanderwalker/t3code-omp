import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderDriverKind,
  ProviderInstanceId,
  PiSettings,
  ThreadId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { beforeEach } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
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
  readonly handle: PiRpcHandle;
};

const runtimeMock = {
  state: {
    spawnInputs: [] as Array<SpawnPiRpcInput>,
    handles: [] as Array<FakePiHandle>,
    requests: [] as Array<Record<string, unknown>>,
    notifications: [] as Array<Record<string, unknown>>,
    closeCalls: [] as Array<string | undefined>,
    promptError: null as PiRuntimeError | null,
    stateData: { sessionId: "pi-session-1" } as unknown,
    statsData: {
      contextUsage: { tokens: 42, contextWindow: 1_000 },
      tokens: { input: 10, cacheRead: 2, output: 30, total: 42 },
      toolCalls: 1,
    } as unknown,
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
    this.state.promptError = null;
    this.state.stateData = { sessionId: "pi-session-1" };
    this.state.statsData = {
      contextUsage: { tokens: 42, contextWindow: 1_000 },
      tokens: { input: 10, cacheRead: 2, output: 30, total: 42 },
      toolCalls: 1,
    };
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
      const eventsQueue = yield* Queue.unbounded<PiRpcEvent>();
      const handle: PiRpcHandle = {
        request: (command) =>
          Effect.gen(function* () {
            runtimeMock.state.requests.push(command);
            const type = commandType(command);
            if (type === "prompt" && runtimeMock.state.promptError) {
              return yield* runtimeMock.state.promptError;
            }
            if (type === "get_state") {
              return {
                type: "response",
                command: type,
                success: true,
                data: runtimeMock.state.stateData,
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
        exitCode: Effect.never,
      };
      const fakeHandle = { input, eventsQueue, handle };
      runtimeMock.state.spawnInputs.push(input);
      runtimeMock.state.handles.push(fakeHandle);
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

beforeEach(() => {
  runtimeMock.reset();
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
      NodeAssert.deepEqual(runtimeMock.state.requests.map(commandType), ["get_state", "abort"]);
      NodeAssert.deepEqual(
        events.map((event) => event.type),
        ["session.started", "thread.started", "session.exited"],
      );
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
      NodeAssert.deepEqual(runtimeMock.state.notifications, [
        { type: "extension_ui_response", id: "approval-1", value: "allow" },
      ]);
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

  it.effect("reconstructs readThread snapshots with synthetic snapshot turn ids", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const threadId = asThreadId("thread-pi-read-thread");
      yield* startPiSession(adapter, threadId);

      const snapshot = yield* adapter.readThread(threadId);

      NodeAssert.equal(snapshot.turns.length, 1);
      NodeAssert.equal(String(snapshot.turns[0]?.id), "pi-snapshot-turn-0");
      NodeAssert.deepEqual(snapshot.turns[0]?.items, [
        { role: "assistant", content: "Hello" },
        { role: "toolResult", content: [{ type: "text", text: "Tool output" }] },
      ]);
    }),
  );
});
