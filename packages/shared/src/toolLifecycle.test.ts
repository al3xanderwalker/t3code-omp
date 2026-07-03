import { describe, expect, it } from "vite-plus/test";

import { toToolLifecycleItemType } from "./toolLifecycle.ts";

describe("toolLifecycle", () => {
  it("classifies provider tool names into canonical lifecycle item types", () => {
    expect(toToolLifecycleItemType("bash")).toBe("command_execution");
    expect(toToolLifecycleItemType("multiedit")).toBe("file_change");
    expect(toToolLifecycleItemType("mcp")).toBe("mcp_tool_call");
    expect(toToolLifecycleItemType("subtask")).toBe("collab_agent_tool_call");
    expect(toToolLifecycleItemType("unknown")).toBe("dynamic_tool_call");
  });
});
