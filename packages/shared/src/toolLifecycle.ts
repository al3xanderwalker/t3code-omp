import type { ToolLifecycleItemType } from "@t3tools/contracts";

const TOOL_LIFECYCLE_MATCHERS: ReadonlyArray<
  readonly [ToolLifecycleItemType, ReadonlyArray<string>]
> = [
  ["command_execution", ["bash", "command"]],
  ["file_change", ["edit", "write", "patch", "multiedit"]],
  ["web_search", ["web"]],
  ["mcp_tool_call", ["mcp"]],
  ["image_view", ["image"]],
  ["collab_agent_tool_call", ["task", "agent", "subtask"]],
];

export function toToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  return (
    TOOL_LIFECYCLE_MATCHERS.find(([, needles]) =>
      needles.some((needle) => normalized.includes(needle)),
    )?.[0] ?? "dynamic_tool_call"
  );
}
