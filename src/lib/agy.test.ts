import { describe, expect, it } from "vitest";
import { engineKind, isBuiltinEngineId, isChatProviderKind } from "./engineKind";
import { engineSupportsSteering } from "./engineSteering";
import { autonomyPresetExecutionPolicyRequest, visibleAutonomyPresets } from "./autonomyPresets";
import { resolvePreferredOnboardingChatSelection, isChatEngineReady } from "./onboarding";
import { resolveEngineCapabilities } from "../components/chat/engineCapabilities";
import { chatProviderSignInCommand } from "./chatProviders";

describe("Antigravity registration", () => {
  it("recognizes builtin and custom Antigravity instances", () => {
    expect(engineKind("agy_work")).toBe("agy");
    expect(isBuiltinEngineId("agy")).toBe(true);
    expect(isChatProviderKind("agy")).toBe(true);
    expect(engineSupportsSteering("agy_work")).toBe(false);
  });

  it("never assigns sandbox or autonomy modes to Antigravity", () => {
    expect(resolveEngineCapabilities("agy_work").permissionModes).toEqual([]);
    expect(resolveEngineCapabilities("agy_work").sandboxModes).toEqual([]);
    expect(visibleAutonomyPresets("agy", "full")).toEqual([]);
    expect(autonomyPresetExecutionPolicyRequest("full", "agy")).toBeNull();
  });

  it("selects the configured model during onboarding with independent health", () => {
    expect(resolvePreferredOnboardingChatSelection(["agy"], [{ id: "agy", models: [{ id: "gemini-3.8-flash-high", hidden: false, isDefault: true }] }])).toEqual({ engineId: "agy", modelId: "gemini-3.8-flash-high" });
    expect(isChatEngineReady("agy", null, { agy: { id: "agy", available: true } })).toBe(true);
  });

  it("signs in through agy rather than the ACP adapter", () => {
    expect(chatProviderSignInCommand({ id: "agy_work", kind: "agy", displayName: "Work", binaryPath: "/custom/agy-acp", homePath: null, launchArgs: null, env: {}, enabled: true, builtIn: false })).toBe("agy");
    expect(resolveEngineCapabilities("agy").diffs).toBe(false);
  });
});
