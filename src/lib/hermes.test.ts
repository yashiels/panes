import { describe, expect, it } from "vitest";
import { engineKind, isBuiltinEngineId, isChatProviderKind } from "./engineKind";
import { engineSupportsSteering } from "./engineSteering";
import { autonomyPresetExecutionPolicyRequest, visibleAutonomyPresets } from "./autonomyPresets";
import { resolvePreferredOnboardingChatSelection, isChatEngineReady } from "./onboarding";
import { resolveEngineCapabilities } from "../components/chat/engineCapabilities";
import { chatProviderSignInCommand } from "./chatProviders";

describe("Hermes registration", () => {
  it("recognizes builtin and custom Hermes instances", () => {
    expect(engineKind("hermes_work")).toBe("hermes");
    expect(isBuiltinEngineId("hermes")).toBe(true);
    expect(isChatProviderKind("hermes")).toBe(true);
    expect(engineSupportsSteering("hermes_work")).toBe(false);
  });

  it("never assigns sandbox or autonomy modes to Hermes", () => {
    expect(resolveEngineCapabilities("hermes_work").permissionModes).toEqual([]);
    expect(resolveEngineCapabilities("hermes_work").sandboxModes).toEqual([]);
    expect(visibleAutonomyPresets("hermes", "full")).toEqual([]);
    expect(autonomyPresetExecutionPolicyRequest("full", "hermes")).toBeNull();
  });

  it("selects the configured model during onboarding with independent health", () => {
    expect(resolvePreferredOnboardingChatSelection(["hermes"], [{ id: "hermes", models: [{ id: "default", hidden: false, isDefault: true }] }])).toEqual({ engineId: "hermes", modelId: "default" });
    expect(isChatEngineReady("hermes", null, { hermes: { id: "hermes", available: true } })).toBe(true);
  });

  it("opens setup with the Hermes instance configuration directory", () => {
    expect(chatProviderSignInCommand({ id: "hermes_work", kind: "hermes", displayName: "Work", binaryPath: null, homePath: "~/.hermes-work", launchArgs: null, env: {}, enabled: true, builtIn: false })).toBe('HERMES_HOME="$HOME/.hermes-work" hermes setup');
  });
});
