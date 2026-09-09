import type { HarnessInfo } from "../types";

export const HARNESS_INSTALL_COMMANDS: Readonly<Record<string, string>> = {
  codex: "npm install -g @openai/codex",
  "claude-code": "curl -fsSL https://claude.ai/install.sh | bash",
  "gemini-cli": "npm install -g @google/gemini-cli",
  antigravity: "curl -fsSL https://antigravity.google/cli/install.sh | bash",
  kiro: "curl -fsSL https://cli.kiro.dev/install | bash",
  opencode: "npm install -g opencode-ai",
  agy: "test \"$(uname -sm)\" = \"Darwin arm64\" && mkdir -p \"$HOME/.local/bin\" && agy_adapter_tmp=$(mktemp) && curl -fL https://github.com/shubzkothekar/antigravity-acp/releases/download/v1.1.0/agy-acp-darwin-arm64 -o \"$agy_adapter_tmp\" && echo \"9ef7afa432341c05d6c049d143349ea71fbb48989813ba625054a7224e2804fc  $agy_adapter_tmp\" | shasum -a 256 -c - && install -m 755 \"$agy_adapter_tmp\" \"$HOME/.local/bin/agy-acp\" && rm \"$agy_adapter_tmp\"",
  hermes: "curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash",
  "kilo-code": "npm install -g @kilocode/cli",
  "factory-droid": "curl -fsSL https://app.factory.ai/cli | sh",
};

// npm package name for each harness whose install command in
// HARNESS_INSTALL_COMMANDS is an `npm install -g <package>` invocation.
// Used to build the `mise use -g npm:<package>` equivalent when mise is
// the preferred install method (e.g. inside a Flatpak sandbox, where /app
// is read-only and a global npm install has nowhere to write to).
const NPM_PACKAGE_NAMES: Readonly<Record<string, string>> = {
  codex: "@openai/codex",
  "gemini-cli": "@google/gemini-cli",
  opencode: "opencode-ai",
  "kilo-code": "@kilocode/cli",
};

export type HarnessTileAction = "launch" | "install" | "manual";

export function getHarnessInstallCommand(
  harnessId: string,
  preferredInstallMethod: string | null = null,
): string | null {
  if (preferredInstallMethod === "mise") {
    const npmPackage = NPM_PACKAGE_NAMES[harnessId];
    if (npmPackage) {
      return `mise use -g npm:${npmPackage}`;
    }
  }

  return HARNESS_INSTALL_COMMANDS[harnessId] ?? null;
}

export function getHarnessTileAction(harness: HarnessInfo): HarnessTileAction | null {
  if (harness.found) {
    return "launch";
  }

  if (harness.canAutoInstall && getHarnessInstallCommand(harness.id)) {
    return "install";
  }

  if (harness.website) {
    return "manual";
  }

  return null;
}
