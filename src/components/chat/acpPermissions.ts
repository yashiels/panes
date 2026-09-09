export interface AcpPermissionOption {
  optionId: string;
  name: string;
  kind: string;
}

export function acpPermissionOptions(details: Record<string, unknown>): AcpPermissionOption[] {
  if (!Array.isArray(details.options)) return [];
  return details.options.filter((option): option is AcpPermissionOption =>
    option !== null && typeof option === "object" &&
    typeof option.optionId === "string" && typeof option.name === "string" &&
    typeof option.kind === "string",
  );
}
