import { describe, expect, it } from "vitest";
import { acpPermissionOptions } from "./acpPermissions";

describe("ACP permission options", () => {
  it("preserves server identities and labels without inferring decisions", () => {
    const options = [
      { optionId: "permit-this-command", name: "Run once", kind: "allow_once" },
      { optionId: "block-this-command", name: "Reject", kind: "reject_once" },
    ];
    expect(acpPermissionOptions({ options })).toEqual(options);
  });

  it("rejects malformed options without inventing generic approvals", () => {
    expect(acpPermissionOptions({ options: [null, {}, { name: "Allow" }] })).toEqual([]);
    expect(acpPermissionOptions({})).toEqual([]);
  });
});
