import { useTranslation } from "react-i18next";
import { useParsedDiff, VirtualizedDiffBody } from "../shared/DiffViewer";

export function AcpApprovalDiff({ diff }: { diff: string }) {
  const { t } = useTranslation("chat");
  const { parseResult, loading } = useParsedDiff(diff);

  if (loading) return <div>{t("messageBlocks.parsingDiff")}</div>;
  if (!parseResult?.parsed.length) return <pre>{diff}</pre>;

  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: "var(--radius-sm)", background: "var(--code-bg)" }}>
      <VirtualizedDiffBody parsed={parseResult.parsed} maxHeight={260} foldContext />
    </div>
  );
}
