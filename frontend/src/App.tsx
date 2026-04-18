import { useEffect, useMemo, useState } from "react";
import { ResultsTable } from "./components/ResultsTable";
import { useRefresh } from "./hooks/useRefresh";
import { useScreenerList, useScreenerResults } from "./hooks/useScreenerResults";
import { formatRelativeTime } from "./lib/format";

export function App() {
  const screeners = useScreenerList();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const results = useScreenerResults(selectedId);
  const refresh = useRefresh();

  useEffect(() => {
    if (!selectedId && screeners.data?.screeners.length) {
      setSelectedId(screeners.data.screeners[0].id);
    }
  }, [screeners.data, selectedId]);

  const meta = useMemo(() => {
    if (!results.data) return null;
    return results.data.screener;
  }, [results.data]);

  const statusText = useMemo(() => {
    const s = refresh.status?.status;
    if (s === "running" || s === "queued") {
      const stage = refresh.status?.stage ?? "starting";
      const p = refresh.status?.progress;
      const pct = p && p.total ? Math.round((p.done / p.total) * 100) : 0;
      return { cls: "running", text: `Running: ${stage} (${p?.done ?? 0}/${p?.total ?? "?"}, ${pct}%)`, pct };
    }
    if (s === "error") return { cls: "error", text: `Error: ${refresh.status?.error ?? "unknown"}` };
    const finished = results.data?.run?.finished_at;
    if (finished) {
      return {
        cls: "",
        text: `Last refreshed: ${formatRelativeTime(finished)} (${results.data?.run?.tickers_out} rows)`,
      };
    }
    return { cls: "", text: "No completed run yet." };
  }, [refresh.status, results.data]);

  return (
    <div className="app">
      <div className="header">
        <h1>Stock Screener</h1>
        <select
          value={selectedId ?? ""}
          onChange={(e) => setSelectedId(e.target.value || null)}
          disabled={!screeners.data}
        >
          {screeners.data?.screeners.map((s) => (
            <option key={s.id} value={s.id}>
              {s.name}
            </option>
          ))}
        </select>
        <div className="sep" />
        <button
          onClick={() => refresh.trigger({ force: false })}
          disabled={refresh.isRunning}
          title="Refresh the screener. Incremental by default."
        >
          {refresh.isRunning ? "Refreshing…" : "Refresh"}
        </button>
        <button
          onClick={() => refresh.trigger({ force: true })}
          disabled={refresh.isRunning}
          title="Force full refresh (ignore cache)."
        >
          Force Refresh
        </button>
      </div>

      {meta && <div className="description">{meta.description}</div>}

      <div className="status-line">
        <span className={statusText.cls}>{statusText.text}</span>
        {statusText.pct !== undefined && (
          <div className="progress">
            <span style={{ width: `${statusText.pct}%` }} />
          </div>
        )}
      </div>

      {results.isLoading && <div className="empty">Loading…</div>}
      {results.isError && (
        <div className="empty" style={{ color: "var(--red)" }}>
          Failed to load: {(results.error as Error).message}
        </div>
      )}
      {results.data && meta && (
        <ResultsTable
          columns={meta.columns}
          rows={results.data.rows}
          defaultSort={meta.default_sort}
        />
      )}
    </div>
  );
}
