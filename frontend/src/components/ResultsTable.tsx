import { useMemo, useState } from "react";
import type { ColumnSpec, ResultRow } from "../api/types";
import {
  formatDate,
  formatInt,
  formatMoney,
  formatPct,
  formatRatio,
  formatString,
} from "../lib/format";

interface Props {
  columns: ColumnSpec[];
  rows: ResultRow[];
  defaultSort: { key: string; dir: "asc" | "desc" };
}

function renderCell(col: ColumnSpec, value: unknown): JSX.Element {
  if (value === null || value === undefined || value === "") {
    const cls = col.tooltip ? "cell-null cell-tooltip" : "cell-null";
    return <span className={cls} title={col.tooltip ?? undefined}>—</span>;
  }
  switch (col.type) {
    case "money": {
      const n = value as number;
      const cls = n < 0 ? "cell-neg" : "";
      return <span className={cls}>{formatMoney(n)}</span>;
    }
    case "int": {
      const n = value as number;
      if (col.key === "ch11_mentions" && n >= 3) {
        return <span className="cell-ch11-high">{formatInt(n)}</span>;
      }
      return <span>{formatInt(n)}</span>;
    }
    case "pct": {
      const n = value as number;
      const cls = n < 0 ? "cell-neg" : n > 0 ? "cell-pos" : "";
      return <span className={cls}>{formatPct(n)}</span>;
    }
    case "ratio": {
      const n = value as number;
      const cls = n < 0 ? "cell-neg" : n > 0 ? "cell-pos" : "";
      return <span className={cls}>{formatRatio(n)}</span>;
    }
    case "date":
      return <span>{formatDate(value as string)}</span>;
    case "flag": {
      const b = value as boolean;
      return b ? <span className="cell-flag-yes">Yes</span> : <span className="cell-null">—</span>;
    }
    case "string":
    default:
      return <span>{formatString(value as string | null)}</span>;
  }
}

type SortState = { key: string; dir: "asc" | "desc" };

export function ResultsTable({ columns, rows, defaultSort }: Props) {
  const [sort, setSort] = useState<SortState>({ key: defaultSort.key, dir: defaultSort.dir });

  const sorted = useMemo(() => {
    const copy = [...rows];
    const key = sort.key;
    const reverse = sort.dir === "desc";
    copy.sort((a, b) => {
      const av = (a as Record<string, unknown>)[key];
      const bv = (b as Record<string, unknown>)[key];
      const aNull = av === null || av === undefined;
      const bNull = bv === null || bv === undefined;
      if (aNull && bNull) return 0;
      if (aNull) return 1;
      if (bNull) return -1;
      if (typeof av === "number" && typeof bv === "number") {
        return reverse ? bv - av : av - bv;
      }
      const as = String(av);
      const bs = String(bv);
      if (as < bs) return reverse ? 1 : -1;
      if (as > bs) return reverse ? -1 : 1;
      return 0;
    });
    return copy;
  }, [rows, sort]);

  const toggleSort = (key: string) => {
    setSort((prev) => {
      if (prev.key !== key) return { key, dir: "asc" };
      return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
    });
  };

  if (rows.length === 0) {
    return (
      <div className="empty">
        No rows yet. Click <strong>Refresh</strong> to run the screener.
      </div>
    );
  }

  return (
    <div className="table-wrap">
      <table className="results">
        <thead>
          <tr>
            {columns.map((c) => {
              const isSorted = sort.key === c.key;
              const arrow = !isSorted ? "" : sort.dir === "asc" ? " ▲" : " ▼";
              return (
                <th
                  key={c.key}
                  className={isSorted ? "sorted" : undefined}
                  title={c.tooltip ?? undefined}
                  onClick={() => toggleSort(c.key)}
                >
                  {c.label}
                  {arrow}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => (
            <tr key={r.ticker}>
              {columns.map((c) => (
                <td key={c.key}>{renderCell(c, (r as Record<string, unknown>)[c.key])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
