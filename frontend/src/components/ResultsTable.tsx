import { useMemo, useState } from "react";
import type { ColumnSpec, ResultRow } from "../api/types";
import {
  formatDate,
  formatInt,
  formatMoney,
  formatMultiple,
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
    case "multiple": {
      const n = value as number;
      // For current ratio: <1 = red (illiquid), >=1 = neutral
      const cls = n < 1 ? "cell-neg" : "";
      return <span className={cls}>{formatMultiple(n)}</span>;
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

// -------- Filters --------

type NumFilter = { min?: number; max?: number };
type DateFilter = { sinceYear?: number };
type StringFilter = { contains?: string };
type FlagFilter = { value?: "yes" | "no" };
type Filter = NumFilter | DateFilter | StringFilter | FlagFilter;
type FilterMap = Record<string, Filter>;

function isNumericCol(t: ColumnSpec["type"]): boolean {
  return t === "money" || t === "int" || t === "pct" || t === "ratio" || t === "multiple";
}

function matchesFilter(col: ColumnSpec, value: unknown, filter: Filter | undefined): boolean {
  if (!filter) return true;
  if (isNumericCol(col.type)) {
    const f = filter as NumFilter;
    if (f.min === undefined && f.max === undefined) return true;
    if (value === null || value === undefined) return false;
    let n = Number(value);
    if (Number.isNaN(n)) return false;
    // `pct` values are stored as fractions (e.g. 0.5). User types percent units (50).
    const scale = col.type === "pct" ? 100 : 1;
    n = n * scale;
    if (f.min !== undefined && n < f.min) return false;
    if (f.max !== undefined && n > f.max) return false;
    return true;
  }
  if (col.type === "date") {
    const f = filter as DateFilter;
    if (f.sinceYear === undefined) return true;
    if (!value) return false;
    const y = parseInt(String(value).slice(0, 4), 10);
    if (Number.isNaN(y)) return false;
    return y >= f.sinceYear;
  }
  if (col.type === "flag") {
    const f = filter as FlagFilter;
    if (!f.value) return true;
    if (f.value === "yes") return !!value;
    return !value; // "no" — includes null and false
  }
  // string
  const f = filter as StringFilter;
  if (!f.contains) return true;
  return String(value ?? "").toLowerCase().includes(f.contains.toLowerCase());
}

interface FilterInputProps {
  col: ColumnSpec;
  value: Filter | undefined;
  onChange: (v: Filter | undefined) => void;
  availableYears?: number[];
}

function FilterInput({ col, value, onChange, availableYears = [] }: FilterInputProps) {
  if (isNumericCol(col.type)) {
    const f = (value ?? {}) as NumFilter;
    const suffix = col.type === "pct" ? "%" : "";
    return (
      <div className="filter-numeric">
        <input
          type="number"
          placeholder={`min${suffix}`}
          value={f.min ?? ""}
          onChange={(e) => {
            const raw = e.target.value;
            onChange({ ...f, min: raw === "" ? undefined : Number(raw) });
          }}
        />
        <input
          type="number"
          placeholder={`max${suffix}`}
          value={f.max ?? ""}
          onChange={(e) => {
            const raw = e.target.value;
            onChange({ ...f, max: raw === "" ? undefined : Number(raw) });
          }}
        />
      </div>
    );
  }
  if (col.type === "date") {
    const f = (value ?? {}) as DateFilter;
    return (
      <select
        value={f.sinceYear ?? ""}
        onChange={(e) => {
          const raw = e.target.value;
          onChange(raw === "" ? undefined : { sinceYear: Number(raw) });
        }}
      >
        <option value="">any year</option>
        {availableYears.map((y) => (
          <option key={y} value={y}>
            {y}+
          </option>
        ))}
      </select>
    );
  }
  if (col.type === "flag") {
    const f = (value ?? {}) as FlagFilter;
    return (
      <select
        value={f.value ?? ""}
        onChange={(e) => {
          const v = e.target.value;
          onChange(v === "" ? undefined : { value: v as "yes" | "no" });
        }}
      >
        <option value="">any</option>
        <option value="yes">Yes</option>
        <option value="no">No / blank</option>
      </select>
    );
  }
  const f = (value ?? {}) as StringFilter;
  return (
    <input
      type="text"
      placeholder="contains"
      value={f.contains ?? ""}
      onChange={(e) => {
        const raw = e.target.value;
        onChange(raw === "" ? undefined : { contains: raw });
      }}
    />
  );
}

function isFilterEmpty(f: Filter | undefined): boolean {
  if (!f) return true;
  return Object.values(f).every((v) => v === undefined || v === "");
}

// -------- Table --------

type SortState = { key: string; dir: "asc" | "desc" };

export function ResultsTable({ columns, rows, defaultSort }: Props) {
  const [sort, setSort] = useState<SortState>({ key: defaultSort.key, dir: defaultSort.dir });
  const [filters, setFilters] = useState<FilterMap>({});

  // Available years per date column — powers the year dropdown in the filter row.
  const dateYearsByCol = useMemo<Record<string, number[]>>(() => {
    const out: Record<string, number[]> = {};
    for (const c of columns) {
      if (c.type !== "date") continue;
      const years = new Set<number>();
      for (const r of rows) {
        const v = (r as Record<string, unknown>)[c.key];
        if (!v) continue;
        const y = parseInt(String(v).slice(0, 4), 10);
        if (!Number.isNaN(y)) years.add(y);
      }
      out[c.key] = [...years].sort((a, b) => a - b);
    }
    return out;
  }, [columns, rows]);

  const filtered = useMemo(() => {
    const active = Object.entries(filters).filter(([, f]) => !isFilterEmpty(f));
    if (active.length === 0) return rows;
    const byCol = Object.fromEntries(columns.map((c) => [c.key, c]));
    return rows.filter((r) => {
      for (const [key, f] of active) {
        const col = byCol[key];
        if (!col) continue;
        if (!matchesFilter(col, (r as Record<string, unknown>)[key], f)) return false;
      }
      return true;
    });
  }, [rows, filters, columns]);

  const sorted = useMemo(() => {
    const copy = [...filtered];
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
  }, [filtered, sort]);

  const toggleSort = (key: string) => {
    setSort((prev) => {
      if (prev.key !== key) return { key, dir: "asc" };
      return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
    });
  };

  const setFilter = (key: string, v: Filter | undefined) => {
    setFilters((prev) => {
      const next = { ...prev };
      if (v === undefined || isFilterEmpty(v)) {
        delete next[key];
      } else {
        next[key] = v;
      }
      return next;
    });
  };

  const activeFilterCount = Object.values(filters).filter((f) => !isFilterEmpty(f)).length;

  if (rows.length === 0) {
    return (
      <div className="empty">
        No rows yet. Click <strong>Refresh</strong> to run the screener.
      </div>
    );
  }

  return (
    <>
      <div className="filter-toolbar">
        <span>
          Showing <strong>{sorted.length}</strong> of {rows.length} rows
          {activeFilterCount > 0 && (
            <>
              {" "}
              · {activeFilterCount} filter{activeFilterCount === 1 ? "" : "s"} active
            </>
          )}
        </span>
        {activeFilterCount > 0 && (
          <button onClick={() => setFilters({})}>Clear all filters</button>
        )}
      </div>
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
            <tr className="filter-row">
              {columns.map((c) => (
                <th key={c.key}>
                  <FilterInput
                    col={c}
                    value={filters[c.key]}
                    onChange={(v) => setFilter(c.key, v)}
                    availableYears={dateYearsByCol[c.key]}
                  />
                </th>
              ))}
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
    </>
  );
}
