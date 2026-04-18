export function formatMoney(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  let str: string;
  if (abs >= 1e12) str = `$${(abs / 1e12).toFixed(2)}T`;
  else if (abs >= 1e9) str = `$${(abs / 1e9).toFixed(2)}B`;
  else if (abs >= 1e6) str = `$${(abs / 1e6).toFixed(2)}M`;
  else if (abs >= 1e3) str = `$${(abs / 1e3).toFixed(1)}K`;
  else str = `$${abs.toFixed(2)}`;
  return `${sign}${str}`;
}

export function formatInt(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(1)}K`;
  return `${sign}${abs.toFixed(0)}`;
}

export function formatPct(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  const pct = v * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

export function formatRatio(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(3)}`;
}

export function formatMultiple(v: number | null | undefined): string {
  if (v === null || v === undefined || !isFinite(v)) return "—";
  return `${v.toFixed(2)}x`;
}

export function formatDate(v: string | null | undefined): string {
  if (!v) return "—";
  return v.slice(0, 10);
}

export function formatString(v: string | null | undefined): string {
  if (v === null || v === undefined || v === "") return "—";
  return v;
}

export function formatRelativeTime(iso: string | null | undefined): string {
  if (!iso) return "never";
  const t = new Date(iso).getTime();
  if (isNaN(t)) return "never";
  const diff = Date.now() - t;
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d}d ago`;
}
