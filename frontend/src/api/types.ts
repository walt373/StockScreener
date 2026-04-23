export type ColumnType = "string" | "money" | "int" | "pct" | "date" | "flag" | "ratio" | "multiple";

export interface ColumnSpec {
  key: string;
  label: string;
  type: ColumnType;
  tooltip?: string | null;
  nullable?: boolean;
}

export interface ScreenerMeta {
  id: string;
  name: string;
  description: string;
  columns: ColumnSpec[];
  default_sort: { key: string; dir: "asc" | "desc" };
}

export interface ScreenerListResponse {
  screeners: ScreenerMeta[];
}

export interface RunInfo {
  id: number;
  started_at: string | null;
  finished_at: string | null;
  tickers_in: number;
  tickers_out: number;
}

export interface ResultRow {
  ticker: string;
  name: string | null;
  sector: string | null;
  exchange: string | null;
  price: number | null;
  market_cap: number | null;
  avg_volume: number | null;
  cash: number | null;
  current_assets: number | null;
  current_liabilities: number | null;
  current_ratio: number | null;
  total_assets: number | null;
  total_liabilities: number | null;
  equity: number | null;
  liabilities_over_assets: number | null;
  revenue_growth: number | null;
  short_interest: number | null;
  trailing_1y_return: number | null;
  realized_vol_1y: number | null;
  furthest_option_expiry: string | null;
  net_income: number | null;
  operating_cash_flow: number | null;
  free_cash_flow: number | null;
  ni_over_mcap: number | null;
  fcf_over_mcap: number | null;
  price_to_book: number | null;
  nearest_debt_maturity: string | null;
  bond_price: number | null;
  bond_yield: number | null;
  bond_last_traded: string | null;
  going_concern_flag: boolean | null;
  ch11_mentions: number | null;
  nt_10k_filed_at: string | null;
  nt_10q_filed_at: string | null;
  [key: string]: unknown;
}

export interface ScreenerResultsResponse {
  screener: ScreenerMeta;
  run: RunInfo | null;
  rows: ResultRow[];
}

export interface RefreshStatus {
  run_id?: number;
  status: "idle" | "queued" | "running" | "ok" | "error" | "already_running";
  stage?: string | null;
  progress?: { done: number; total: number };
  started_at?: string | null;
  finished_at?: string | null;
  tickers_in?: number;
  tickers_out?: number;
  error?: string | null;
}

