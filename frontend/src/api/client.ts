import type {
  BondOverride,
  RefreshStatus,
  ScreenerListResponse,
  ScreenerResultsResponse,
} from "./types";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}: ${await resp.text()}`);
  return resp.json() as Promise<T>;
}

export const api = {
  listScreeners: () => request<ScreenerListResponse>("/api/screeners"),
  getResults: (id: string) => request<ScreenerResultsResponse>(`/api/screeners/${id}/results`),
  refresh: (body: { force?: boolean; limit?: number }) =>
    request<{ run_id: number; status: string }>("/api/refresh", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  refreshStatus: (runId?: number) =>
    request<RefreshStatus>(
      runId !== undefined ? `/api/refresh/status?run_id=${runId}` : "/api/refresh/status"
    ),
  listBondOverrides: () => request<{ overrides: BondOverride[] }>("/api/bond-overrides"),
  upsertBondOverride: (
    ticker: string,
    body: Partial<Omit<BondOverride, "ticker" | "updated_at">>
  ) =>
    request<{ ticker: string; ok: true }>(`/api/bond-overrides/${ticker}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  deleteBondOverride: (ticker: string) =>
    request<{ ticker: string; ok: true }>(`/api/bond-overrides/${ticker}`, {
      method: "DELETE",
    }),
};
