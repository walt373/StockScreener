import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import { useBondOverrides } from "../hooks/useScreenerResults";

interface Props {
  onClose: () => void;
}

export function BondOverridesDialog({ onClose }: Props) {
  const qc = useQueryClient();
  const { data } = useBondOverrides();
  const [ticker, setTicker] = useState("");
  const [price, setPrice] = useState("");
  const [yieldPct, setYieldPct] = useState("");
  const [lastTraded, setLastTraded] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const overrides = data?.overrides ?? [];

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["bond-overrides"] });
    qc.invalidateQueries({ queryKey: ["results"] });
  };

  const onSave = async () => {
    const t = ticker.trim().toUpperCase();
    if (!t) return;
    setError(null);
    setSaving(true);
    try {
      await api.upsertBondOverride(t, {
        price: price === "" ? null : parseFloat(price),
        yield_pct: yieldPct === "" ? null : parseFloat(yieldPct),
        last_traded_date: lastTraded || null,
      });
      setTicker("");
      setPrice("");
      setYieldPct("");
      setLastTraded("");
      invalidate();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const onDelete = async (t: string) => {
    try {
      await api.deleteBondOverride(t);
      invalidate();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div className="dialog" onClick={(e) => e.stopPropagation()}>
        <header>Bond Overrides</header>
        <div className="body">
          <p style={{ color: "var(--muted)", margin: 0, fontSize: 12 }}>
            Bond price / yield / last traded date aren&apos;t available from free sources at scale.
            Paste values here for tickers you&apos;re tracking and they&apos;ll join into the
            results table.
          </p>

          <label>Ticker</label>
          <input type="text" value={ticker} onChange={(e) => setTicker(e.target.value)} />

          <label>Price (clean, per $100)</label>
          <input type="number" value={price} onChange={(e) => setPrice(e.target.value)} />

          <label>Yield (%)</label>
          <input type="number" value={yieldPct} onChange={(e) => setYieldPct(e.target.value)} />

          <label>Last traded (YYYY-MM-DD)</label>
          <input type="text" value={lastTraded} onChange={(e) => setLastTraded(e.target.value)} />

          {error && <div style={{ color: "var(--red)", marginTop: 8 }}>{error}</div>}

          {overrides.length > 0 && (
            <table className="overrides">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Price</th>
                  <th>Yield</th>
                  <th>Last traded</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {overrides.map((o) => (
                  <tr key={o.ticker}>
                    <td>{o.ticker}</td>
                    <td>{o.price ?? "—"}</td>
                    <td>{o.yield_pct ?? "—"}</td>
                    <td>{o.last_traded_date ?? "—"}</td>
                    <td>
                      <button onClick={() => onDelete(o.ticker)}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <footer>
          <button onClick={onClose}>Close</button>
          <button onClick={onSave} disabled={saving || !ticker.trim()}>
            {saving ? "Saving…" : "Save"}
          </button>
        </footer>
      </div>
    </div>
  );
}
