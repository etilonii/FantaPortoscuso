import { useEffect, useMemo, useState } from "react";
import MarketAdvisorPanel from "./MarketAdvisorPanel";

const MARKET_WINDOW_KEYS = [
  "30-03-2026_02-04-2026",
  "03-02-2026_06-02-2026",
  "17-11-2025_20-11-2025",
  "08-09-2025_11-09-2025",
];

const normalizeTeam = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

export default function MercatoSection({
  API_BASE,
  sessionTeam = "",
  marketUpdatedAt,
  marketCountdown,
  marketItems,
  marketStandings,
  formatInt,
  formatDecimal,
  openPlayer,
  isAdmin = false,
}) {
  const [activeTeam, setActiveTeam] = useState("");
  const [advisorData, setAdvisorData] = useState(null);
  const [advisorLoading, setAdvisorLoading] = useState(false);
  const [advisorError, setAdvisorError] = useState("");
  const [boughtVisibleCount, setBoughtVisibleCount] = useState(8);
  const [releasedVisibleCount, setReleasedVisibleCount] = useState(8);

  const marketTeamsByName = useMemo(() => {
    const map = new Map();
    (marketItems || []).forEach((item, index) => {
      const rawTeam = String(item?.team || "").trim();
      if (!rawTeam) return;
      const entry = map.get(rawTeam) || {
        team: rawTeam,
        items: [],
        count: 0,
        lastDate: "",
        lastIndex: index,
      };
      entry.items.push(item);
      if (item?.date && item.date > entry.lastDate) {
        entry.lastDate = item.date;
      }
      entry.lastIndex = Math.max(entry.lastIndex ?? index, index);
      map.set(rawTeam, entry);
    });
    return Array.from(map.values()).map((entry) => {
      const latestItems = entry.lastDate
        ? entry.items.filter((item) => item.date === entry.lastDate)
        : entry.items;
      return {
        ...entry,
        items: latestItems,
        count: latestItems.length,
      };
    });
  }, [marketItems]);

  const standingsMap = useMemo(() => {
    const map = new Map();
    (marketStandings || []).forEach((row, idx) => {
      const team = String(row?.team || row?.Squadra || "").trim();
      if (!team) return;
      const rawPos = Number(row?.pos ?? row?.Pos ?? idx + 1);
      map.set(normalizeTeam(team), Number.isFinite(rawPos) ? rawPos : idx + 1);
    });
    return map;
  }, [marketStandings]);

  const orderedTeams = useMemo(() => {
    const merged = new Map();
    (marketStandings || []).forEach((row, idx) => {
      const team = String(row?.team || row?.Squadra || "").trim();
      if (!team) return;
      merged.set(team, { team, pos: Number(row?.pos ?? row?.Pos ?? idx + 1), count: 0, items: [] });
    });
    marketTeamsByName.forEach((entry) => {
      const current = merged.get(entry.team) || { team: entry.team, pos: null, count: 0, items: [] };
      merged.set(entry.team, {
        ...current,
        count: entry.count,
        items: entry.items,
      });
    });
    return Array.from(merged.values()).sort((a, b) => {
      const aPos = standingsMap.get(normalizeTeam(a.team));
      const bPos = standingsMap.get(normalizeTeam(b.team));
      const aKnown = Number.isFinite(aPos);
      const bKnown = Number.isFinite(bPos);
      if (aKnown && bKnown && aPos !== bPos) return aPos - bPos;
      if (aKnown && !bKnown) return -1;
      if (!aKnown && bKnown) return 1;
      return a.team.localeCompare(b.team, "it", { sensitivity: "base" });
    });
  }, [marketStandings, marketTeamsByName, standingsMap]);

  const scopedTeam = String(sessionTeam || "").trim();
  const lockedToSessionTeam = !isAdmin && Boolean(scopedTeam);

  useEffect(() => {
    if (lockedToSessionTeam) {
      setActiveTeam(scopedTeam);
      return;
    }
    if (!orderedTeams.length) {
      if (activeTeam) setActiveTeam("");
      return;
    }
    const exists = orderedTeams.some((team) => team.team === activeTeam);
    if (!exists) {
      setActiveTeam(orderedTeams[0].team);
    }
  }, [lockedToSessionTeam, scopedTeam, orderedTeams, activeTeam]);

  useEffect(() => {
    setBoughtVisibleCount(8);
    setReleasedVisibleCount(8);
  }, [marketUpdatedAt]);

  useEffect(() => {
    const teamName = String(activeTeam || "").trim();
    if (!API_BASE || !teamName) {
      setAdvisorData(null);
      setAdvisorError("");
      return;
    }

    let cancelled = false;
    const loadAdvisor = async () => {
      setAdvisorLoading(true);
      setAdvisorError("");

      for (const windowKey of MARKET_WINDOW_KEYS) {
        try {
          const params = new URLSearchParams({ team: teamName, window: windowKey, top_n: "18" });
          const res = await fetch(`${API_BASE}/market/advisor?${params.toString()}`, {
            cache: "no-store",
          });
          const payload = await res.json().catch(() => ({}));
          if (!res.ok) {
            continue;
          }
          if (!cancelled) {
            setAdvisorData(payload);
            setAdvisorLoading(false);
          }
          return;
        } catch {
          // try next configured window
        }
      }

      if (!cancelled) {
        setAdvisorData(null);
        setAdvisorError("Dati mercato non ancora disponibili.");
        setAdvisorLoading(false);
      }
    };

    void loadAdvisor();
    return () => {
      cancelled = true;
    };
  }, [API_BASE, activeTeam]);

  const activeTeamEntry = useMemo(() => {
    return orderedTeams.find((team) => team.team === activeTeam) || null;
  }, [orderedTeams, activeTeam]);

  const latestMarketDate = useMemo(() => {
    const dates = (marketItems || [])
      .map((item) => String(item?.date || "").trim())
      .filter(Boolean)
      .sort();
    return dates.length ? dates[dates.length - 1] : "";
  }, [marketItems]);

  const marketWindowItems = useMemo(() => {
    if (!latestMarketDate) return marketItems || [];
    return (marketItems || []).filter((item) => String(item?.date || "").trim() === latestMarketDate);
  }, [marketItems, latestMarketDate]);

  const rankings = useMemo(() => {
    const aggregate = (nameField, roleField, teamField) => {
      const map = new Map();
      marketWindowItems.forEach((item) => {
        const name = String(item?.[nameField] || "").trim();
        if (!name || name === "-") return;
        const key = name.toLowerCase();
        const row = map.get(key) || {
          name,
          role: String(item?.[roleField] || "").trim(),
          squadra: String(item?.[teamField] || "").trim(),
          count: 0,
          teams: new Set(),
        };
        row.count += 1;
        if (item?.team) row.teams.add(String(item.team).trim());
        map.set(key, row);
      });
      return Array.from(map.values())
        .map((row) => ({
          ...row,
          teamCount: row.teams.size,
          teamsList: Array.from(row.teams).sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" })),
        }))
        .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
    };
    return {
      bought: aggregate("in", "in_ruolo", "in_squadra"),
      released: aggregate("out", "out_ruolo", "out_squadra"),
    };
  }, [marketWindowItems]);

  const visibleBought = useMemo(
    () => rankings.bought.slice(0, boughtVisibleCount),
    [rankings.bought, boughtVisibleCount]
  );
  const visibleReleased = useMemo(
    () => rankings.released.slice(0, releasedVisibleCount),
    [rankings.released, releasedVisibleCount]
  );

  return (
    <section className="dashboard market-advisor-page">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Mercato</p>
          <h2>{lockedToSessionTeam ? "Market Advisor — La tua squadra" : "Market Advisor"}</h2>
          <p className="muted">
            {lockedToSessionTeam
              ? "Vista personale delle mosse possibili per la tua rosa."
              : "Seleziona una squadra e leggi le mosse suggerite in modo piu chiaro."}
          </p>
        </div>

        {marketUpdatedAt ? <div className="market-update-badge">Aggiornamento mercato: {marketUpdatedAt}</div> : null}
      </div>

      <div className="panel market-advisor-header-panel">
        <div className="market-advisor-toolbar">
          {lockedToSessionTeam ? (
            <div className="summary-card">
              <span>Squadra attiva</span>
              <strong>{activeTeam || scopedTeam || "-"}</strong>
            </div>
          ) : (
            <label className="field market-team-select-wrap">
              <span>Squadra</span>
              <select
                className="input market-team-select"
                value={activeTeam || ""}
                onChange={(e) => setActiveTeam(e.target.value)}
              >
                {orderedTeams.map((team) => (
                  <option key={team.team} value={team.team}>
                    {team.pos ? `#${team.pos} ` : ""}
                    {team.team}
                  </option>
                ))}
              </select>
            </label>
          )}

          <div className="summary-card">
            <span>Chiusura mercato</span>
            <strong>{marketCountdown || "Dato non ancora disponibile"}</strong>
          </div>
        </div>
      </div>

      <MarketAdvisorPanel
        advisor={advisorData}
        loading={advisorLoading}
        error={advisorError}
        activeTeam={activeTeam}
        formatInt={formatInt}
        formatDecimal={formatDecimal}
        marketCountdown={marketCountdown}
        marketUpdatedAt={marketUpdatedAt}
        openPlayer={openPlayer}
      />

      <div className="panel market-overview-panel">
        <div className="panel-header">
          <h3>Panoramica mercato</h3>
        </div>
        {!activeTeamEntry?.items?.length ? (
          <p className="muted">Nessun trasferimento disponibile per la squadra selezionata.</p>
        ) : (
          <div className="market-moves-list">
            {activeTeamEntry.items.map((item, idx) => (
              <article key={`${activeTeamEntry.team}-${idx}`} className="market-move-card">
                <div>
                  <p className="rank-title">
                    <span className="team-name">{activeTeamEntry.team}</span>
                  </p>
                  <div className="market-swap-card compact">
                    <div className="market-swap-col">
                      <span className="market-swap-label">Svincolo</span>
                      <span className="market-swap-name">{item.out || "-"}</span>
                      <span className="market-swap-meta-row">
                        <span className="market-swap-meta">
                          {(item.out_ruolo || "-")} · {(item.out_squadra || "-")}
                        </span>
                        <span className="market-swap-value-badge">{formatInt(item.out_value)}</span>
                      </span>
                    </div>
                    <span className="market-swap-arrow">→</span>
                    <div className="market-swap-col">
                      <span className="market-swap-label">Acquisto</span>
                      <span className="market-swap-name">{item.in || "-"}</span>
                      <span className="market-swap-meta-row">
                        <span className="market-swap-meta">
                          {(item.in_ruolo || "-")} · {(item.in_squadra || "-")}
                        </span>
                        <span className="market-swap-value-badge">{formatInt(item.in_value)}</span>
                      </span>
                    </div>
                  </div>
                </div>
                <div className={`market-swap-delta-card ${Number(item.delta) >= 0 ? "pos" : "neg"}`}>
                  <span className="delta-label">Saldo</span>
                  <span className="delta-value">{formatInt(item.delta)}</span>
                </div>
              </article>
            ))}
          </div>
        )}

        <div className="market-ranking-wrap market-ranking-grid advisor-market-ranking-grid">
          <details className="accordion market-accordion">
            <summary>
              <span>Giocatori piu acquistati</span>
              <strong>{rankings.bought.length}</strong>
            </summary>
            {visibleBought.length ? (
              <div className="list market-ranking-list">
                {visibleBought.map((row, idx) => (
                  <div key={`buy-${row.name}-${idx}`} className="list-item market-ranking-item">
                    <div>
                      <p className="rank-title">
                        <span className="rank-badge">#{idx + 1}</span>
                        <span className="market-ranking-name">{row.name}</span>
                      </p>
                      <span className="muted">
                        {(row.role || "-")} · {(row.squadra || "-")} · {row.teamCount} team
                      </span>
                    </div>
                    <strong>{row.count}</strong>
                  </div>
                ))}
              </div>
            ) : (
              <p className="muted">Nessun acquisto disponibile.</p>
            )}
            {rankings.bought.length > visibleBought.length ? (
              <button
                type="button"
                className="ghost market-expand-btn"
                onClick={() => setBoughtVisibleCount((prev) => Math.min(prev + 8, rankings.bought.length))}
              >
                Mostra altri
              </button>
            ) : null}
          </details>

          <details className="accordion market-accordion">
            <summary>
              <span>Giocatori piu svincolati</span>
              <strong>{rankings.released.length}</strong>
            </summary>
            {visibleReleased.length ? (
              <div className="list market-ranking-list">
                {visibleReleased.map((row, idx) => (
                  <div key={`rel-${row.name}-${idx}`} className="list-item market-ranking-item">
                    <div>
                      <p className="rank-title">
                        <span className="rank-badge">#{idx + 1}</span>
                        <span className="market-ranking-name">{row.name}</span>
                      </p>
                      <span className="muted">
                        {(row.role || "-")} · {(row.squadra || "-")} · {row.teamCount} team
                      </span>
                    </div>
                    <strong>{row.count}</strong>
                  </div>
                ))}
              </div>
            ) : (
              <p className="muted">Nessuno svincolo disponibile.</p>
            )}
            {rankings.released.length > visibleReleased.length ? (
              <button
                type="button"
                className="ghost market-expand-btn"
                onClick={() => setReleasedVisibleCount((prev) => Math.min(prev + 8, rankings.released.length))}
              >
                Mostra altri
              </button>
            ) : null}
          </details>
        </div>
      </div>
    </section>
  );
}
