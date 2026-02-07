import { useEffect, useMemo, useState } from "react";

export default function MercatoSection({
  marketUpdatedAt,
  marketCountdown,
  marketItems,
  marketStandings,
  formatInt,
}) {
  const [activeTeam, setActiveTeam] = useState("");

  const marketTeamsByName = useMemo(() => {
    const map = new Map();
    marketItems.forEach((item, index) => {
      const rawTeam = (item.team || "").trim();
      if (!rawTeam) return;
      const entry = map.get(rawTeam) || {
        team: rawTeam,
        items: [],
        count: 0,
        lastDate: "",
        lastIndex: index,
      };
      entry.items.push(item);
      if (item.date && item.date > entry.lastDate) {
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

  const normalizeTeam = (value) =>
    String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");

  const standingsMap = useMemo(() => {
    const map = new Map();
    (marketStandings || []).forEach((row, idx) => {
      const team = String(row.team || row.Squadra || "").trim();
      if (!team) return;
      const rawPos = Number(row.pos ?? row.Pos ?? idx + 1);
      const pos = Number.isFinite(rawPos) ? rawPos : idx + 1;
      map.set(normalizeTeam(team), pos);
    });
    return map;
  }, [marketStandings]);

  const orderedTeams = useMemo(() => {
    return [...marketTeamsByName]
      .sort((a, b) => {
      const aPos = standingsMap.get(normalizeTeam(a.team));
      const bPos = standingsMap.get(normalizeTeam(b.team));
      const aKnown = Number.isFinite(aPos);
      const bKnown = Number.isFinite(bPos);
      if (aKnown && bKnown && aPos !== bPos) return aPos - bPos;
      if (aKnown && !bKnown) return -1;
      if (!aKnown && bKnown) return 1;
      return a.team.localeCompare(b.team, "it", { sensitivity: "base" });
      })
      .map((team) => {
        const pos = standingsMap.get(normalizeTeam(team.team));
        return {
          ...team,
          pos: Number.isFinite(pos) ? pos : null,
        };
      });
  }, [marketTeamsByName, standingsMap]);

  const activeTeamEntry = useMemo(() => {
    if (!orderedTeams.length) return null;
    return (
      orderedTeams.find((team) => team.team === activeTeam) ||
      orderedTeams[0] ||
      null
    );
  }, [orderedTeams, activeTeam]);

  const latestMarketDate = useMemo(() => {
    const dates = (marketItems || [])
      .map((item) => String(item?.date || "").trim())
      .filter(Boolean);
    if (!dates.length) return "";
    const sorted = dates.sort();
    return sorted[sorted.length - 1] || "";
  }, [marketItems]);

  const marketWindowItems = useMemo(() => {
    if (!latestMarketDate) return marketItems || [];
    return (marketItems || []).filter(
      (item) => String(item?.date || "").trim() === latestMarketDate
    );
  }, [marketItems, latestMarketDate]);

  const buildRank = useMemo(() => {
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
        if (!row.role && item?.[roleField]) row.role = String(item[roleField]).trim();
        if (!row.squadra && item?.[teamField]) row.squadra = String(item[teamField]).trim();
        map.set(key, row);
      });
      return Array.from(map.values())
        .map((row) => ({
          ...row,
          teamCount: row.teams.size,
        }))
        .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
    };
    return {
      bought: aggregate("in", "in_ruolo", "in_squadra"),
      released: aggregate("out", "out_ruolo", "out_squadra"),
    };
  }, [marketWindowItems]);

  const topBought = useMemo(() => buildRank.bought.slice(0, 20), [buildRank]);
  const topReleased = useMemo(() => buildRank.released.slice(0, 20), [buildRank]);

  useEffect(() => {
    if (!orderedTeams.length) {
      if (activeTeam) setActiveTeam("");
      return;
    }
    const exists = orderedTeams.some((team) => team.team === activeTeam);
    if (!exists) {
      setActiveTeam(orderedTeams[0].team);
    }
  }, [orderedTeams, activeTeam]);

  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Mercato</p>
          <h2>Mercato Aperto</h2>
          {marketUpdatedAt ? (
            <div className="market-update-badge">
              Aggiornamento mercato: {marketUpdatedAt}
            </div>
          ) : null}
        </div>
      </div>

      <div className="panel market-panel">
        <div className="market-warning">
          <div className="market-warning-badge">Live</div>
          <h3>Trasferimenti di mercato</h3>
          <p className="muted">Ultimi cambi registrati. Seleziona un team per i dettagli.</p>

          <div className="market-countdown-inline">
            <span>Chiusura tra</span>
            <strong>{marketCountdown}</strong>
          </div>
          {marketItems.length ? (
            <>
              {orderedTeams.length ? (
                <>
                  <div className="market-team-controls market-team-select-wrap">
                    <label className="market-team-select-label" htmlFor="market-team-select">
                      Team
                    </label>
                    <select
                      id="market-team-select"
                      className="input market-team-select"
                      value={activeTeamEntry?.team || ""}
                      onChange={(e) => setActiveTeam(e.target.value)}
                    >
                      {orderedTeams.map((team) => (
                        <option key={team.team} value={team.team}>
                          {team.pos ? `#${team.pos} ` : ""}{team.team} ({team.count})
                        </option>
                      ))}
                    </select>
                  </div>

                  {activeTeamEntry ? (
                    <div className="list market-preview-list">
                      {activeTeamEntry.items.map((item, idx) => (
                        <div
                          key={`${activeTeamEntry.team}-${idx}`}
                          className="list-item player-card"
                        >
                          <div>
                            <p className="rank-title">
                              <span className="team-name">
                                {activeTeamEntry.pos ? `#${activeTeamEntry.pos} ` : ""}
                                {activeTeamEntry.team}
                              </span>
                            </p>
                            <div className="market-swap-card">
                              <div className="market-swap-col">
                                <span className="market-swap-label">Svincolo</span>
                                <span className="market-swap-name">{item.out || "-"}</span>
                                <span className="market-swap-meta-row">
                                  <span className="market-swap-meta">
                                    {(item.out_ruolo || "-")} - {(item.out_squadra || "-")}
                                  </span>
                                  <span className="market-swap-value-badge">
                                    {formatInt(item.out_value)}
                                  </span>
                                </span>
                              </div>
                              <span className="market-swap-arrow">-&gt;</span>
                              <div className="market-swap-col">
                                <span className="market-swap-label">Acquisto</span>
                                <span className="market-swap-name">{item.in || "-"}</span>
                                <span className="market-swap-meta-row">
                                  <span className="market-swap-meta">
                                    {(item.in_ruolo || "-")} - {(item.in_squadra || "-")}
                                  </span>
                                  <span className="market-swap-value-badge">
                                    {formatInt(item.in_value)}
                                  </span>
                                </span>
                              </div>
                              <div
                                className={`market-swap-delta-card ${
                                  Number(item.delta) >= 0 ? "pos" : "neg"
                                }`}
                              >
                                <span className="delta-label">Saldo</span>
                                <span className="delta-value">{formatInt(item.delta)}</span>
                              </div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">Nessun cambio disponibile.</p>
                  )}

                  <div className="market-ranking-wrap">
                    <details className="accordion market-accordion" open>
                      <summary>
                        <span>Giocatori più acquistati</span>
                        <strong>{topBought.length}</strong>
                      </summary>
                      {topBought.length ? (
                        <div className="list market-ranking-list">
                          {topBought.map((row, idx) => (
                            <div key={`buy-${row.name}-${idx}`} className="list-item market-ranking-item">
                              <div>
                                <p className="rank-title">
                                  <span
                                    className={`rank-badge ${
                                      idx === 0 ? "gold" : idx === 1 ? "silver" : idx === 2 ? "bronze" : ""
                                    }`}
                                  >
                                    #{idx + 1}
                                  </span>
                                  <span className="market-ranking-name">{row.name}</span>
                                </p>
                                <span className="muted">
                                  {(row.role || "-")} - {(row.squadra || "-")} · {row.teamCount} team
                                </span>
                              </div>
                              <strong>{row.count}</strong>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="muted">Nessun acquisto disponibile.</p>
                      )}
                    </details>

                    <details className="accordion market-accordion">
                      <summary>
                        <span>Giocatori più svincolati</span>
                        <strong>{topReleased.length}</strong>
                      </summary>
                      {topReleased.length ? (
                        <div className="list market-ranking-list">
                          {topReleased.map((row, idx) => (
                            <div key={`rel-${row.name}-${idx}`} className="list-item market-ranking-item">
                              <div>
                                <p className="rank-title">
                                  <span
                                    className={`rank-badge ${
                                      idx === 0 ? "gold" : idx === 1 ? "silver" : idx === 2 ? "bronze" : ""
                                    }`}
                                  >
                                    #{idx + 1}
                                  </span>
                                  <span className="market-ranking-name">{row.name}</span>
                                </p>
                                <span className="muted">
                                  {(row.role || "-")} - {(row.squadra || "-")} · {row.teamCount} team
                                </span>
                              </div>
                              <strong>{row.count}</strong>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="muted">Nessuno svincolo disponibile.</p>
                      )}
                    </details>
                  </div>
                </>
              ) : (
                <p className="muted">Nessun team trovato.</p>
              )}
            </>
          ) : (
            <p className="muted">Nessun trasferimento disponibile.</p>
          )}
        </div>
      </div>

    </section>
  );
}
