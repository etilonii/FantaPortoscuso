import { useEffect, useMemo, useState } from "react";

export default function MercatoSection({
  marketUpdatedAt,
  marketCountdown,
  marketItems,
  formatInt,
}) {
  const [teamSearch, setTeamSearch] = useState("");
  const [showAllTeams, setShowAllTeams] = useState(false);
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

  const orderedTeams = useMemo(() => {
    return [...marketTeamsByName].sort((a, b) => {
      const aDate = a.lastDate || "";
      const bDate = b.lastDate || "";
      if (aDate !== bDate) return bDate.localeCompare(aDate);
      return (b.lastIndex ?? 0) - (a.lastIndex ?? 0);
    });
  }, [marketTeamsByName]);

  const filteredTeams = useMemo(() => {
    const query = teamSearch.trim().toLowerCase();
    if (!query) return orderedTeams;
    return orderedTeams.filter((team) => team.team.toLowerCase().includes(query));
  }, [orderedTeams, teamSearch]);

  const visibleTeams = useMemo(() => {
    if (!teamSearch.trim() && !showAllTeams) {
      return filteredTeams.slice(0, 5);
    }
    return filteredTeams;
  }, [filteredTeams, showAllTeams, teamSearch]);

  const activeTeamEntry = useMemo(() => {
    if (!visibleTeams.length) return null;
    return (
      visibleTeams.find((team) => team.team === activeTeam) ||
      visibleTeams[0] ||
      null
    );
  }, [visibleTeams, activeTeam]);

  useEffect(() => {
    if (!visibleTeams.length) {
      if (activeTeam) setActiveTeam("");
      return;
    }
    const exists = visibleTeams.some((team) => team.team === activeTeam);
    if (!exists) {
      setActiveTeam(visibleTeams[0].team);
    }
  }, [visibleTeams, activeTeam]);

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
              <div className="market-team-controls">
                <input
                  className="input market-team-search"
                  placeholder="Cerca team..."
                  value={teamSearch}
                  onChange={(e) => setTeamSearch(e.target.value)}
                />
                {filteredTeams.length > 5 && !teamSearch.trim() ? (
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => setShowAllTeams((prev) => !prev)}
                  >
                    {showAllTeams ? "Mostra meno team" : "Mostra piu team"}
                  </button>
                ) : null}
              </div>

              {visibleTeams.length ? (
                <>
                  <div className="market-team-list">
                    {visibleTeams.map((team) => (
                      <button
                        key={team.team}
                        type="button"
                        className={`market-team-card ${
                          activeTeamEntry?.team === team.team ? "active" : ""
                        }`}
                        onClick={() => setActiveTeam(team.team)}
                      >
                        <span className="market-team-title">{team.team}</span>
                        <span className="market-team-count">{team.count} cambi</span>
                      </button>
                    ))}
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
                              <span className="team-name">{activeTeamEntry.team}</span>
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
