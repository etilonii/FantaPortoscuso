export default function StatsSection({
  statsTab,
  setStatsTab,
  statsQuery,
  setStatsQuery,
  filteredStatsItems,
  slugify,
  openPlayer,
  tabToColumn,
}) {
  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Statistiche Giocatori</p>
          <h2>Classifiche per statistica</h2>
        </div>
      </div>

      <div className="panel">
        <div className="role-switch">
          {[
            { key: "gol", label: "Gol" },
            { key: "assist", label: "Assist" },
            { key: "ammonizioni", label: "Ammonizioni" },
            { key: "espulsioni", label: "Espulsioni" },
            { key: "cleansheet", label: "Clean Sheet" },
          ].map((tab) => (
            <button
              key={tab.key}
              className={`role-pill ${statsTab === tab.key ? "active" : ""}`}
              onClick={() => setStatsTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <label className="stats-search field">
          <input
            className="input"
            type="text"
            placeholder="Cerca per nome..."
            value={statsQuery}
            onChange={(e) => setStatsQuery(e.target.value)}
          />
        </label>

        <div className="list">
          {filteredStatsItems.length === 0 ? (
            <p className="muted">Nessun dato disponibile.</p>
          ) : (
            filteredStatsItems.map((item, index) => {
              const itemSlug = slugify(item.Giocatore);
              const rank = item.rank ?? index + 1;
              const podioClass = rank <= 3 ? `podio-${rank}` : "";
              return (
                <div
                  key={`${item.Giocatore}-${index}`}
                  id={`stat-${statsTab}-${itemSlug}`}
                  className={`list-item player-card stats-item podio-row ${podioClass}`}
                  onClick={() => openPlayer(item.Giocatore)}
                >
                  <div>
                    <p className="rank-title">
                      <span className={`rank-badge rank-${rank}`}>
                        #{rank}
                      </span>
                      <button
                        type="button"
                        className="link-button"
                        onClick={(e) => {
                          e.stopPropagation();
                          openPlayer(item.Giocatore);
                        }}
                      >
                        {item.Giocatore}
                      </button>
                    </p>
                    <span className="muted stats-inline">
                      Ruolo: {item.Ruolo || item.Posizione || "-"} · Squadra:{" "}
                      {item.Squadra || "-"}
                    </span>
                  </div>
                  <strong>{item[tabToColumn(statsTab)] ?? item[statsTab] ?? "-"}</strong>
                </div>
              );
            })
          )}
        </div>
      </div>
    </section>
  );
}
