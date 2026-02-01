export default function TopAcquistiSection({
  activeTopRole,
  setActiveTopRole,
  aggregatesLoading,
  topAcquistiQuery,
  setTopAcquistiQuery,
  filteredTopAcquisti,
  openPlayer,
  formatInt,
}) {
  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Giocatori più acquistati</p>
          <h2>Per Ruolo</h2>
        </div>
      </div>

      <div className="panel">
        <div className="role-switch">
          {[
            { key: "P", label: "Portieri" },
            { key: "D", label: "Difensori" },
            { key: "C", label: "Centrocampisti" },
            { key: "A", label: "Attaccanti" },
          ].map((role) => (
            <button
              key={role.key}
              className={`role-pill ${activeTopRole === role.key ? "active" : ""}`}
              onClick={() => setActiveTopRole(role.key)}
            >
              {role.label}
            </button>
          ))}
        </div>

        <div className="list">
          {aggregatesLoading ? (
            <p className="muted">Caricamento...</p>
          ) : (
            <>
              <label className="stats-search field">
                <input
                  className="input"
                  type="text"
                  placeholder="Cerca giocatore..."
                  value={topAcquistiQuery}
                  onChange={(e) => setTopAcquistiQuery(e.target.value)}
                />
              </label>

              {filteredTopAcquisti.length ? (
                filteredTopAcquisti.map((p, idx) => (
                  <div
                    key={`${p.name}-${idx}`}
                    className="list-item player-card"
                    onClick={() => openPlayer(p.name)}
                  >
                    <div>
                      <p className="rank-title">
                        <span className="rank-badge">#{idx + 1}</span>
                        <button
                          type="button"
                          className="link-button"
                          onClick={(e) => {
                            e.stopPropagation();
                            openPlayer(p.name);
                          }}
                        >
                          {p.name}
                        </button>
                      </p>
                      <span className="muted">
                        Squadra: {p.squadra || "-"} · Teams: {p.count}
                      </span>
                      <div className="team-tags">
                        {(p.teams || []).map((t) => (
                          <span key={`${p.name}-${t}`} className="team-tag">
                            {t}
                          </span>
                        ))}
                      </div>
                    </div>
                    <strong>{formatInt(p.qa)}</strong>
                  </div>
                ))
              ) : (
                <p className="muted">Nessun dato disponibile.</p>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}
