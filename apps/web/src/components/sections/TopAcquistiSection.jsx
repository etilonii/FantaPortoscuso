export default function TopAcquistiSection({
  activeTopRole,
  setActiveTopRole,
  aggregatesLoading,
  topAcquistiQuery,
  setTopAcquistiQuery,
  topAcquistiSelected = [],
  topAcquistiSearchMatches = [],
  topAcquistiIntersection = { players: [], teams: [], count: 0 },
  onAddTopAcquistiPlayer,
  onRemoveTopAcquistiPlayer,
  onClearTopAcquistiPlayers,
  topPosFrom,
  setTopPosFrom,
  topPosTo,
  setTopPosTo,
  topAcquistiRangeLabel,
  onResetTopAcquistiFilters,
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
              <div className="top-acquisti-multi-search">
                <label className="stats-search field">
                  <input
                    className="input"
                    type="text"
                    placeholder="Cerca e aggiungi giocatore..."
                    value={topAcquistiQuery}
                    onChange={(e) => setTopAcquistiQuery(e.target.value)}
                  />
                </label>

                {topAcquistiSearchMatches.length > 0 ? (
                  <div className="top-acquisti-suggestions">
                    {topAcquistiSearchMatches.map((player) => (
                      <button
                        type="button"
                        key={`suggest-${player.name}`}
                        className="top-acquisti-suggestion"
                        onClick={() => onAddTopAcquistiPlayer(player)}
                      >
                        <span>
                          <strong>{player.name}</strong>
                          <small>
                            {player.role || "-"} | {player.squadra || "-"}
                          </small>
                        </span>
                        <em>{formatInt(player.teams?.length || 0)}</em>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              {topAcquistiSelected.length > 0 ? (
                <div className="top-acquisti-combo">
                  <div className="top-acquisti-selected-head">
                    <span className="muted">Giocatori selezionati</span>
                    <button
                      type="button"
                      className="ghost compact"
                      onClick={onClearTopAcquistiPlayers}
                    >
                      Svuota
                    </button>
                  </div>
                  <div className="top-acquisti-selected">
                    {topAcquistiSelected.map((player) => (
                      <button
                        type="button"
                        key={`selected-${player.name}`}
                        className="top-acquisti-chip"
                        onClick={() => onRemoveTopAcquistiPlayer(player.name)}
                        title="Rimuovi"
                      >
                        <span>{player.name}</span>
                        <small>{player.role || "-"}</small>
                      </button>
                    ))}
                  </div>
                  <div className="top-acquisti-intersection">
                    <div>
                      <p className="rank-title">
                        <span className="rank-badge">AND</span>
                        <span>{topAcquistiIntersection.players.join(", ")}</span>
                      </p>
                      {topAcquistiIntersection.teams.length > 0 ? (
                        <div className="team-tags">
                          {topAcquistiIntersection.teams.map((team) => (
                            <span key={`intersection-${team}`} className="team-tag">
                              {team}
                            </span>
                          ))}
                        </div>
                      ) : (
                        <span className="muted">
                          Nessuna squadra li ha tutti contemporaneamente.
                        </span>
                      )}
                    </div>
                    <strong>{formatInt(topAcquistiIntersection.count)}</strong>
                  </div>
                </div>
              ) : null}

              <div className="top-acquisti-range">
                <label className="field">
                  <span>Posizione da</span>
                  <input
                    className="input"
                    type="number"
                    min="1"
                    step="1"
                    placeholder="Es. 7"
                    value={topPosFrom}
                    onChange={(e) => setTopPosFrom(e.target.value)}
                  />
                </label>
                <label className="field">
                  <span>Posizione a</span>
                  <input
                    className="input"
                    type="number"
                    min="1"
                    step="1"
                    placeholder="Es. 30"
                    value={topPosTo}
                    onChange={(e) => setTopPosTo(e.target.value)}
                  />
                </label>
              </div>

              <div className="top-acquisti-toolbar">
                <span className="muted">{topAcquistiRangeLabel}</span>
                <button
                  type="button"
                  className="ghost"
                  onClick={onResetTopAcquistiFilters}
                >
                  Reset filtri
                </button>
              </div>

              {filteredTopAcquisti.length ? (
                filteredTopAcquisti.map((p, idx) => (
                  <div
                    key={`${p.name}-${idx}`}
                    className="list-item player-card"
                    onClick={() => onAddTopAcquistiPlayer(p)}
                  >
                    <div>
                      <p className="rank-title">
                        <span className="rank-badge">#{p.rank ?? idx + 1}</span>
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
                        Squadra: {p.squadra || "-"} Teams: {p.count}
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

