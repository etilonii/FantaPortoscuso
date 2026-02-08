export default function HomeSection({
  summary,
  dataStatus,
  formatDataStatusDate,
  activeTab,
  setActiveTab,
  query,
  setQuery,
  hasSearched,
  aggregatedRoseResults,
  expandedRose,
  setExpandedRose,
  quoteSearchResults,
  formatInt,
  openPlayer,
  topTab,
  setTopTab,
  topQuotes,
  topPlusvalenze,
  topStats,
  statsTab,
  setStatsTab,
  statColumn,
  goToTeam,
  setActiveMenu,
}) {
  const stepLabels = {
    rose: "Rose/Quotazioni",
    stats: "Statistiche",
    strength: "Forza squadra",
  };

  const formatStepStatus = (value) => {
    const v = String(value || "").toLowerCase();
    if (v === "ok") return "OK";
    if (v === "error") return "Errore";
    if (v === "running") return "In corso";
    if (v === "pending") return "In attesa";
    return "-";
  };

  const stepBadgeClass = (value) => {
    const v = String(value || "").toLowerCase();
    if (v === "ok") return "status-badge ok";
    if (v === "running") return "status-badge running";
    if (v === "error") return "status-badge error";
    return "status-badge";
  };

  const hasSteps =
    dataStatus?.steps &&
    typeof dataStatus.steps === "object" &&
    Object.keys(dataStatus.steps).length > 0;

  const statusValue = String(dataStatus?.result || "").toLowerCase();
  const statusClass =
    statusValue === "ok"
      ? "ok"
      : statusValue === "running"
      ? "running"
      : "error";
  const statusLabel =
    statusValue === "ok"
      ? "OK"
      : statusValue === "running"
      ? "In corso"
      : statusValue === "error"
      ? "Errore"
      : "Sconosciuto";
  const hasMatchday =
    dataStatus?.matchday !== null &&
    dataStatus?.matchday !== undefined &&
    Number.isFinite(Number(dataStatus.matchday));

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Home</p>
          <h2>Panoramica Lega</h2>
        </div>

        <div className="summary">
          <button
            type="button"
            className="summary-card clickable"
            onClick={() => setActiveMenu("rose")}
          >
            <span>Squadre</span>
            <strong>{summary.teams}</strong>
          </button>

          <button
            type="button"
            className="summary-card clickable"
            onClick={() => setActiveMenu("listone")}
          >
            <span>Giocatori</span>
            <strong>{summary.players}</strong>
          </button>
        </div>
      </div>

      <div className="panel data-status-panel">
        <div className="panel-header">
          <h3>Stato Dati</h3>
          <span className={`status-badge ${statusClass}`}>
            {statusLabel}
          </span>
        </div>
        <div className="data-status-meta">
          <span className="muted">
            Ultimo aggiornamento: {formatDataStatusDate(dataStatus?.last_update)}
          </span>
          {dataStatus?.season ? (
            <span className="muted">Stagione: {dataStatus.season}</span>
          ) : null}
          {hasMatchday ? (
            <span className="muted">Giornata: {dataStatus.matchday}</span>
          ) : null}
          {dataStatus?.update_id ? (
            <span className="muted">Update ID: {dataStatus.update_id}</span>
          ) : null}
        </div>
        <p className="data-status-message">{dataStatus?.message || "-"}</p>
        {hasSteps ? (
          <div className="list">
            {["rose", "stats", "strength"].map((key) => {
              const value = dataStatus?.steps?.[key];
              if (!value) return null;
              return (
                <div key={key} className="list-item">
                  <span>{stepLabels[key]}</span>
                  <span className={stepBadgeClass(value)}>
                    {formatStepStatus(value)}
                  </span>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>

      <div className="panel">
        <div className="panel-header">
          <h3>Ricerca rapida</h3>
          <div className="tabs">
            {["Rose", "Quotazioni"].map((tab) => (
              <button
                key={tab}
                className={activeTab === tab.toLowerCase() ? "tab active" : "tab"}
                onClick={() => setActiveTab(tab.toLowerCase())}
              >
                {tab}
              </button>
            ))}
          </div>
        </div>

        <p className="muted search-hint">
          {activeTab === "rose"
            ? "Cerca quali Team hanno un giocatore"
            : "Cerca per Quotazione Attuale"}
        </p>
        <div className="search-box">
          <input
            placeholder="Scrivi il nome del giocatore..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>

        <div className="search-grid">
          {activeTab === "rose" ? (
            !hasSearched ? null : aggregatedRoseResults.length === 0 ? (
              <p className="muted">Nessun risultato disponibile.</p>
            ) : (
              aggregatedRoseResults.map((item) => (
                <article
                  key={item.name}
                  className="search-card rose-card"
                  onClick={() => openPlayer(item.name)}
                >
                  <div>
                    <p className="player-name">{item.name}</p>
                    <div className="team-pills">
                      {(expandedRose.has(item.name) ? item.teamsAll : item.teamsList).map(
                        (team) => (
                          <span key={team} className="team-pill">
                            {team}
                          </span>
                        )
                      )}
                    </div>
                    {item.hasMore ? (
                      <button
                        type="button"
                        className="ghost expand-button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setExpandedRose((prev) => {
                            const next = new Set(prev);
                            if (next.has(item.name)) {
                              next.delete(item.name);
                            } else {
                              next.add(item.name);
                            }
                            return next;
                          });
                        }}
                      >
                        {expandedRose.has(item.name) ? "Riduci" : "Espandi"}
                      </button>
                    ) : null}
                  </div>
                  <strong className="team-count">{item.teamCount}</strong>
                </article>
              ))
            )
          ) : !hasSearched ? null : quoteSearchResults.length === 0 ? (
            <p className="muted">Nessun dato disponibile.</p>
          ) : (
            quoteSearchResults.map((item) => (
              <article
                key={`${item.name}-${item.team}`}
                className="search-card quote-card"
                onClick={() => openPlayer(item.name)}
              >
                <div className="quote-row">
                  <div>
                    <p className="player-name">
                      {item.name || "-"}
                      {item.role ? ` - ${item.role}` : ""}
                    </p>
                    <p className="muted">{item.team || "Squadra non disponibile"}</p>
                  </div>
                  <div className="quote-price">
                    Quotazione attuale: {formatInt(item.price)}
                  </div>
                </div>
              </article>
            ))
          )}
        </div>
      </div>

      <div className="panel">
        <section className="top-shell" aria-labelledby="top-title">
          <div className="top-shell-header">
            <div>
              <p className="eyebrow">Classifiche</p>
            </div>
            <div className="top-tabs" role="tablist" aria-label="Top tabs">
              {[
                { key: "quotazioni", label: "Top Quotazioni" },
                { key: "plusvalenze", label: "Top Plusvalenze" },
                { key: "statistiche", label: "Top Statistiche" },
              ].map((tab) => (
                <button
                  key={tab.key}
                  type="button"
                  role="tab"
                  aria-selected={topTab === tab.key}
                  aria-controls={`top-panel-${tab.key}`}
                  className={`top-tab ${topTab === tab.key ? "active" : ""}`}
                  onClick={() => setTopTab(tab.key)}
                >
                  {tab.label}
                </button>
              ))}
            </div>
          </div>

          <div className="top-content">
            <article
              id="top-panel-quotazioni"
              role="tabpanel"
              className={`top-panel ${topTab === "quotazioni" ? "active" : ""}`}
            >
              {topQuotes.length === 0 ? (
                <p className="muted">Nessuna quotazione disponibile.</p>
              ) : (
                topQuotes.map((item, index) => (
                  <div
                    key={`${item.Giocatore}-${index}`}
                    className={`top-row top-quote-row podio-row podio-${index + 1}`}
                    onClick={() => openPlayer(item.Giocatore)}
                  >
                    <div className="top-main">
                      <span className={`rank-badge rank-${index + 1}`}>
                        #{index + 1}
                      </span>
                      <div className="top-text">
                        <span className="top-title">{item.Giocatore}</span>
                        <span className="muted top-subtitle">
                          Ruolo: {item.Ruolo || "-"} · Squadra: {item.Squadra || "-"}
                        </span>
                      </div>
                    </div>
                    <strong className="top-metric">
                      {formatInt(item.PrezzoAttuale || item.QuotazioneAttuale)}
                    </strong>
                  </div>
                ))
              )}
            </article>

            <article
              id="top-panel-plusvalenze"
              role="tabpanel"
              className={`top-panel ${topTab === "plusvalenze" ? "active" : ""}`}
            >
              {topPlusvalenze.length === 0 ? (
                <p className="muted">Nessun dato disponibile.</p>
              ) : (
                topPlusvalenze.map((item, index) => (
                  <div
                    key={`${item.team}-${index}`}
                    className={`top-row podio-row podio-${index + 1}`}
                    onClick={() => goToTeam(item.team)}
                  >
                    <div className="top-main">
                      <span className={`rank-badge rank-${index + 1}`}>
                        #{index + 1}
                      </span>
                      <div className="top-text">
                        <span className="top-title">{item.team}</span>
                        <span className="muted top-subtitle">
                          Acquisto: {formatInt(item.acquisto)} Attuale:{" "}
                          {formatInt(item.attuale)} ({item.percentuale}%)
                        </span>
                      </div>
                    </div>
                    <strong className="top-metric">{formatInt(item.plusvalenza)}</strong>
                  </div>
                ))
              )}
            </article>

            <article
              id="top-panel-statistiche"
              role="tabpanel"
              className={`top-panel ${topTab === "statistiche" ? "active" : ""}`}
            >
              <div className="top-stat-switch">
                {[
                  { key: "gol", label: "Gol" },
                  { key: "assist", label: "Assist" },
                  { key: "ammonizioni", label: "Ammonizioni" },
                  { key: "espulsioni", label: "Espulsioni" },
                  { key: "cleansheet", label: "Clean Sheet" },
                ].map((tab) => (
                  <button
                    key={tab.key}
                    type="button"
                    className={`top-stat-btn ${statsTab === tab.key ? "active" : ""}`}
                    onClick={() => setStatsTab(tab.key)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
              {topStats.length === 0 ? (
                <p className="muted">Nessun dato disponibile.</p>
              ) : (
                topStats.map((item, index) => (
                  <div
                    key={`${item.Giocatore}-${index}`}
                    className={`top-row podio-row podio-${index + 1}`}
                    onClick={() => openPlayer(item.Giocatore)}
                  >
                    <div className="top-main">
                      <span className={`rank-badge rank-${index + 1}`}>
                        #{index + 1}
                      </span>
                      <div className="top-text">
                        <span className="top-title">{item.Giocatore}</span>
                        <span className="muted top-subtitle">
                          Ruolo: {item.Ruolo || item.Posizione || "-"} · Squadra:{" "}
                          {item.Squadra || "-"}
                        </span>
                      </div>
                    </div>
                    <strong className="top-metric">
                      {statColumn}:{" "}
                      {formatInt(
                        item?.[statColumn] ?? item?.[statColumn?.toLowerCase?.()] ?? "-"
                      )}
                    </strong>
                  </div>
                ))
              )}
            </article>
          </div>

          <nav className="top-cta" aria-label="Vai alle classifiche">
            {topTab === "quotazioni" && (
              <button
                type="button"
                className="ghost cta-button"
                onClick={() => {
                  setActiveMenu("listone");
                  setActiveTab("quotazioni");
                }}
              >
                Vai a Quotazioni
              </button>
            )}
            {topTab === "plusvalenze" && (
              <button
                type="button"
                className="ghost cta-button"
                onClick={() => setActiveMenu("plusvalenze")}
              >
                Vai a Plusvalenze
              </button>
            )}
            {topTab === "statistiche" && (
              <button
                type="button"
                className="ghost cta-button"
                onClick={() => setActiveMenu("stats")}
              >
                Vai a Statistiche
              </button>
            )}
          </nav>
        </section>
      </div>
    </section>
  );
}
