export default function FormazioniSection({
  formations,
  formationTeam,
  setFormationTeam,
  formationRound,
  onFormationRoundChange,
  formationOrder,
  onFormationOrderChange,
  formationMeta,
  reloadFormazioni,
  openPlayer,
  formatDecimal,
}) {
  const source = String(formationMeta?.source || "projection").toLowerCase();
  const isRealSource = source === "real";
  const availableRounds = Array.from(
    new Set(
      (formationMeta?.availableRounds || [])
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value))
    )
  ).sort((a, b) => a - b);
  const activeRound = Number(formationMeta?.round);
  const hasRound = Number.isFinite(activeRound);
  const orderAllowed = Array.isArray(formationMeta?.orderAllowed)
    ? formationMeta.orderAllowed
        .map((value) => String(value || "").trim().toLowerCase())
        .filter((value) => value === "classifica" || value === "live_total")
    : ["classifica", "live_total"];
  const orderValue =
    String(formationOrder || "").toLowerCase() === "live_total" ? "live_total" : "classifica";
  const orderLabel =
    orderValue === "live_total" ? "classifica live giornata" : "classifica campionato";

  const teamOptions = [
    "all",
    ...Array.from(
      new Set((formations || []).map((item) => String(item.team || "").trim()).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" })),
  ];

  const visibleItems =
    formationTeam === "all"
      ? formations || []
      : (formations || []).filter(
          (item) => String(item.team || "").trim() === String(formationTeam || "").trim()
        );

  const renderPlayerPills = (players, scoreMap) => {
    if (!players || !players.length) return <span className="muted">-</span>;
    return (
      <div className="formation-pills">
        {players.map((name) => {
          const score = scoreMap?.[name] || null;
          const voteLabel = score?.vote_label || "-";
          const fantavoteLabel = score?.fantavote_label || "-";
          const scoreClass = score?.is_sv ? "formation-pill-metrics sv" : "formation-pill-metrics";
          return (
            <button
              key={name}
              type="button"
              className="formation-pill"
              onClick={() => openPlayer(name)}
            >
              <span className="formation-pill-name">{name}</span>
              <span className={scoreClass}>
                V {voteLabel} | FV {fantavoteLabel}
              </span>
            </button>
          );
        })}
      </div>
    );
  };

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Formazioni</p>
          <h2>{hasRound ? `Giornata ${activeRound}` : "Giornata corrente"}</h2>
          <p className="muted">
            {isRealSource
              ? `Formazioni reali ordinate per ${orderLabel}.`
              : `Fallback: miglior XI ordinato per ${orderLabel}.`}
          </p>
        </div>
      </div>

      <div className="panel">
        <div className="filters inline centered">
          <label className="field">
            <span>Squadra</span>
            <select
              className="select"
              value={formationTeam}
              onChange={(e) => setFormationTeam(e.target.value)}
            >
              <option value="all">Tutte</option>
              {teamOptions
                .filter((value) => value !== "all")
                .map((team) => (
                  <option key={team} value={team}>
                    {team}
                  </option>
              ))}
            </select>
          </label>
          {availableRounds.length > 0 && (
            <label className="field">
              <span>Giornata</span>
              <select
                className="select"
                value={formationRound || ""}
                onChange={(e) => onFormationRoundChange(e.target.value)}
              >
                {availableRounds.map((roundValue) => (
                  <option key={roundValue} value={String(roundValue)}>
                    {roundValue}
                  </option>
                ))}
              </select>
            </label>
          )}
          {orderAllowed.length > 1 ? (
            <label className="field">
              <span>Ordine</span>
              <select
                className="select"
                value={orderValue}
                onChange={(e) => onFormationOrderChange(e.target.value)}
              >
                {orderAllowed.includes("classifica") ? (
                  <option value="classifica">Classifica campionato</option>
                ) : null}
                {orderAllowed.includes("live_total") ? (
                  <option value="live_total">Classifica live giornata</option>
                ) : null}
              </select>
            </label>
          ) : null}
          <button type="button" className="ghost" onClick={() => reloadFormazioni()}>
            Corrente
          </button>
        </div>
        {formationMeta?.note ? <p className="muted compact">{formationMeta.note}</p> : null}

        {visibleItems.length === 0 ? (
          <p className="muted">Nessuna formazione disponibile.</p>
        ) : (
          <div className="formations-grid">
            {visibleItems.map((item, index) => {
              const standingPos = Number.isFinite(Number(item.standing_pos))
                ? Number(item.standing_pos)
                : Number.isFinite(Number(item.pos))
                  ? Number(item.pos)
                  : index + 1;
              const rankClass =
                standingPos === 1 || standingPos === 2 || standingPos === 3
                  ? `rank-${standingPos}`
                  : "";
              return (
              <article key={`${item.team}-${index}`} className="formation-card">
                <header className="formation-card-head">
                  <p className="rank-title">
                    <span className={`rank-badge ${rankClass}`.trim()}>
                      #{standingPos}
                    </span>
                    <span>{item.team}</span>
                  </p>
                  <div className="formation-meta">
                    <span className="muted">Modulo {item.modulo || "-"}</span>
                    {Number.isFinite(Number(item.forza_titolari)) ? (
                      <strong>{formatDecimal(item.forza_titolari, 2)}</strong>
                    ) : null}
                    <span className="formation-live-total">
                      Live{" "}
                      {Number.isFinite(Number(item.totale_live))
                        ? formatDecimal(item.totale_live, 2)
                        : "-"}
                    </span>
                  </div>
                </header>

                <div className="formation-lines">
                  <div className="formation-line">
                    <span className="formation-label">P</span>
                    {renderPlayerPills(item.portiere ? [item.portiere] : [], item.player_scores)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">D</span>
                    {renderPlayerPills(item.difensori, item.player_scores)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">C</span>
                    {renderPlayerPills(item.centrocampisti, item.player_scores)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">A</span>
                    {renderPlayerPills(item.attaccanti, item.player_scores)}
                  </div>
                </div>
                <div className="formation-live-breakdown">
                  <span>
                    Base{" "}
                    {Number.isFinite(Number(item.totale_live_base))
                      ? formatDecimal(item.totale_live_base, 2)
                      : "-"}
                  </span>
                  <span>Mod D {formatDecimal(item.mod_difesa || 0, 2)}</span>
                  <span>Mod C {formatDecimal(item.mod_capitano || 0, 2)}</span>
                </div>
              </article>
              );
            })}
          </div>
        )}
      </div>
    </section>
  );
}
