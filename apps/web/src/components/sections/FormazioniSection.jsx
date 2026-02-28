export default function FormazioniSection({
  variant = "formazioni",
  formations,
  formationTeam,
  setFormationTeam,
  formationRound,
  onFormationRoundChange,
  formationOrder,
  onFormationOrderChange,
  formationMeta,
  reloadFormazioni,
  optimizerData,
  optimizerLoading,
  optimizerError,
  runOptimizer,
  openPlayer,
  formatDecimal,
  isAdmin = false,
  sessionTeam = "",
}) {
  const isConsigliata = String(variant || "").trim().toLowerCase() === "consigliata";
  const scopedTeam = String(sessionTeam || "").trim();
  const teamScoped = !isAdmin;
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

  const teamOptions = teamScoped
    ? (scopedTeam ? [scopedTeam] : [])
    : [
        "all",
        ...Array.from(
          new Set((formations || []).map((item) => String(item.team || "").trim()).filter(Boolean))
        ).sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" })),
      ];

  const effectiveTeam = teamScoped ? scopedTeam : String(formationTeam || "").trim();
  const hasEffectiveTeam = Boolean(effectiveTeam);

  const visibleItems =
    !hasEffectiveTeam || effectiveTeam.toLowerCase() === "all"
      ? formations || []
      : (formations || []).filter(
          (item) => String(item.team || "").trim() === effectiveTeam
        );

  const parseLiveTotal = (value) => {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value !== "string") return null;
    const parsed = Number(value.replace(",", "."));
    return Number.isFinite(parsed) ? parsed : null;
  };

  const orderedItems = [...visibleItems].sort((a, b) => {
    const posA = Number(a?.standing_pos ?? a?.pos ?? 9999);
    const posB = Number(b?.standing_pos ?? b?.pos ?? 9999);

    if (orderValue === "live_total") {
      const left = parseLiveTotal(a?.totale_live);
      const right = parseLiveTotal(b?.totale_live);
      if (left !== null && right !== null && left !== right) return right - left;
      if (left !== null && right === null) return -1;
      if (left === null && right !== null) return 1;
    }

    if (Number.isFinite(posA) && Number.isFinite(posB) && posA !== posB) return posA - posB;
    return String(a?.team || "").localeCompare(String(b?.team || ""), "it", {
      sensitivity: "base",
    });
  });

  const normalizePlayerKey = (value) => String(value || "").trim().toLowerCase();
  const canOptimize =
    hasEffectiveTeam && String(effectiveTeam || "").trim().toLowerCase() !== "all";
  const optimizerRoundCandidate = Number(formationMeta?.optimizerRound);
  const optimizerRoundForRun =
    Number.isFinite(optimizerRoundCandidate) && optimizerRoundCandidate > 0
      ? optimizerRoundCandidate
      : Number.isFinite(activeRound) && activeRound > 0
        ? activeRound
        : null;
  const displayRound = isConsigliata
    ? (optimizerRoundForRun || (hasRound ? activeRound : null))
    : (hasRound ? activeRound : null);
  const unavailablePlayers = Array.isArray(optimizerData?.availability?.unavailable_players)
    ? optimizerData.availability.unavailable_players
    : [];
  const injuredPlayers = unavailablePlayers.filter(
    (item) => String(item?.status || "").trim().toLowerCase() === "injured"
  );
  const suspendedPlayers = unavailablePlayers.filter(
    (item) => String(item?.status || "").trim().toLowerCase() === "suspended"
  );
  const otherUnavailablePlayers = unavailablePlayers.filter((item) => {
    const status = String(item?.status || "").trim().toLowerCase();
    return status !== "injured" && status !== "suspended";
  });

  const formatUnavailableMeta = (item) => {
    const role = String(item?.role || "").trim().toUpperCase();
    const club = String(item?.club || "").trim();
    const rounds = Array.isArray(item?.rounds)
      ? item.rounds.filter((value) => Number.isFinite(Number(value)))
      : [];
    const roundsLabel = rounds.length
      ? `G ${rounds
          .map((value) => Number(value))
          .sort((a, b) => a - b)
          .join(", ")}`
      : "";
    return [role, club, roundsLabel].filter(Boolean).join(" | ");
  };

  const renderUnavailableGroup = (title, items, toneClass) => {
    if (!items.length) return null;
    return (
      <div className={`formation-unavailability-group ${toneClass}`}>
        <p className="formation-unavailability-title">
          <span>{title}</span>
          <strong>{items.length}</strong>
        </p>
        <div className="formation-unavailability-items">
          {items.map((item, idx) => {
            const name = String(item?.name || "").trim();
            const note = String(item?.note || "").trim();
            if (!name) return null;
            return (
              <button
                key={`${title}-${name}-${idx}`}
                type="button"
                className="formation-unavailability-item"
                onClick={() => openPlayer(name)}
                title={note || String(item?.reason || "").trim()}
              >
                <span className="formation-unavailability-name">{name}</span>
                <span className="formation-unavailability-meta">{formatUnavailableMeta(item)}</span>
              </button>
            );
          })}
        </div>
      </div>
    );
  };

  const toPlayerEntry = (entry) => {
    if (typeof entry === "string") {
      return { name: entry.trim(), role: "" };
    }
    if (entry && typeof entry === "object") {
      return {
        name: String(entry.name || "").trim(),
        role: String(entry.role || "").trim().toUpperCase(),
      };
    }
    return { name: "", role: "" };
  };

  const renderPlayerPills = (players, scoreMap, options = {}) => {
    const showRole = Boolean(options.showRole);
    const captainKey = normalizePlayerKey(options.captainName);
    const viceCaptainKey = normalizePlayerKey(options.viceCaptainName);
    const normalizedPlayers = (players || [])
      .map((entry) => toPlayerEntry(entry))
      .filter((entry) => entry.name);
    if (!normalizedPlayers.length) return <span className="muted">-</span>;

    return (
      <div className="formation-pills">
        {normalizedPlayers.map((player, idx) => {
          const name = player.name;
          const playerKey = normalizePlayerKey(name);
          const isCaptain = Boolean(captainKey) && playerKey === captainKey;
          const isViceCaptain = Boolean(viceCaptainKey) && playerKey === viceCaptainKey;
          const score = scoreMap?.[name] || null;
          const voteLabel = score?.vote_label || "-";
          const fantavoteLabel = score?.fantavote_label || "-";
          const scoreClass = score?.is_sv ? "formation-pill-metrics sv" : "formation-pill-metrics";
          return (
            <button
              key={`${name}-${idx}`}
              type="button"
              className="formation-pill"
              onClick={() => openPlayer(name)}
            >
              {showRole && player.role ? (
                <span className="formation-pill-role">{player.role}</span>
              ) : null}
              <span className="formation-pill-name-wrap">
                <span className="formation-pill-name">{name}</span>
                {isCaptain ? <span className="formation-pill-badge captain">C</span> : null}
                {isViceCaptain ? <span className="formation-pill-badge vice">VC</span> : null}
              </span>
              <span className={scoreClass}>
                V {voteLabel} | FV {fantavoteLabel}
              </span>
            </button>
          );
        })}
      </div>
    );
  };

  const renderOptimizerPills = (players) => {
    const normalized = Array.isArray(players) ? players : [];
    if (!normalized.length) return <span className="muted">-</span>;
    const probableBucketLabel = (bucket) => {
      const key = String(bucket || "").trim().toLowerCase();
      if (key === "titolare") return "Titolare";
      if (key === "ballottaggio") return "Ballottaggio";
      if (key === "panchina") return "Panchina";
      return "Non disp.";
    };
    const probableBucketClass = (bucket, recommended) => {
      const key = String(bucket || "").trim().toLowerCase();
      const base =
        key === "titolare"
          ? "titolare"
          : key === "ballottaggio"
            ? "ballottaggio"
            : key === "panchina"
              ? "panchina"
              : "unknown";
      return `${base}${recommended ? "" : " off"}`;
    };

    return (
      <div className="formation-pills">
        {normalized.map((entry, idx) => {
          const name = String(entry?.name || "").trim();
          if (!name) return null;
          const role = String(entry?.role || "").trim().toUpperCase();
          const adjusted = Number(entry?.adjusted_force);
          const base = Number(entry?.base_force);
          const factor = Number(entry?.fixture_factor);
          const opponent = String(entry?.fixture_opponent || "").trim();
          const homeAway = String(entry?.fixture_home_away || "").trim().toUpperCase();
          const venueLabel =
            homeAway === "H" ? "Casa" : homeAway === "A" ? "Trasferta" : "Match non disp.";
          const probableBucket = String(entry?.probable_bucket || "").trim().toLowerCase();
          const probablePercentage = Number(entry?.probable_percentage);
          const probableRecommended = Boolean(entry?.probable_recommended);
          const probableLabel = probableBucketLabel(probableBucket);
          const opponentLabel = opponent ? `${venueLabel} vs ${opponent}` : venueLabel;
          const factorLabel = Number.isFinite(factor) ? `x${factor.toFixed(3)}` : "x1.000";
          return (
            <button
              key={`${name}-${idx}`}
              type="button"
              className="formation-pill formation-pill-optimizer"
              onClick={() => openPlayer(name)}
            >
              <span className="formation-pill-topline">
                {role ? <span className="formation-pill-role">{role}</span> : null}
                <span className="formation-pill-name">{name}</span>
                <span
                  className={`formation-pill-probable ${probableBucketClass(
                    probableBucket,
                    probableRecommended
                  )}`}
                >
                  {probableLabel}
                  {Number.isFinite(probablePercentage)
                    ? ` ${formatDecimal(probablePercentage, 0)}%`
                    : ""}
                </span>
              </span>
              <span className="formation-pill-metrics">
                Valore Base: {Number.isFinite(base) ? formatDecimal(base, 2) : "-"}
              </span>
              <span className="formation-pill-metrics">
                Fattore Partita: {factorLabel}
              </span>
              <span className="formation-pill-metrics">
                Valore Finale: {Number.isFinite(adjusted) ? formatDecimal(adjusted, 2) : "-"}
              </span>
              <span className="formation-pill-metrics">
                {opponentLabel}
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
          <p className="eyebrow">{isConsigliata ? "Formazione consigliata" : "Formazioni"}</p>
          <h2>{displayRound ? `Giornata ${displayRound}` : "Giornata corrente"}</h2>
          <p className="muted">
            {isConsigliata
              ? "XI ottimizzata per il team selezionato."
              : isRealSource
                ? `Formazioni reali ordinate per ${orderLabel}.`
                : `Fallback: miglior XI ordinato per ${orderLabel}.`}
          </p>
        </div>
      </div>

      <div className="panel">
        <div className="filters inline centered">
          {!teamScoped ? (
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
          ) : (
            <label className="field">
              <span>Squadra</span>
              <select className="select" value={scopedTeam || ""} disabled>
                {scopedTeam ? (
                  <option value={scopedTeam}>{scopedTeam}</option>
                ) : (
                  <option value="">Non associata</option>
                )}
              </select>
            </label>
          )}
          {!isConsigliata && availableRounds.length > 0 && (
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
          {!isConsigliata && orderAllowed.length > 1 ? (
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
          {!isConsigliata ? (
            <button type="button" className="ghost" onClick={() => reloadFormazioni()}>
              Corrente
            </button>
          ) : null}
          {isConsigliata && canOptimize ? (
            <button
              type="button"
              className="primary"
              onClick={() => runOptimizer(effectiveTeam, optimizerRoundForRun || null)}
              disabled={optimizerLoading}
            >
              {optimizerLoading ? "Calcolo..." : "XI ottimizzata"}
            </button>
          ) : null}
        </div>
        {!isConsigliata && formationMeta?.note ? <p className="muted compact">{formationMeta.note}</p> : null}
        {isConsigliata && optimizerError ? <p className="error">{optimizerError}</p> : null}

        {isConsigliata && canOptimize && optimizerData ? (
          <article className="formation-card formation-optimizer-card">
            <header className="formation-card-head">
              <p className="rank-title">
                <span className="rank-badge rank-1">XI</span>
                <span>
                  {optimizerData.team} | Giornata {optimizerData.round}
                </span>
              </p>
              <div className="formation-meta">
                <span className="muted">Modulo {optimizerData.module || "-"}</span>
                <strong>
                  Indice XI {formatDecimal(optimizerData?.totals?.adjusted_force || 0, 2)}
                </strong>
                <span className="formation-live-total">
                  Valore XI {formatDecimal(optimizerData?.totals?.base_force || 0, 2)}
                </span>
              </div>
            </header>
            <p className="muted compact">
              Calcolo su forza giocatore locale + coefficiente partita (casa/trasferta + forza
              avversario) + probabili formazioni.
            </p>
            {optimizerData?.probable_formations ? (
              <p className="muted compact formation-probable-summary">
                Probabili: G
                {Number.isFinite(Number(optimizerData?.probable_formations?.round))
                  ? Number(optimizerData.probable_formations.round)
                  : "-"}
                {optimizerData?.probable_formations?.last_update_label
                  ? ` | Agg. ${optimizerData.probable_formations.last_update_label}`
                  : ""}
              </p>
            ) : null}
            {unavailablePlayers.length > 0 ? (
              <div className="formation-unavailability">
                <p className="muted compact">
                  Indisponibili esclusi dall&apos;XI ({unavailablePlayers.length})
                </p>
                <div className="formation-unavailability-grid">
                  {renderUnavailableGroup("Infortunati", injuredPlayers, "injured")}
                  {renderUnavailableGroup("Squalificati", suspendedPlayers, "suspended")}
                  {renderUnavailableGroup("Altri", otherUnavailablePlayers, "other")}
                </div>
              </div>
            ) : null}
            <div className="formation-lines">
              <div className="formation-line">
                <span className="formation-label">P</span>
                {renderOptimizerPills(optimizerData?.lineup?.portiere_details || [])}
              </div>
              <div className="formation-line">
                <span className="formation-label">D</span>
                {renderOptimizerPills(optimizerData?.lineup?.difensori_details || [])}
              </div>
              <div className="formation-line">
                <span className="formation-label">C</span>
                {renderOptimizerPills(optimizerData?.lineup?.centrocampisti_details || [])}
              </div>
              <div className="formation-line">
                <span className="formation-label">A</span>
                {renderOptimizerPills(optimizerData?.lineup?.attaccanti_details || [])}
              </div>
              <div className="formation-line formation-line-bench">
                <span className="formation-label bench">B</span>
                {renderOptimizerPills(optimizerData?.lineup?.panchina_details || [])}
              </div>
            </div>
            <div className="formation-live-breakdown">
              <span>Capitano {optimizerData?.captain || "-"}</span>
              <span>Vice {optimizerData?.vice_captain || "-"}</span>
            </div>
          </article>
        ) : null}

        {isConsigliata ? (
          !canOptimize ? (
            <p className="muted">Team associato alla key non disponibile.</p>
          ) : !optimizerData && !optimizerLoading ? (
            <p className="muted">Dati formazione consigliata non disponibili.</p>
          ) : null
        ) : orderedItems.length === 0 ? (
          <p className="muted">Nessuna formazione disponibile.</p>
        ) : (
          <div className="formations-grid">
            {orderedItems.map((item, index) => {
              const standingPos = Number.isFinite(Number(item.standing_pos))
                ? Number(item.standing_pos)
                : Number.isFinite(Number(item.pos))
                  ? Number(item.pos)
                  : index + 1;
              const rankNumber = orderValue === "live_total" ? index + 1 : standingPos;
              const rankClass =
                rankNumber === 1 || rankNumber === 2 || rankNumber === 3
                  ? `rank-${rankNumber}`
                  : "";
              const benchDetails = Array.isArray(item.panchina_details)
                ? item.panchina_details
                : Array.isArray(item.panchina)
                  ? item.panchina
                  : [];
              const playerTagOptions = {
                captainName: item.capitano,
                viceCaptainName: item.vice_capitano,
              };
              return (
              <article key={`${item.team}-${index}`} className="formation-card">
                <header className="formation-card-head">
                  <p className="rank-title">
                    <span className={`rank-badge ${rankClass}`.trim()}>
                      #{rankNumber}
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
                    {renderPlayerPills(
                      item.portiere ? [item.portiere] : [],
                      item.player_scores,
                      playerTagOptions
                    )}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">D</span>
                    {renderPlayerPills(item.difensori, item.player_scores, playerTagOptions)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">C</span>
                    {renderPlayerPills(item.centrocampisti, item.player_scores, playerTagOptions)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">A</span>
                    {renderPlayerPills(item.attaccanti, item.player_scores, playerTagOptions)}
                  </div>
                  <div className="formation-line formation-line-bench">
                    <span className="formation-label bench">B</span>
                    {renderPlayerPills(benchDetails, item.player_scores, {
                      showRole: true,
                      ...playerTagOptions,
                    })}
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
