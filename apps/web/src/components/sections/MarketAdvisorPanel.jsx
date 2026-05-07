import MarketPrioritySummary from "./MarketPrioritySummary";
import MarketSuggestionCard from "./MarketSuggestionCard";

const ROLE_LABELS = {
  Por: "Portieri",
  Dif: "Difesa",
  Cen: "Centrocampo",
  Att: "Attacco",
};

const roleBadgeLabel = (role) => ROLE_LABELS[String(role || "").trim()] || String(role || "-");

const scoreToBadge = (score) => {
  const value = Number(score);
  if (!Number.isFinite(value)) return "watch";
  if (value >= 82) return "opportunity";
  if (value >= 72) return "high";
  if (value >= 60) return "medium";
  if (value >= 48) return "watch";
  return "risk";
};

const gainToBadge = (gain) => {
  const value = Number(gain);
  if (!Number.isFinite(value)) return "watch";
  if (value >= 25) return "opportunity";
  if (value >= 15) return "high";
  if (value >= 8) return "medium";
  if (value >= 4) return "low";
  return "watch";
};

const normalizeName = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const buildRoleAverages = (rows) => {
  const buckets = { Por: [], Dif: [], Cen: [], Att: [] };
  (rows || []).forEach((row) => {
    const reparto = String(row?.reparto || "").trim();
    const score = Number(row?.MarketScoreFinal);
    if (buckets[reparto] && Number.isFinite(score)) {
      buckets[reparto].push(score);
    }
  });
  return Object.fromEntries(
    Object.entries(buckets).map(([role, values]) => [
      role,
      values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null,
    ])
  );
};

export default function MarketAdvisorPanel({
  advisor,
  loading,
  error,
  activeTeam,
  formatInt,
  formatDecimal,
  marketCountdown,
  marketUpdatedAt,
  openPlayer,
}) {
  if (loading) {
    return (
      <div className="panel market-advisor-empty">
        <p className="muted">Caricamento Market Advisor in corso...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="panel market-advisor-empty">
        <p className="muted">{error}</p>
      </div>
    );
  }

  if (!advisor) {
    return (
      <div className="panel market-advisor-empty">
        <p className="muted">Dati mercato non ancora disponibili.</p>
      </div>
    );
  }

  const squadAudit = Array.isArray(advisor?.squad_audit) ? advisor.squad_audit : [];
  const bestPlans = Array.isArray(advisor?.best_plans) ? advisor.best_plans : [];
  const roleRankings = Array.isArray(advisor?.role_rankings) ? advisor.role_rankings : [];
  const roleAverages = buildRoleAverages(squadAudit);
  const roleEntries = Object.entries(roleAverages).filter(([, value]) => Number.isFinite(value));
  const weakestRole = roleEntries.length
    ? roleEntries.slice().sort((a, b) => Number(a[1]) - Number(b[1]))[0][0]
    : "";
  const strongestRole = roleEntries.length
    ? roleEntries.slice().sort((a, b) => Number(b[1]) - Number(a[1]))[0][0]
    : "";

  const priorities = [];
  const rosterReasons = Array.isArray(advisor?.roster_audit?.reasons) ? advisor.roster_audit.reasons : [];
  if (rosterReasons.length > 0) {
    priorities.push({
      title: "Vincoli rosa da rivedere",
      message: rosterReasons[0],
      label: "Alta priorita",
      className: "is-warning",
    });
  }
  if (weakestRole) {
    priorities.push({
      title: `Possibile upgrade in ${roleBadgeLabel(weakestRole)}`,
      message: `Il reparto oggi ha lo score medio piu basso della rosa.`,
      label: "Da monitorare",
      className: "is-neutral",
    });
  }
  if (strongestRole) {
    priorities.push({
      title: `Base solida in ${roleBadgeLabel(strongestRole)}`,
      message: "Non serve forzare mosse pesanti se non migliorano il saldo complessivo.",
      label: "Bassa priorita",
      className: "is-ok",
    });
  }

  const cedibili = squadAudit
    .slice()
    .sort((a, b) => Number(a?.MarketScoreFinal || 0) - Number(b?.MarketScoreFinal || 0))
    .slice(0, 4);

  const rankingMap = new Map(roleRankings.map((row) => [normalizeName(row?.name), row]));
  const targetNames = [];
  bestPlans.forEach((plan) => {
    (plan?.in_players || []).forEach((name) => {
      if (name && !targetNames.includes(name)) targetNames.push(name);
    });
  });
  const targets = targetNames
    .map((name) => rankingMap.get(normalizeName(name)) || { name })
    .slice(0, 6);

  return (
    <div className="market-advisor-shell">
      <div className="market-advisor-overview">
        <div className="summary-card">
          <span>Squadra</span>
          <strong>{activeTeam || advisor?.team || "-"}</strong>
        </div>
        <div className="summary-card">
          <span>Crediti residui</span>
          <strong>
            {Number.isFinite(Number(advisor?.credits?.residual))
              ? formatDecimal(advisor?.credits?.residual, 0)
              : "Crediti residui non disponibili"}
          </strong>
        </div>
        <div className="summary-card">
          <span>Movimenti massimi</span>
          <strong>{advisor?.window?.max_changes ?? "-"}</strong>
        </div>
        <div className="summary-card">
          <span>Mercato</span>
          <strong>{marketCountdown || "Dato non ancora disponibile"}</strong>
        </div>
      </div>

      <MarketPrioritySummary priorities={priorities} />

      <div className="market-advisor-grid">
        <div className="market-advisor-column">
          <div className="panel">
            <div className="panel-header">
              <h3>Giocatori cedibili</h3>
            </div>
            {cedibili.length === 0 ? (
              <p className="muted">Nessuna mossa consigliata al momento.</p>
            ) : (
              <div className="market-suggestion-list">
                {cedibili.map((player, index) => (
                  <MarketSuggestionCard
                    key={`${player?.name || "out"}-${index}`}
                    title={player?.name || "-"}
                    badge={scoreToBadge(player?.MarketScoreFinal)}
                    subtitle={`${player?.club || "-"} · ${roleBadgeLabel(player?.reparto)}`}
                    meta={[
                      { label: "Score", value: formatDecimal(player?.MarketScoreFinal, 1) },
                      { label: "Tier", value: player?.Tier || "-" },
                      { label: "QA", value: formatInt(player?.prezzo_attuale) },
                    ]}
                    body={
                      Number(player?.MarketScoreFinal) < 50
                        ? "Cedibile se serve credito: rendimento sintetico sotto la media della rosa."
                        : "Da monitorare: puo diventare il sacrificio giusto se serve liberare budget."
                    }
                    onClick={openPlayer ? () => openPlayer(player?.name) : null}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="panel">
            <div className="panel-header">
              <h3>Target consigliati</h3>
            </div>
            {targets.length === 0 ? (
              <p className="muted">Nessuna mossa consigliata al momento.</p>
            ) : (
              <div className="market-suggestion-list">
                {targets.map((player, index) => (
                  <MarketSuggestionCard
                    key={`${player?.name || "in"}-${index}`}
                    title={player?.name || "-"}
                    badge={scoreToBadge(player?.MarketScoreFinal)}
                    subtitle={`${player?.club || "-"} · ${roleBadgeLabel(player?.reparto)}`}
                    meta={[
                      { label: "Score", value: formatDecimal(player?.MarketScoreFinal, 1) },
                      { label: "Tier", value: player?.Tier || "-" },
                      {
                        label: "Costo",
                        value: Number.isFinite(Number(player?.prezzo_attuale))
                          ? formatInt(player?.prezzo_attuale)
                          : "-",
                      },
                    ]}
                    body={
                      weakestRole && String(player?.reparto || "") === weakestRole
                        ? `Possibile upgrade diretto per il reparto oggi piu corto della rosa.`
                        : "Profilo interessante per qualita/prezzo: da monitorare nel prossimo giro."
                    }
                    onClick={openPlayer ? () => openPlayer(player?.name) : null}
                  />
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="market-advisor-column">
          <div className="panel">
            <div className="panel-header">
              <h3>Scambi e mosse suggerite</h3>
            </div>
            {bestPlans.length === 0 ? (
              <p className="muted">Nessuna mossa consigliata al momento.</p>
            ) : (
              <div className="market-suggestion-list">
                {bestPlans.map((plan, index) => (
                  <MarketSuggestionCard
                    key={`${plan?.plan_id || "plan"}-${index}`}
                    title={`Piano ${plan?.plan_id || index + 1}`}
                    badge={gainToBadge(plan?.package_gain)}
                    subtitle={`Saldo stimato ${formatDecimal(plan?.package_gain, 1)} punti advisor`}
                    meta={[
                      { label: "OUT", value: (plan?.out_players || []).join(", ") || "-" },
                      { label: "IN", value: (plan?.in_players || []).join(", ") || "-" },
                      {
                        label: "Crediti finali",
                        value: Number.isFinite(Number(plan?.credits_residual_after))
                          ? formatDecimal(plan?.credits_residual_after, 0)
                          : "-",
                      },
                    ]}
                    body={
                      Number(plan?.package_gain) >= 15
                        ? "Pacchetto ad alto impatto: migliora il profilo medio della rosa senza forzare i vincoli."
                        : "Possibile upgrade graduale: utile se vuoi alzare il livello senza strappi."
                    }
                    footer={
                      <details className="admin-details">
                        <summary>Dettagli mossa</summary>
                        <p className="muted">{plan?.notes || "Nessuna nota tecnica disponibile."}</p>
                      </details>
                    }
                  />
                ))}
              </div>
            )}
          </div>

          <div className="panel">
            <div className="panel-header">
              <h3>Crediti residui</h3>
            </div>
            <div className="market-credit-grid">
              <div className="summary-card">
                <span>Residui attuali</span>
                <strong>
                  {Number.isFinite(Number(advisor?.credits?.residual))
                    ? formatDecimal(advisor?.credits?.residual, 0)
                    : "Crediti residui non disponibili"}
                </strong>
              </div>
              <div className="summary-card">
                <span>Fonte</span>
                <strong>{advisor?.credits?.source || "-"}</strong>
              </div>
              <div className="summary-card">
                <span>Aggiornamento</span>
                <strong>{marketUpdatedAt || "Dato non ancora disponibile"}</strong>
              </div>
            </div>
          </div>

          <details className="admin-details">
            <summary>Dettagli tecnici</summary>
            <div className="market-tech-grid">
              <div>
                <span>Player universe</span>
                <strong>{advisor?.candidate_pool?.universe_players ?? "-"}</strong>
              </div>
              <div>
                <span>Candidati IN</span>
                <strong>{advisor?.candidate_pool?.in_candidates ?? "-"}</strong>
              </div>
              <div>
                <span>Candidati OUT</span>
                <strong>{advisor?.candidate_pool?.out_candidates ?? "-"}</strong>
              </div>
              <div>
                <span>Piani valutati</span>
                <strong>{advisor?.search?.evaluated ?? "-"}</strong>
              </div>
            </div>
          </details>
        </div>
      </div>
    </div>
  );
}
