const formatScore = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return numeric.toFixed(2).replace(".", ",");
};

const positionTrendMeta = (value) => {
  switch (String(value || "").trim().toLowerCase()) {
    case "up":
      return { label: "In salita", className: "up" };
    case "down":
      return { label: "In calo", className: "down" };
    case "stable":
      return { label: "Stabile", className: "stable" };
    default:
      return { label: "Non disponibile", className: "unknown" };
  }
};

export default function TeamTrendCard({ trend }) {
  const available = Boolean(trend?.available);
  const loading = Boolean(trend?.loading);
  const lastFive = Array.isArray(trend?.last_5) ? trend.last_5 : [];
  const rounds = Array.isArray(trend?.rounds) ? trend.rounds : [];
  const items = lastFive.length ? lastFive : rounds.slice(-5);
  const bestRound = trend?.best_round;
  const worstRound = trend?.worst_round;
  const averageLastFive = trend?.average_last_5;
  const positionTrend = positionTrendMeta(trend?.position_trend);
  const scores = items
    .map((item) => Number(item?.score))
    .filter((value) => Number.isFinite(value));
  const maxScore = scores.length ? Math.max(...scores) : 0;

  return (
    <div className="panel team-trend-card">
      <div className="panel-header">
        <div>
          <h3>Andamento</h3>
          <p className="muted team-trend-copy">
            {loading
              ? "Caricamento andamento squadra..."
              : String(trend?.message || "Andamento non ancora disponibile.")}
          </p>
        </div>
        <span className={`status-badge ${positionTrend.className === "up" ? "is-ok" : positionTrend.className === "down" ? "is-warning" : "is-neutral"}`}>
          {positionTrend.label}
        </span>
      </div>

      {!available ? (
        <p className="muted">
          {loading ? "Caricamento andamento squadra..." : "Andamento non ancora disponibile."}
        </p>
      ) : (
        <>
          <div className="team-trend-kpis">
            <div className="team-trend-kpi">
              <span>Media ultime 5</span>
              <strong>{formatScore(averageLastFive)}</strong>
            </div>
            <div className="team-trend-kpi">
              <span>Migliore</span>
              <strong>
                {bestRound ? `G${bestRound.round} · ${formatScore(bestRound.score)}` : "-"}
              </strong>
            </div>
            <div className="team-trend-kpi">
              <span>Peggiore</span>
              <strong>
                {worstRound ? `G${worstRound.round} · ${formatScore(worstRound.score)}` : "-"}
              </strong>
            </div>
            <div className="team-trend-kpi">
              <span>Storico utile</span>
              <strong>{rounds.length || 0} giornate</strong>
            </div>
          </div>

          <div className="team-trend-bars" aria-label="Ultime giornate squadra">
            {items.map((item, index) => {
              const score = Number(item?.score);
              const height = Number.isFinite(score) && maxScore > 0 ? Math.max(24, Math.round((score / maxScore) * 100)) : 24;
              const position = Number(item?.position);
              return (
                <div key={`${item?.round || "round"}-${index}`} className="team-trend-bar-card">
                  <div className="team-trend-bar-track">
                    <div className="team-trend-bar-fill" style={{ height: `${height}%` }} />
                  </div>
                  <strong>{formatScore(score)}</strong>
                  <span>G{item?.round || "-"}</span>
                  <small>{Number.isFinite(position) ? `Pos ${position}` : "Pos n/d"}</small>
                </div>
              );
            })}
          </div>

          <div className="team-trend-list">
            {items.map((item, index) => (
              <div key={`${item?.round || "r"}-${index}`} className="team-trend-item">
                <span>Giornata {item?.round || "-"}</span>
                <strong>{formatScore(item?.score)}</strong>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
