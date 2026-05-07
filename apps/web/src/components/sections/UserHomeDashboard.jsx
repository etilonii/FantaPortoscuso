import LiveStatusBadge from "./LiveStatusBadge";
import TeamAlertsCard from "./TeamAlertsCard";
import TeamStrengthExplainer from "./TeamStrengthExplainer";
import TeamTrendCard from "./TeamTrendCard";

const ROLE_LABELS = {
  P: "P",
  D: "D",
  C: "C",
  A: "A",
};

export default function UserHomeDashboard({
  teamName,
  dataStatus,
  formatDataStatusDate,
  standingRow,
  rosterSummary,
  teamFormation,
  alerts,
  strengthRow,
  startingStrengthRow,
  strengthBreakdown,
  trend,
  formatInt,
  formatDecimal,
  onOpenTeam,
  onOpenStandings,
  onOpenFormation,
}) {
  const statusText = String(dataStatus?.result || "").trim().toLowerCase();
  const dataStatusLabel =
    statusText === "ok" ? "Tutto ok" : statusText === "running" ? "In corso" : "Da verificare";

  return (
    <section className="dashboard user-home-dashboard">
      <div className="dashboard-header left row user-home-header">
        <div>
          <p className="eyebrow">La tua squadra</p>
          <h2>{teamName || "Squadra associata"}</h2>
          <p className="muted">
            Ultimo aggiornamento dati: {formatDataStatusDate(dataStatus?.last_update) || "-"}
          </p>
        </div>

        <div className="user-home-status-row">
          <span className={`status-badge ${statusText === "ok" ? "is-ok" : statusText === "running" ? "is-running" : "is-warning"}`}>
            {dataStatusLabel}
          </span>
          {teamFormation?.live_status ? (
            <LiveStatusBadge
              status={teamFormation.live_status}
              label={teamFormation.live_status_label}
              reason={teamFormation.live_status_reason}
            />
          ) : null}
        </div>
      </div>

      <div className="user-home-hero">
        <button type="button" className="admin-card user-hero-card" onClick={onOpenStandings}>
          <span>Posizione in classifica</span>
          <strong>
            {Number.isFinite(Number(standingRow?.pos)) ? `#${standingRow.pos}` : "Dato non ancora disponibile"}
          </strong>
          <small>
            {Number.isFinite(Number(standingRow?.points_live ?? standingRow?.points))
              ? `${formatDecimal(standingRow?.points_live ?? standingRow?.points, 2)} pt totali`
              : "Dato non ancora disponibile"}
          </small>
        </button>

        <button type="button" className="admin-card user-hero-card" onClick={onOpenFormation}>
          <span>Punteggio giornata</span>
          <strong>
            {Number.isFinite(Number(standingRow?.live_total))
              ? formatDecimal(standingRow?.live_total, 2)
              : "Dato non ancora disponibile"}
          </strong>
          <small>{teamFormation?.live_status_reason || "Stato live squadra"}</small>
        </button>

        <button type="button" className="admin-card user-hero-card" onClick={onOpenTeam}>
          <span>Valore rosa attuale</span>
          <strong>
            {Number.isFinite(Number(rosterSummary?.totals?.attuale))
              ? formatInt(rosterSummary?.totals?.attuale)
              : "Dato non ancora disponibile"}
          </strong>
          <small>
            {Number.isFinite(Number(rosterSummary?.totals?.delta))
              ? `${Number(rosterSummary.totals.delta) >= 0 ? "+" : ""}${formatInt(rosterSummary.totals.delta)} vs acquisto`
              : "Delta non disponibile"}
          </small>
        </button>
      </div>

      <div className="user-home-role-grid">
        {["P", "D", "C", "A"].map((role) => (
          <div key={role} className="summary-card">
            <span>{ROLE_LABELS[role]}</span>
            <strong>{rosterSummary?.byRole?.[role]?.count ?? "-"}</strong>
          </div>
        ))}
      </div>

      <div className="user-home-panels">
        <TeamAlertsCard alerts={alerts} emptyLabel="Nessun alert prioritario per la tua squadra." />
        <TeamStrengthExplainer
          strengthRow={strengthRow}
          startingStrengthRow={startingStrengthRow}
          breakdown={strengthBreakdown}
          formatDecimal={formatDecimal}
        />
        <TeamTrendCard trend={trend} />
      </div>
    </section>
  );
}
