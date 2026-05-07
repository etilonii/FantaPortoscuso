const ROLE_LABELS = {
  P: "Portieri",
  D: "Difesa",
  C: "Centrocampo",
  A: "Attacco",
};

export default function TeamStrengthExplainer({
  strengthRow,
  startingStrengthRow,
  breakdown,
  formatDecimal,
}) {
  const roles = breakdown?.roles && typeof breakdown.roles === "object" ? breakdown.roles : null;
  const strongestRole = breakdown?.strongest_role || "";
  const weakestRole = breakdown?.weakest_role || "";

  return (
    <div className="panel team-strength-card">
      <div className="panel-header">
        <h3>Forza rosa</h3>
      </div>
      {!strengthRow ? (
        <p className="muted">Ranking forza rosa non ancora disponibile.</p>
      ) : (
        <>
          <div className="team-strength-summary">
            <div className="team-strength-kpi">
              <span>Ranking generale</span>
              <strong>#{strengthRow?.Pos || "-"}</strong>
            </div>
            <div className="team-strength-kpi">
              <span>Forza totale</span>
              <strong>{formatDecimal(strengthRow?.ForzaSquadra, 2)}</strong>
            </div>
            <div className="team-strength-kpi">
              <span>Forza titolari</span>
              <strong>
                {formatDecimal(
                  startingStrengthRow?.ForzaTitolari ?? strengthRow?.ForzaTitolari,
                  2
                )}
              </strong>
            </div>
          </div>

          {roles ? (
            <div className="team-strength-role-grid">
              {["P", "D", "C", "A"].map((role) => {
                const roleData = roles?.[role] || {};
                return (
                  <div key={role} className="team-strength-role-card">
                    <span>{ROLE_LABELS[role]}</span>
                    <strong>{formatDecimal(roleData?.total_force, 2)}</strong>
                    <small>{roleData?.players || 0} giocatori</small>
                  </div>
                );
              })}
            </div>
          ) : null}

          <p className="muted team-strength-copy">
            {roles
              ? `Punto forte: ${ROLE_LABELS[strongestRole] || "n/d"}. Punto debole: ${
                  ROLE_LABELS[weakestRole] || "n/d"
                }.`
              : "Breakdown per ruolo non ancora disponibile."}
          </p>
        </>
      )}
    </div>
  );
}
