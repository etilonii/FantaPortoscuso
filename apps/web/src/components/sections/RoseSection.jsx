import LiveStatusBadge from "./LiveStatusBadge";
import TeamAlertsCard from "./TeamAlertsCard";
import TeamStrengthExplainer from "./TeamStrengthExplainer";
import TeamTrendCard from "./TeamTrendCard";

export default function RoseSection({
  teams,
  selectedTeam,
  setSelectedTeam,
  rosterQuery,
  setRosterQuery,
  roleFilter,
  setRoleFilter,
  squadraFilter,
  setSquadraFilter,
  roster,
  formatInt,
  openPlayer,
  formatDecimal,
  sessionTeam = "",
  teamStanding = null,
  teamFormation = null,
  teamAlerts = [],
  strengthRow = null,
  startingStrengthRow = null,
  strengthBreakdown = null,
  teamTrend = null,
}) {
  const baseFiltered = (roster || [])
    .filter((it) => (roleFilter === "all" ? true : it.Ruolo === roleFilter))
    .filter((it) => (squadraFilter === "all" ? true : it.Squadra === squadraFilter))
    .filter((it) =>
      rosterQuery.trim()
        ? String(it.Giocatore || "")
            .toLowerCase()
            .includes(rosterQuery.trim().toLowerCase())
        : true
    );

  const totals = baseFiltered.reduce(
    (acc, it) => {
      const acq = Number(it.PrezzoAcquisto || 0);
      const att = Number(it.PrezzoAttuale || 0);
      if (!Number.isNaN(acq)) acc.acquisto += acq;
      if (!Number.isNaN(att)) acc.attuale += att;
      return acc;
    },
    { acquisto: 0, attuale: 0 }
  );
  const isOwnTeam =
    String(sessionTeam || "").trim() &&
    String(sessionTeam || "").trim().toLowerCase() === String(selectedTeam || "").trim().toLowerCase();

  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Rose</p>
          <h2>{isOwnTeam ? "La tua rosa" : "Rosa squadra"}</h2>
        </div>
        {teamFormation?.live_status ? (
          <LiveStatusBadge
            status={teamFormation.live_status}
            label={teamFormation.live_status_label}
            reason={teamFormation.live_status_reason}
          />
        ) : null}
      </div>

      <div className="team-page-grid">
        <div className="panel team-page-summary">
          <div className="panel-header">
            <h3>{isOwnTeam ? "La tua giornata" : "Riepilogo squadra"}</h3>
          </div>
          <div className="team-page-summary-grid">
            <div className="summary-card">
              <span>Posizione</span>
              <strong>{Number.isFinite(Number(teamStanding?.pos)) ? `#${teamStanding.pos}` : "-"}</strong>
            </div>
            <div className="summary-card">
              <span>Punteggio live</span>
              <strong>
                {Number.isFinite(Number(teamStanding?.live_total))
                  ? formatDecimal(teamStanding?.live_total, 2)
                  : "Dato non disponibile"}
              </strong>
            </div>
            <div className="summary-card">
              <span>Valore attuale</span>
              <strong>{formatInt(totals.attuale)}</strong>
            </div>
            <div className="summary-card">
              <span>Delta valore</span>
              <strong>{`${totals.attuale - totals.acquisto >= 0 ? "+" : ""}${formatInt(totals.attuale - totals.acquisto)}`}</strong>
            </div>
          </div>
        </div>

        <TeamAlertsCard
          alerts={teamAlerts}
          emptyLabel={isOwnTeam ? "I tuoi alert non segnalano criticita immediate." : "Nessun alert rilevante."}
        />
      </div>

      <div className="panel rose-compact">
        <label className="field">
          <span>Seleziona squadra</span>
          <select
            className="select"
            value={selectedTeam}
            onChange={(e) => setSelectedTeam(e.target.value)}
          >
            {teams.map((team) => (
              <option key={team} value={team}>
                {team}
              </option>
            ))}
          </select>
        </label>

        <label className="field">
          <span>Cerca giocatore</span>
          <input
            className="input rose-search-input"
            placeholder="Es. Lautaro, Di Lorenzo..."
            value={rosterQuery}
            onChange={(e) => setRosterQuery(e.target.value)}
          />
        </label>

        <div className="filters">
          <label className="field">
            <span>Ruolo</span>
            <select
              className="select"
              value={roleFilter}
              onChange={(e) => setRoleFilter(e.target.value)}
            >
              <option value="all">Tutti</option>
              <option value="P">Portieri</option>
              <option value="D">Difensori</option>
              <option value="C">Centrocampisti</option>
              <option value="A">Attaccanti</option>
            </select>
          </label>

          <label className="field">
            <span>Squadra reale</span>
            <select
              className="select"
              value={squadraFilter}
              onChange={(e) => setSquadraFilter(e.target.value)}
            >
              <option value="all">Tutte</option>
              {[...new Set(roster.map((r) => r.Squadra).filter(Boolean))].map((sq) => (
                <option key={sq} value={sq}>
                  {sq}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      <div className="panel rose-compact">
        {roster.length === 0 ? (
          <p className="muted">Nessun dato disponibile.</p>
        ) : (
          ["P", "D", "C", "A"].map((role) => {
            const roleLabel =
              role === "P"
                ? "Portieri"
                : role === "D"
                ? "Difensori"
                : role === "C"
                ? "Centrocampisti"
                : "Attaccanti";

            const filtered = baseFiltered.filter((it) => it.Ruolo === role);

            if (!filtered.length) return null;

            const totalSpesa = filtered.reduce((sum, it) => {
              const v = Number(it.PrezzoAcquisto || 0);
              return Number.isNaN(v) ? sum : sum + v;
            }, 0);

            return (
              <details key={role} className="accordion" open>
                <summary>
                  <span>{roleLabel}</span>
                  <strong>
                    <span className="rose-role-label">Spesa</span>
                    <span className="rose-role-label">: </span>
                    <span className="rose-role-value">{formatInt(totalSpesa)}</span>
                  </strong>
                </summary>

                <div className="list">
                  {filtered.map((it, idx) => (
                    <div
                      key={`${it.Giocatore}-${idx}`}
                      className="list-item player-card"
                      onClick={() => openPlayer(it.Giocatore)}
                    >
                      <div>
                        <p>
                          <button
                            type="button"
                            className="link-button"
                            onClick={(e) => {
                              e.stopPropagation();
                              openPlayer(it.Giocatore);
                            }}
                          >
                            {it.Giocatore}
                          </button>{" "}
                          <span className="muted">· {it.Squadra || "-"}</span>
                        </p>
                        <span className="muted">
                          Acquisto {formatInt(it.PrezzoAcquisto)} · Attuale{" "}
                          {formatInt(it.PrezzoAttuale)}
                        </span>
                      </div>
                      <strong>{it.Ruolo}</strong>
                    </div>
                  ))}
                </div>
              </details>
            );
          })
        )}

        <div className="rose-totals">
          <div>
            <span className="rose-total-label">Quotazione acquisto:</span>{" "}
            <strong>{formatInt(totals.acquisto)}</strong>
          </div>
          <div>
            <span className="rose-total-label">Quotazione attuale</span>{" "}
            <strong>{formatInt(totals.attuale)}</strong>
          </div>
        </div>
      </div>

      <div className="team-page-grid">
        <TeamStrengthExplainer
          strengthRow={strengthRow}
          startingStrengthRow={startingStrengthRow}
          breakdown={strengthBreakdown}
          formatDecimal={formatDecimal}
        />
        <TeamTrendCard trend={teamTrend} />
      </div>
    </section>
  );
}
