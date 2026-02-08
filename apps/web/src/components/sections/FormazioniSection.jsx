export default function FormazioniSection({
  formations,
  formationTeam,
  setFormationTeam,
  openPlayer,
  formatDecimal,
}) {
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

  const renderPlayerPills = (players) => {
    if (!players || !players.length) return <span className="muted">-</span>;
    return (
      <div className="formation-pills">
        {players.map((name) => (
          <button
            key={name}
            type="button"
            className="formation-pill"
            onClick={() => openPlayer(name)}
          >
            {name}
          </button>
        ))}
      </div>
    );
  };

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Formazioni</p>
          <h2>Miglior XI per squadra</h2>
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
        </div>

        {visibleItems.length === 0 ? (
          <p className="muted">Nessuna formazione disponibile.</p>
        ) : (
          <div className="formations-grid">
            {visibleItems.map((item, index) => (
              <article key={`${item.team}-${index}`} className="formation-card">
                <header className="formation-card-head">
                  <p className="rank-title">
                    <span className={`rank-badge rank-${index + 1}`}>#{item.pos || index + 1}</span>
                    <span>{item.team}</span>
                  </p>
                  <div className="formation-meta">
                    <span className="muted">Modulo {item.modulo || "-"}</span>
                    <strong>{formatDecimal(item.forza_titolari, 2)}</strong>
                  </div>
                </header>

                <div className="formation-lines">
                  <div className="formation-line">
                    <span className="formation-label">P</span>
                    {renderPlayerPills(item.portiere ? [item.portiere] : [])}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">D</span>
                    {renderPlayerPills(item.difensori)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">C</span>
                    {renderPlayerPills(item.centrocampisti)}
                  </div>
                  <div className="formation-line">
                    <span className="formation-label">A</span>
                    {renderPlayerPills(item.attaccanti)}
                  </div>
                </div>
              </article>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
