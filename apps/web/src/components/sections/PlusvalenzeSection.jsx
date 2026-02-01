export default function PlusvalenzeSection({
  plusvalenzePeriod,
  setPlusvalenzePeriod,
  plusvalenzeQuery,
  setPlusvalenzeQuery,
  filteredPlusvalenze,
  formatInt,
  goToTeam,
}) {
  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Plusvalenze</p>
          <h2>Classifica completa</h2>
        </div>
      </div>

      <div className="panel">
        <div className="filters inline centered plusvalenze-filters">
          <label className="field">
            <span>Periodo</span>
            <select
              className="select"
              value={plusvalenzePeriod}
              onChange={(e) => setPlusvalenzePeriod(e.target.value)}
            >
              <option value="december">Da Dicembre</option>
              <option value="start">Dall&apos;inizio</option>
            </select>
          </label>

          <label className="field stats-search">
            <span>Cerca team</span>
            <input
              className="input"
              type="text"
              placeholder="Cerca team..."
              value={plusvalenzeQuery}
              onChange={(e) => setPlusvalenzeQuery(e.target.value)}
            />
          </label>
        </div>

        <div className="list">
          {filteredPlusvalenze.length === 0 ? (
            <p className="muted">Nessun dato disponibile.</p>
          ) : (
            filteredPlusvalenze.map((item, index) => (
              <div
                key={item.team}
                className="list-item player-card"
                onClick={() => goToTeam(item.team)}
              >
                <div>
                  <p className="rank-title">
                    <span className="rank-badge">#{index + 1}</span>
                    <span className="team-name">{item.team}</span>
                  </p>
                  <span className="muted">
                    Acquisto {formatInt(item.acquisto)} · Attuale {formatInt(item.attuale)}
                  </span>
                </div>
                <strong>
                  {formatInt(item.plusvalenza)} ({item.percentuale}%)
                </strong>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  );
}
