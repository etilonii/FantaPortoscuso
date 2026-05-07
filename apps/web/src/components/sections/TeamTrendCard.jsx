export default function TeamTrendCard({ trend }) {
  const recentRounds = Array.isArray(trend?.recent_rounds) ? trend.recent_rounds : [];

  return (
    <div className="panel team-trend-card">
      <div className="panel-header">
        <h3>Andamento</h3>
      </div>
      {recentRounds.length === 0 ? (
        <p className="muted">Andamento non ancora disponibile.</p>
      ) : (
        <div className="team-trend-list">
          {recentRounds.map((item, index) => (
            <div key={`${item?.round || "r"}-${index}`} className="team-trend-item">
              <span>Giornata {item?.round || "-"}</span>
              <strong>{item?.score ?? "-"}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
