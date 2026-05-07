export default function MarketPrioritySummary({ priorities = [] }) {
  const items = Array.isArray(priorities) ? priorities.filter(Boolean) : [];

  return (
    <div className="panel market-priority-panel">
      <div className="panel-header">
        <h3>Priorita squadra</h3>
      </div>
      {items.length === 0 ? (
        <p className="muted">Nessuna priorita evidente al momento.</p>
      ) : (
        <div className="market-priority-list">
          {items.map((item, index) => (
            <div key={`${item?.title || "priority"}-${index}`} className="market-priority-item">
              <div>
                <strong>{item?.title || "-"}</strong>
                <p className="muted">{item?.message || "-"}</p>
              </div>
              <span className={`status-badge ${item?.className || "is-neutral"}`}>
                {item?.label || "Da monitorare"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
