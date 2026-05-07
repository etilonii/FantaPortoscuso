const ALERT_CLASS = {
  success: "is-success",
  info: "is-info",
  warning: "is-warning",
  danger: "is-danger",
};

export default function TeamAlertsCard({ alerts = [], emptyLabel = "Nessun alert rilevante." }) {
  const items = Array.isArray(alerts) ? alerts.slice(0, 5) : [];

  return (
    <div className="panel team-alerts-card">
      <div className="panel-header">
        <h3>Alert</h3>
      </div>
      {items.length === 0 ? (
        <p className="muted">{emptyLabel}</p>
      ) : (
        <div className="team-alert-list">
          {items.map((alert, index) => (
            <article
              key={`${alert?.title || "alert"}-${index}`}
              className={`team-alert-item ${ALERT_CLASS[String(alert?.severity || "info").toLowerCase()] || "is-info"}`}
            >
              <strong>{alert?.title || "Alert"}</strong>
              <p>{alert?.message || "-"}</p>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}
