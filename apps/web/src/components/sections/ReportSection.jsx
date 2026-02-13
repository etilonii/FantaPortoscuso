export default function ReportSection({
  eyebrow,
  title,
  description,
  loading,
  error,
  onReload,
  rows,
  columns,
  emptyLabel = "Nessun dato disponibile.",
}) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const safeColumns = Array.isArray(columns) ? columns : [];

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
          {description ? <p className="muted">{description}</p> : null}
        </div>
      </div>

      <div className="panel">
        <div className="panel-header spaced">
          <h3>Dati</h3>
          {typeof onReload === "function" ? (
            <button className="ghost" type="button" onClick={onReload} disabled={Boolean(loading)}>
              {loading ? "Aggiorno..." : "Aggiorna"}
            </button>
          ) : null}
        </div>
        {error ? <p className="error">{error}</p> : null}
        {loading ? <p className="muted">Caricamento...</p> : null}
        {!loading && safeRows.length === 0 ? <p className="muted">{emptyLabel}</p> : null}
        {!loading && safeRows.length > 0 && safeColumns.length > 0 ? (
          <div className="report-table-wrap">
            <table className="report-table">
              <thead>
                <tr>
                  {safeColumns.map((col) => (
                    <th key={col.key || col.label}>{col.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {safeRows.map((row, index) => (
                  <tr key={row?.id || row?.name || row?.Team || row?.team || `${index}`}>
                    {safeColumns.map((col) => (
                      <td key={`${col.key || col.label}-${index}`}>
                        {typeof col.render === "function"
                          ? col.render(row, index)
                          : String(row?.[col.key] ?? "-")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </div>
    </section>
  );
}
