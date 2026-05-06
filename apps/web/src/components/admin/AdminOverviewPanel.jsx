import {
  cleanText,
  formatDateTime,
  getPrimaryJob,
  humanJobName,
  statusLabel,
  statusTone,
  summarizeStepStatus,
} from "./adminRoomUtils";

function OverviewCard({ title, status, summary, meta, children }) {
  return (
    <article className="admin-card overview-card">
      <div className="admin-card-head">
        <div>
          <h4>{title}</h4>
          <p>{summary}</p>
        </div>
        <span className={`status-badge compact ${statusTone(status)}`}>{statusLabel(status)}</span>
      </div>
      {meta ? <p className="muted overview-card-meta">{meta}</p> : null}
      {children}
    </article>
  );
}

export default function AdminOverviewPanel({
  dataStatus,
  jobsStatus,
  keySummary,
  maintenanceStatus,
}) {
  const jobs = Array.isArray(jobsStatus?.jobs) ? jobsStatus.jobs : [];
  const primaryJob = getPrimaryJob(jobs);
  const steps = dataStatus?.steps || {};
  const maintenanceEnabled = Boolean(maintenanceStatus?.enabled);

  const dataMeta = Object.entries({
    rose: steps?.rose,
    stats: steps?.stats,
    strength: steps?.strength,
    quotazioni: steps?.quotazioni,
  })
    .filter(([, value]) => value)
    .map(([key, value]) => `${key}: ${summarizeStepStatus(value)}`)
    .join(" · ");

  return (
    <section id="admin-overview" className="panel admin-section">
      <div className="admin-section-header">
        <div>
          <p className="eyebrow">Panoramica</p>
          <h3>Situazione attuale</h3>
        </div>
      </div>

      <div className="admin-overview-grid">
        <OverviewCard
          title="Stato dati"
          status={dataStatus?.result || "unknown"}
          summary={
            String(dataStatus?.result || "").toLowerCase() === "ok"
              ? "Dati aggiornati"
              : "Serve attenzione sui dati correnti"
          }
          meta={`${cleanText(dataStatus?.message, "Nessun dettaglio")} · Ultimo update ${formatDateTime(
            dataStatus?.last_update
          )}`}
        >
          {dataMeta ? <p className="muted overview-card-detail">{dataMeta}</p> : null}
        </OverviewCard>

        <OverviewCard
          title="Sincronizzazioni automatiche"
          status={primaryJob?.status || "unknown"}
          summary={
            primaryJob
              ? `${humanJobName(primaryJob?.job_name)} · ${statusLabel(primaryJob?.status)}`
              : "Nessun job osservato"
          }
          meta={
            primaryJob
              ? `${cleanText(primaryJob?.reason || primaryJob?.message, "Nessun messaggio")} · Ultimo run ${formatDateTime(
                  primaryJob?.finished_at || primaryJob?.updated_at || primaryJob?.started_at
                )}`
              : "Apri la sezione Sync per il dettaglio completo"
          }
        />

        <OverviewCard
          title="Accessi e key"
          status={keySummary.blocked > 0 ? "running" : "ok"}
          summary={`${keySummary.total} key · ${keySummary.used} usate · ${keySummary.online} online`}
          meta={`${keySummary.free} libere · ${keySummary.admin} admin · ${keySummary.withResetUsage} con reset usati`}
        >
          <p className="muted overview-card-detail">
            {keySummary.withTeam} associate a un team · {keySummary.blocked} bloccate
          </p>
        </OverviewCard>

        <OverviewCard
          title="Manutenzione"
          status={maintenanceEnabled ? "running" : "ok"}
          summary={maintenanceEnabled ? "Modalita manutenzione attiva" : "Manutenzione disattivata"}
          meta={
            maintenanceEnabled
              ? `${cleanText(maintenanceStatus?.message, "Messaggio non impostato")} · Retry ${Number(
                  maintenanceStatus?.retry_after_minutes || 10
                )} minuti`
              : `Ultimo update ${formatDateTime(maintenanceStatus?.updated_at)}`
          }
        />
      </div>
    </section>
  );
}
