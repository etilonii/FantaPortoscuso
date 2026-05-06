import {
  asArray,
  cleanText,
  formatDateTime,
  formatDuration,
  humanJobName,
  statusLabel,
  statusTone,
} from "./adminRoomUtils";

const JOB_ORDER = [
  "auto_leghe_sync",
  "auto_live_import",
  "auto_seriea_live_context_sync",
  "sync_complete_total",
];

function JobMetric({ label, value }) {
  return (
    <div className="job-metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function JobCard({ job }) {
  const downloadedKeys = asArray(job?.downloaded_keys);
  const updatedAt = job?.finished_at || job?.updated_at || job?.started_at;

  return (
    <article className="admin-card job-card">
      <div className="admin-card-head">
        <div>
          <h4>{humanJobName(job?.job_name)}</h4>
          <p>{cleanText(job?.reason || job?.message, "Nessun messaggio disponibile")}</p>
        </div>
        <span className={`status-badge compact ${statusTone(job?.status)}`}>
          {statusLabel(job?.status)}
        </span>
      </div>

      <div className="job-metrics">
        <JobMetric label="Ultimo run" value={formatDateTime(updatedAt)} />
        <JobMetric label="Durata" value={formatDuration(job?.duration_ms)} />
        <JobMetric
          label="Righe importate"
          value={job?.imported_rows === null || job?.imported_rows === undefined ? "-" : job.imported_rows}
        />
        <JobMetric
          label="Next run"
          value={job?.next_run_estimate_utc ? formatDateTime(job.next_run_estimate_utc) : "-"}
        />
      </div>

      {downloadedKeys.length ? (
        <p className="muted job-inline-detail">Downloaded keys: {downloadedKeys.join(", ")}</p>
      ) : null}

      <details className="admin-details">
        <summary>Dettagli tecnici</summary>
        <div className="admin-details-body">
          <p>Source: {cleanText(job?.source)}</p>
          <p>Started at: {formatDateTime(job?.started_at)}</p>
          <p>Finished at: {formatDateTime(job?.finished_at)}</p>
          <p>Updated at: {formatDateTime(job?.updated_at)}</p>
          <p>Running flag: {job?.running ? "true" : "false"}</p>
          <p>Message: {cleanText(job?.message)}</p>
          <p>Reason: {cleanText(job?.reason)}</p>
        </div>
      </details>
    </article>
  );
}

export default function LegacyJobsStatusPanel({ productModeStatus, jobsStatus }) {
  const jobs = Array.isArray(jobsStatus?.jobs) ? jobsStatus.jobs : [];
  const jobsMap = new Map(jobs.map((job) => [String(job?.job_name || "").trim(), job]));
  const visibleJobs = JOB_ORDER.map((name) => jobsMap.get(name) || { job_name: name, status: "unknown" });

  return (
    <section id="admin-sync" className="panel admin-section">
      <div className="admin-section-header">
        <div>
          <p className="eyebrow">Sincronizzazioni automatiche</p>
          <h3>Cron legacy</h3>
        </div>
      </div>

      <div
        className={`admin-info-banner ${
          productModeStatus?.legacy_remote_imports_enabled ? "is-active" : "is-muted"
        }`}
      >
        {productModeStatus?.legacy_remote_imports_enabled
          ? "Modalita privata analyzer attiva: i cron legacy possono aggiornare i dati automaticamente."
          : "Import automatici legacy disattivati: il sistema usa import manuale."}
      </div>

      <div className="jobs-grid">
        {visibleJobs.map((job) => (
          <JobCard key={job.job_name} job={job} />
        ))}
      </div>
    </section>
  );
}
