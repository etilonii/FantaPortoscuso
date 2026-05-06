export const asArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [String(value)];
};

export const cleanText = (value, fallback = "-") => {
  const text = String(value || "").trim();
  return text || fallback;
};

export const formatDateTime = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("it-IT", {
    dateStyle: "short",
    timeStyle: "short",
  });
};

export const formatDuration = (value) => {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms <= 0) return "-";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds >= 10 ? 0 : 1)} s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);
  return `${minutes}m ${remainingSeconds}s`;
};

export const statusTone = (status) => {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "ok") return "ok";
  if (normalized === "error") return "error";
  if (normalized === "running") return "running";
  if (normalized === "skipped") return "pending";
  return "pending";
};

export const statusLabel = (status) => {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "ok") return "Tutto ok";
  if (normalized === "error") return "Errore";
  if (normalized === "running") return "In corso";
  if (normalized === "skipped") return "Saltato";
  return "Mai eseguito";
};

export const manualImportStatusLabel = (status) => {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "ok") return "ok";
  if (normalized === "error") return "errore";
  return "mai importato";
};

export const humanJobName = (jobName) => {
  const key = String(jobName || "").trim().toLowerCase();
  if (key === "auto_leghe_sync") return "Leghe sync";
  if (key === "auto_live_import") return "Live import";
  if (key === "auto_seriea_live_context_sync") return "Serie A live sync";
  if (key === "sync_complete_total") return "Sync completa";
  return cleanText(jobName, "Job");
};

export const summarizeStepStatus = (status) => {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "ok") return "ok";
  if (normalized === "error") return "errore";
  return normalized || "n/d";
};

export const getPrimaryJob = (jobs) => {
  const values = Array.isArray(jobs) ? jobs.filter(Boolean) : [];
  if (!values.length) return null;
  return (
    values.find((job) => String(job?.status || "").toLowerCase() === "error") ||
    values.find((job) => String(job?.status || "").toLowerCase() === "running") ||
    values.find((job) => String(job?.status || "").toLowerCase() === "skipped") ||
    values
      .slice()
      .sort((a, b) => {
        const aTime = new Date(a?.finished_at || a?.updated_at || a?.started_at || 0).getTime();
        const bTime = new Date(b?.finished_at || b?.updated_at || b?.started_at || 0).getTime();
        return bTime - aTime;
      })[0] ||
    null
  );
};

export const deriveGlobalStatus = ({ dataStatus, maintenanceStatus, jobs }) => {
  const jobList = Array.isArray(jobs) ? jobs : [];
  if (jobList.some((job) => String(job?.status || "").toLowerCase() === "error")) return "error";
  if (jobList.some((job) => String(job?.status || "").toLowerCase() === "running")) return "running";
  if (String(dataStatus?.result || "").toLowerCase() === "error") return "error";
  if (maintenanceStatus?.enabled) return "running";
  if (String(dataStatus?.result || "").toLowerCase() === "ok") return "ok";
  return "unknown";
};
