const STATUS_META = {
  final: { label: "Finale", className: "is-ok" },
  in_progress: { label: "In corso", className: "is-running" },
  needs_review: { label: "Da verificare", className: "is-warning" },
  unknown: { label: "Mai eseguito", className: "is-neutral" },
};

export default function LiveStatusBadge({ status, label, reason = "", compact = false }) {
  const key = String(status || "").trim().toLowerCase();
  const meta = STATUS_META[key] || STATUS_META.unknown;
  return (
    <span
      className={`status-badge live-status-badge ${meta.className}${compact ? " compact" : ""}`}
      title={reason || label || meta.label}
    >
      {label || meta.label}
    </span>
  );
}
