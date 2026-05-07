const PRIORITY_META = {
  high: { label: "Alta priorita", className: "is-warning" },
  medium: { label: "Media priorita", className: "is-neutral" },
  low: { label: "Bassa priorita", className: "is-ok" },
  opportunity: { label: "Occasione", className: "is-ok" },
  risk: { label: "Rischio", className: "is-danger" },
  watch: { label: "Da monitorare", className: "is-neutral" },
};

export default function MarketSuggestionCard({
  title,
  badge = "watch",
  subtitle = "",
  meta = [],
  body = "",
  footer = null,
  onClick = null,
}) {
  const tone = PRIORITY_META[String(badge || "").trim().toLowerCase()] || PRIORITY_META.watch;

  const content = (
    <>
      <div className="market-advisor-card-head">
        <div>
          <h4>{title || "-"}</h4>
          {subtitle ? <p className="muted">{subtitle}</p> : null}
        </div>
        <span className={`status-badge ${tone.className}`}>{tone.label}</span>
      </div>

      {meta.length ? (
        <div className="market-advisor-meta">
          {meta.map((item, index) => (
            <span key={`${item?.label || "meta"}-${index}`}>
              <strong>{item?.label || "-"}</strong> {item?.value || "-"}
            </span>
          ))}
        </div>
      ) : null}

      {body ? <p className="market-advisor-copy">{body}</p> : null}
      {footer}
    </>
  );

  if (typeof onClick === "function") {
    return (
      <button type="button" className="panel market-advisor-card actionable" onClick={onClick}>
        {content}
      </button>
    );
  }

  return <article className="panel market-advisor-card">{content}</article>;
}
