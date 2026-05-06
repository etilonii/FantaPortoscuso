import { cleanText, formatDateTime, statusLabel, statusTone } from "./adminRoomUtils";

export default function MaintenancePanel({
  maintenanceStatus,
  maintenanceRetryMinutesDraft,
  setMaintenanceRetryMinutesDraft,
  maintenanceMessageDraft,
  setMaintenanceMessageDraft,
  maintenanceApplying,
  setMaintenanceMode,
  loadMaintenanceStatus,
}) {
  const enabled = Boolean(maintenanceStatus?.enabled);

  return (
    <section id="admin-maintenance" className={`panel admin-section ${enabled ? "maintenance-active" : ""}`}>
      <div className="admin-section-header">
        <div>
          <p className="eyebrow">Manutenzione</p>
          <h3>Stato servizio</h3>
        </div>
        <button className="ghost" onClick={loadMaintenanceStatus}>
          Aggiorna
        </button>
      </div>

      <div className="maintenance-shell">
        <div className="admin-card maintenance-state-card">
          <div className="admin-card-head">
            <div>
              <h4>{enabled ? "Manutenzione attiva" : "Manutenzione disattivata"}</h4>
              <p>
                {enabled
                  ? "Il sito sta limitando l'uso normale e mostra l'avviso di blocco."
                  : "Il sito e in stato regolare. Nessun blocco utente attivo."}
              </p>
            </div>
            <span className={`status-badge compact ${statusTone(enabled ? "running" : "ok")}`}>
              {statusLabel(enabled ? "running" : "ok")}
            </span>
          </div>
          <div className="maintenance-state-grid">
            <p className="muted">Ultimo update: {formatDateTime(maintenanceStatus?.updated_at)}</p>
            <p className="muted">Retry minuti: {Number(maintenanceStatus?.retry_after_minutes || 10)}</p>
            <p className="muted">Messaggio attuale: {cleanText(maintenanceStatus?.message)}</p>
          </div>
        </div>

        <div className="admin-card">
          <div className="admin-card-head">
            <div>
              <h4>Configura manutenzione</h4>
              <p>Imposta messaggio e finestra di retry prima di attivare o disattivare il blocco.</p>
            </div>
          </div>
          <div className="admin-actions">
            <textarea
              className="admin-textarea"
              placeholder="Messaggio mostrato agli utenti durante la manutenzione"
              value={maintenanceMessageDraft}
              onChange={(event) => setMaintenanceMessageDraft(event.target.value)}
            />
            <div className="admin-row">
              <input
                className="input"
                type="number"
                min="1"
                max="120"
                placeholder="Minuti attesa"
                value={maintenanceRetryMinutesDraft}
                onChange={(event) => setMaintenanceRetryMinutesDraft(event.target.value)}
              />
              <button
                className="ghost"
                onClick={() => setMaintenanceMode(true)}
                disabled={maintenanceApplying || enabled}
              >
                {maintenanceApplying ? "Aggiorno..." : "Attiva manutenzione"}
              </button>
              <button
                className="ghost"
                onClick={() => setMaintenanceMode(false)}
                disabled={maintenanceApplying || !enabled}
              >
                {maintenanceApplying ? "Aggiorno..." : "Disattiva manutenzione"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
