import { useMemo } from "react";
import AdminKeysPanel from "./AdminKeysPanel";
import AdminOverviewPanel from "./AdminOverviewPanel";
import LegacyJobsStatusPanel from "./LegacyJobsStatusPanel";
import MaintenancePanel from "./MaintenancePanel";
import ManualImportPanel from "./ManualImportPanel";
import {
  cleanText,
  deriveGlobalStatus,
  formatDateTime,
  statusLabel,
  statusTone,
} from "./adminRoomUtils";

function QuickAction({ targetId, label }) {
  return (
    <button
      className="ghost"
      type="button"
      onClick={() => {
        const node = document.getElementById(targetId);
        if (node) node.scrollIntoView({ behavior: "smooth", block: "start" });
      }}
    >
      {label}
    </button>
  );
}

function HeaderKpi({ label, value, status }) {
  return (
    <div className="admin-card admin-kpi-card">
      <span>{label}</span>
      <strong>{value}</strong>
      {status ? <span className={`status-badge compact ${statusTone(status)}`}>{statusLabel(status)}</span> : null}
    </div>
  );
}

export default function AdminRoom(props) {
  const {
    API_BASE,
    fetchWithAuth,
    dataStatus,
    productModeStatus,
    loadDataStatus,
    loadAdminStatus,
    adminStatus,
    adminKeys,
    maintenanceStatus,
    maintenanceRetryMinutesDraft,
    setMaintenanceRetryMinutesDraft,
    maintenanceMessageDraft,
    setMaintenanceMessageDraft,
    maintenanceApplying,
    setMaintenanceMode,
    loadMaintenanceStatus,
    createNewKey,
    loadAdminKeys,
    adminNotice,
    newKey,
    adminSetAdminKey,
    setAdminSetAdminKey,
    setAdminForKey,
    adminTeamKey,
    setAdminTeamKey,
    adminTeamName,
    setAdminTeamName,
    assignTeamKey,
    adminResetKey,
    setAdminResetKey,
    adminResetNote,
    setAdminResetNote,
    adminResetUsage,
    resetKeyAdmin,
    adminImportKeys,
    setAdminImportKeys,
    adminImportIsAdmin,
    setAdminImportIsAdmin,
    importKeysAdmin,
    adminImportTeamKeys,
    setAdminImportTeamKeys,
    importTeamKeysAdmin,
    adminKeyNotesDraft,
    updateAdminKeyNoteDraft,
    saveAdminKeyNote,
    adminSavingNoteKey,
    adminDeletingKey,
    adminBlockingKey,
    blockAdminKey,
    unblockAdminKey,
    deleteAdminKey,
    formatLastAccess,
    refreshMarketAdmin,
  } = props;

  const jobsStatus = dataStatus?.jobs_status || { jobs: [] };
  const jobs = Array.isArray(jobsStatus?.jobs) ? jobsStatus.jobs : [];
  const globalStatus = deriveGlobalStatus({
    dataStatus,
    maintenanceStatus,
    jobs,
  });

  const keySummary = useMemo(() => {
    const items = Array.isArray(adminKeys) ? adminKeys : [];
    return {
      total: items.length,
      used: items.filter((item) => item?.used).length,
      free: items.filter((item) => !item?.used).length,
      admin: items.filter((item) => item?.is_admin).length,
      online: items.filter((item) => item?.online).length,
      blocked: items.filter((item) => item?.blocked).length,
      withResetUsage: items.filter((item) => Number(item?.reset_used || 0) > 0).length,
      withTeam: items.filter((item) => String(item?.team || "").trim()).length,
    };
  }, [adminKeys]);

  return (
    <section className="dashboard admin-room">
      <div className="panel admin-room-header">
        <div className="admin-room-title">
          <div>
            <p className="eyebrow">Admin</p>
            <h2>Admin Room</h2>
            <p className="muted">Cabina di regia per dati, sync, accessi e manutenzione</p>
          </div>
          <button
            className="ghost"
            onClick={() => {
              loadAdminStatus();
              loadDataStatus();
              loadAdminKeys();
              loadMaintenanceStatus();
            }}
          >
            Aggiorna stato
          </button>
        </div>

        <div className="admin-kpi-grid">
          <HeaderKpi
            label="Modalita attuale"
            value={cleanText(productModeStatus?.effective_mode_label, "Safe manual import mode")}
            status={globalStatus}
          />
          <HeaderKpi
            label="Legacy imports"
            value={productModeStatus?.legacy_remote_imports_enabled ? "Attivi" : "Disattivati"}
            status={productModeStatus?.legacy_remote_imports_enabled ? "ok" : "pending"}
          />
          <HeaderKpi
            label="Manual imports"
            value={productModeStatus?.manual_imports_enabled ? "Attivi" : "Disattivati"}
            status={productModeStatus?.manual_imports_enabled ? "ok" : "error"}
          />
          <HeaderKpi label="Ultimo aggiornamento dati" value={formatDateTime(dataStatus?.last_update)} />
          <HeaderKpi label="Stato globale" value={statusLabel(globalStatus)} status={globalStatus} />
        </div>

        <div className="admin-quick-actions">
          <QuickAction targetId="admin-overview" label="Panoramica" />
          <QuickAction targetId="admin-sync" label="Vai a Sync" />
          <QuickAction targetId="admin-import" label="Vai a Import" />
          <QuickAction targetId="admin-keys" label="Vai a Key" />
          <QuickAction targetId="admin-maintenance" label="Vai a Manutenzione" />
        </div>

        {adminNotice ? <div className="admin-room-notice">{adminNotice}</div> : null}
      </div>

      <AdminOverviewPanel
        dataStatus={dataStatus}
        jobsStatus={jobsStatus}
        keySummary={keySummary}
        maintenanceStatus={maintenanceStatus}
      />

      <LegacyJobsStatusPanel
        productModeStatus={productModeStatus}
        jobsStatus={jobsStatus}
      />

      <div id="admin-import">
        <ManualImportPanel
          API_BASE={API_BASE}
          fetchWithAuth={fetchWithAuth}
          dataStatus={dataStatus}
          productModeStatus={productModeStatus}
          onReloadDataStatus={loadDataStatus}
        />
      </div>

      <AdminKeysPanel
        adminKeys={adminKeys}
        newKey={newKey}
        createNewKey={createNewKey}
        loadAdminKeys={loadAdminKeys}
        adminSetAdminKey={adminSetAdminKey}
        setAdminSetAdminKey={setAdminSetAdminKey}
        setAdminForKey={setAdminForKey}
        adminTeamKey={adminTeamKey}
        setAdminTeamKey={setAdminTeamKey}
        adminTeamName={adminTeamName}
        setAdminTeamName={setAdminTeamName}
        assignTeamKey={assignTeamKey}
        adminResetKey={adminResetKey}
        setAdminResetKey={setAdminResetKey}
        adminResetNote={adminResetNote}
        setAdminResetNote={setAdminResetNote}
        adminResetUsage={adminResetUsage}
        resetKeyAdmin={resetKeyAdmin}
        adminImportKeys={adminImportKeys}
        setAdminImportKeys={setAdminImportKeys}
        adminImportIsAdmin={adminImportIsAdmin}
        setAdminImportIsAdmin={setAdminImportIsAdmin}
        importKeysAdmin={importKeysAdmin}
        adminImportTeamKeys={adminImportTeamKeys}
        setAdminImportTeamKeys={setAdminImportTeamKeys}
        importTeamKeysAdmin={importTeamKeysAdmin}
        adminKeyNotesDraft={adminKeyNotesDraft}
        updateAdminKeyNoteDraft={updateAdminKeyNoteDraft}
        saveAdminKeyNote={saveAdminKeyNote}
        adminSavingNoteKey={adminSavingNoteKey}
        adminDeletingKey={adminDeletingKey}
        adminBlockingKey={adminBlockingKey}
        blockAdminKey={blockAdminKey}
        unblockAdminKey={unblockAdminKey}
        deleteAdminKey={deleteAdminKey}
        formatLastAccess={formatLastAccess}
      />

      <MaintenancePanel
        maintenanceStatus={maintenanceStatus}
        maintenanceRetryMinutesDraft={maintenanceRetryMinutesDraft}
        setMaintenanceRetryMinutesDraft={setMaintenanceRetryMinutesDraft}
        maintenanceMessageDraft={maintenanceMessageDraft}
        setMaintenanceMessageDraft={setMaintenanceMessageDraft}
        maintenanceApplying={maintenanceApplying}
        setMaintenanceMode={setMaintenanceMode}
        loadMaintenanceStatus={loadMaintenanceStatus}
      />

      <section id="admin-diagnostics" className="panel admin-section">
        <div className="admin-section-header">
          <div>
            <p className="eyebrow">Diagnostica tecnica</p>
            <h3>Dettagli e link utili</h3>
          </div>
        </div>

        <div className="admin-diagnostics-grid">
          <div className="admin-card">
            <h4>Meta endpoint</h4>
            <div className="admin-diagnostic-links">
              <a href={`${API_BASE}/meta/product-mode`} target="_blank" rel="noreferrer">
                /meta/product-mode
              </a>
              <a href={`${API_BASE}/meta/data-status`} target="_blank" rel="noreferrer">
                /meta/data-status
              </a>
              <a href={`${API_BASE}/meta/jobs-status`} target="_blank" rel="noreferrer">
                /meta/jobs-status
              </a>
            </div>
          </div>

          <div className="admin-card">
            <h4>Snapshot corrente</h4>
            <p className="muted">Data status: {cleanText(dataStatus?.result)}</p>
            <p className="muted">Messaggio: {cleanText(dataStatus?.message)}</p>
            <p className="muted">Jobs osservati: {jobs.length}</p>
            <p className="muted">Ultimo accesso API admin: {formatDateTime(adminStatus?.auth?.last_seen_at)}</p>
          </div>

          <div className="admin-card">
            <h4>Azioni tecniche</h4>
            <div className="admin-inline-actions">
              <button className="ghost" onClick={refreshMarketAdmin}>
                Refresh mercato
              </button>
              <button className="ghost" onClick={loadAdminStatus}>
                Aggiorna admin status
              </button>
              <button className="ghost" onClick={loadDataStatus}>
                Aggiorna data status
              </button>
            </div>
          </div>
        </div>
      </section>
    </section>
  );
}
