import { useMemo, useState } from "react";
import { cleanText } from "./adminRoomUtils";

function KeySummaryCard({ label, value, tone = "neutral" }) {
  return (
    <div className={`admin-mini-stat ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function KeyBadge({ children, tone = "neutral" }) {
  return <span className={`admin-inline-badge ${tone}`}>{children}</span>;
}

function KeyCard({
  item,
  noteDraft,
  savingNote,
  deletingKey,
  blockingKey,
  onNoteChange,
  onSaveNote,
  onBlockToggle,
  onDelete,
  formatLastAccess,
}) {
  const isBlocked = Boolean(item?.blocked);

  return (
    <article className="admin-card key-card">
      <div className="admin-card-head">
        <div>
          <h4>{String(item?.key || "").toUpperCase()}</h4>
          <div className="admin-inline-badges">
            <KeyBadge tone={item?.used ? "ok" : "neutral"}>{item?.used ? "Usata" : "Libera"}</KeyBadge>
            {item?.is_admin ? <KeyBadge tone="accent">Admin</KeyBadge> : null}
            {item?.online ? <KeyBadge tone="info">Online</KeyBadge> : null}
            {isBlocked ? <KeyBadge tone="error">Bloccata</KeyBadge> : null}
          </div>
        </div>
        <div className="key-card-meta">
          <strong>{cleanText(item?.team)}</strong>
          <span>Team</span>
        </div>
      </div>

      <div className="key-card-grid">
        <p className="muted">Ultimo accesso: {item?.online ? "Online ora" : formatLastAccess(item?.last_seen_at || item?.used_at)}</p>
        <p className="muted">
          Reset: {item?.reset_used ?? 0}/{item?.reset_limit ?? 3}
          {item?.reset_season ? ` · Stagione ${item.reset_season}` : ""}
        </p>
        <p className={isBlocked ? "muted key-blocked" : "muted"}>
          Blocco:{" "}
          {isBlocked
            ? item?.blocked_until
              ? `attivo fino a ${formatLastAccess(item.blocked_until)}`
              : "attivo fino a sblocco manuale"
            : "nessuno"}
        </p>
        <p className="muted">Nota attuale: {cleanText(item?.note)}</p>
      </div>

      {isBlocked && item?.blocked_reason ? (
        <p className="muted key-blocked-reason">Motivo blocco: {item.blocked_reason}</p>
      ) : null}

      <div className="admin-row admin-row-key-note">
        <input
          className="input"
          placeholder="Nota opzionale per questa key"
          value={noteDraft}
          maxLength={255}
          onChange={(event) => onNoteChange(event.target.value)}
        />
        <button
          className={savingNote ? "ghost note-save-btn is-loading" : "ghost note-save-btn"}
          onClick={onSaveNote}
          disabled={savingNote || deletingKey || blockingKey}
        >
          {savingNote ? "Salvataggio..." : "Salva nota"}
        </button>
      </div>

      <details className="admin-details">
        <summary>Azioni avanzate</summary>
        <div className="admin-details-body">
          <div className="admin-inline-actions">
            <button
              className={
                blockingKey
                  ? "ghost key-block-btn is-loading"
                  : isBlocked
                  ? "ghost key-block-btn is-unblock"
                  : "ghost key-block-btn"
              }
              onClick={onBlockToggle}
              disabled={blockingKey || deletingKey || savingNote}
            >
              {blockingKey ? "Aggiorno..." : isBlocked ? "Sblocca key" : "Blocca key"}
            </button>
            <button
              className={deletingKey ? "ghost key-delete-btn is-loading" : "ghost key-delete-btn"}
              onClick={onDelete}
              disabled={deletingKey || savingNote || blockingKey}
            >
              {deletingKey ? "Eliminazione..." : "Elimina key"}
            </button>
          </div>
          <p className="muted">Usa il blocco per sospensioni temporanee. L'eliminazione resta un'azione rara.</p>
        </div>
      </details>
    </article>
  );
}

export default function AdminKeysPanel({
  adminKeys,
  newKey,
  createNewKey,
  loadAdminKeys,
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
}) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("all");

  const summary = useMemo(() => {
    const items = Array.isArray(adminKeys) ? adminKeys : [];
    return {
      total: items.length,
      used: items.filter((item) => item?.used).length,
      free: items.filter((item) => !item?.used).length,
      admin: items.filter((item) => item?.is_admin).length,
      blocked: items.filter((item) => item?.blocked).length,
      online: items.filter((item) => item?.online).length,
      withResetUsage: items.filter((item) => Number(item?.reset_used || 0) > 0).length,
    };
  }, [adminKeys]);

  const filteredKeys = useMemo(() => {
    const needle = String(search || "").trim().toLowerCase();
    return (Array.isArray(adminKeys) ? adminKeys : []).filter((item) => {
      const matchesSearch =
        !needle ||
        String(item?.key || "").toLowerCase().includes(needle) ||
        String(item?.team || "").toLowerCase().includes(needle) ||
        String(item?.note || "").toLowerCase().includes(needle);
      if (!matchesSearch) return false;
      if (filter === "used") return Boolean(item?.used);
      if (filter === "free") return !item?.used;
      if (filter === "admin") return Boolean(item?.is_admin);
      if (filter === "blocked") return Boolean(item?.blocked);
      if (filter === "online") return Boolean(item?.online);
      return true;
    });
  }, [adminKeys, filter, search]);

  return (
    <section id="admin-keys" className="panel admin-section">
      <div className="admin-section-header">
        <div>
          <p className="eyebrow">Accessi e key</p>
          <h3>Controllo accessi</h3>
        </div>
        <button className="ghost" onClick={loadAdminKeys}>
          Aggiorna key
        </button>
      </div>

      <div className="admin-mini-stats">
        <KeySummaryCard label="Totale key" value={summary.total} tone="accent" />
        <KeySummaryCard label="Usate" value={summary.used} tone="ok" />
        <KeySummaryCard label="Libere" value={summary.free} />
        <KeySummaryCard label="Admin" value={summary.admin} tone="info" />
        <KeySummaryCard label="Online" value={summary.online} tone="info" />
        <KeySummaryCard label="Bloccate" value={summary.blocked} tone="error" />
        <KeySummaryCard label="Reset usati" value={summary.withResetUsage} tone="warning" />
      </div>

      <div className="admin-actions-grid">
        <article className="admin-card">
          <div className="admin-card-head">
            <div>
              <h4>Genera key</h4>
              <p>Crea una nuova key singola.</p>
            </div>
          </div>
          <div className="admin-inline-actions">
            <button className="primary" onClick={createNewKey}>
              Genera nuova key
            </button>
          </div>
          {newKey ? (
            <div className="new-key">
              <span>Nuova key</span>
              <strong>{String(newKey || "").toUpperCase()}</strong>
            </div>
          ) : null}
        </article>

        <article className="admin-card">
          <div className="admin-card-head">
            <div>
              <h4>Importa key</h4>
              <p>Una per riga o separate da spazio, virgola o punto e virgola.</p>
            </div>
          </div>
          <div className="admin-actions">
            <textarea
              className="admin-textarea"
              placeholder="abc123&#10;def456"
              value={adminImportKeys}
              onChange={(event) => setAdminImportKeys(event.target.value)}
            />
            <label className="admin-checkbox">
              <input
                type="checkbox"
                checked={adminImportIsAdmin}
                onChange={(event) => setAdminImportIsAdmin(event.target.checked)}
              />
              Importa come key admin
            </label>
            <button className="ghost" onClick={importKeysAdmin}>
              Importa key
            </button>
          </div>
        </article>

        <article className="admin-card">
          <div className="admin-card-head">
            <div>
              <h4>Associazioni team</h4>
              <p>Formato: `key, team` oppure `key; team`, una riga per voce.</p>
            </div>
          </div>
          <div className="admin-actions">
            <div className="admin-row">
              <input
                className="input"
                placeholder="Key da associare"
                value={adminTeamKey}
                onChange={(event) => setAdminTeamKey(event.target.value)}
              />
              <input
                className="input"
                placeholder="Team"
                value={adminTeamName}
                onChange={(event) => setAdminTeamName(event.target.value)}
              />
            </div>
            <div className="admin-inline-actions">
              <button className="ghost" onClick={assignTeamKey}>
                Associa key-team
              </button>
            </div>
            <textarea
              className="admin-textarea"
              placeholder="abc123, Pi-Ciaccio&#10;def456, Team B"
              value={adminImportTeamKeys}
              onChange={(event) => setAdminImportTeamKeys(event.target.value)}
            />
            <button className="ghost" onClick={importTeamKeysAdmin}>
              Importa associazioni
            </button>
          </div>
        </article>

        <article className="admin-card">
          <div className="admin-card-head">
            <div>
              <h4>Ruoli admin e reset</h4>
              <p>Azioni amministrative puntuali sulle key esistenti.</p>
            </div>
          </div>
          <div className="admin-actions">
            <div className="admin-row">
              <input
                className="input"
                placeholder="Key da rendere admin"
                value={adminSetAdminKey}
                onChange={(event) => setAdminSetAdminKey(event.target.value)}
              />
              <button className="ghost" onClick={setAdminForKey}>
                Rendi admin
              </button>
            </div>
            <div className="admin-row">
              <input
                className="input"
                placeholder="Key da resettare"
                value={adminResetKey}
                onChange={(event) => setAdminResetKey(event.target.value)}
              />
              <input
                className="input"
                placeholder="Nota reset"
                value={adminResetNote}
                onChange={(event) => setAdminResetNote(event.target.value)}
              />
              <button className="ghost" onClick={resetKeyAdmin}>
                Reset key
              </button>
            </div>
            <p className="muted">
              Key selezionata {adminResetKey ? adminResetKey.toUpperCase() : "-"} · reset usati{" "}
              {adminResetUsage?.used ?? 0}/{adminResetUsage?.limit ?? 3}
              {adminResetUsage?.season ? ` · stagione ${adminResetUsage.season}` : ""}
            </p>
          </div>
        </article>
      </div>

      <div className="admin-section-subheader">
        <div>
          <h4>Lista key</h4>
          <p className="muted">Ricerca rapida e gestione puntuale di note, blocchi e accessi.</p>
        </div>
      </div>

      <div className="admin-list-toolbar">
        <input
          className="input"
          placeholder="Cerca key, team o nota"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
        <select className="input admin-select" value={filter} onChange={(event) => setFilter(event.target.value)}>
          <option value="all">Tutte</option>
          <option value="used">Usate</option>
          <option value="free">Libere</option>
          <option value="admin">Admin</option>
          <option value="blocked">Bloccate</option>
          <option value="online">Online</option>
        </select>
      </div>

      <div className="admin-key-list">
        {filteredKeys.length === 0 ? (
          <p className="muted">Nessuna key compatibile con i filtri correnti.</p>
        ) : (
          filteredKeys.map((item) => {
            const rowKey = String(item?.key || "").trim().toLowerCase();
            return (
              <KeyCard
                key={rowKey}
                item={item}
                noteDraft={String(adminKeyNotesDraft[rowKey] ?? item?.note ?? "")}
                savingNote={adminSavingNoteKey === rowKey}
                deletingKey={adminDeletingKey === rowKey}
                blockingKey={adminBlockingKey === rowKey}
                onNoteChange={(value) => updateAdminKeyNoteDraft(rowKey, value)}
                onSaveNote={() => saveAdminKeyNote(rowKey)}
                onBlockToggle={() => (item?.blocked ? unblockAdminKey(item.key) : blockAdminKey(item.key))}
                onDelete={() => deleteAdminKey(item.key)}
                formatLastAccess={formatLastAccess}
              />
            );
          })
        )}
      </div>

      <details className="admin-details danger-zone">
        <summary>Dettagli tecnici</summary>
        <div className="admin-details-body">
          <p>Filtri attivi: ricerca `{cleanText(search, "vuota")}` · stato `{filter}`</p>
          <p>
            Conteggi correnti: totale {summary.total} · usate {summary.used} · admin {summary.admin} · bloccate{" "}
            {summary.blocked}
          </p>
        </div>
      </details>
    </section>
  );
}
