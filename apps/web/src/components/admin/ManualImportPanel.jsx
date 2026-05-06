import { useMemo, useState } from "react";
import { asArray, cleanText, formatDateTime, manualImportStatusLabel, statusTone } from "./adminRoomUtils";

const SOURCES = [
  {
    key: "rose",
    title: "Rose",
    endpoint: "/data/admin/manual-import/rose",
    buttonLabel: "Carica rose",
  },
  {
    key: "quotazioni",
    title: "Quotazioni",
    endpoint: "/data/admin/manual-import/quotazioni",
    buttonLabel: "Carica quotazioni",
  },
];

const extractBackendMessages = (payload) => {
  const detail = payload?.detail;
  if (detail && typeof detail === "object") {
    return {
      errors: asArray(detail.errors).length
        ? asArray(detail.errors)
        : asArray(detail.message || detail.reason || "Upload non riuscito"),
      warnings: asArray(detail.warnings),
    };
  }
  if (typeof detail === "string" && detail.trim()) {
    return { errors: [detail], warnings: [] };
  }
  return {
    errors: asArray(payload?.errors),
    warnings: asArray(payload?.warnings),
  };
};

function StatusList({ title, items, kind }) {
  const values = asArray(items);
  if (!values.length) return null;
  return (
    <div className={`manual-import-messages ${kind}`}>
      <p>{title}</p>
      <ul>
        {values.map((item, index) => (
          <li key={`${kind}-${index}`}>{cleanText(item)}</li>
        ))}
      </ul>
    </div>
  );
}

function SourceBlock({
  source,
  status,
  selectedFile,
  uploading,
  result,
  disabled,
  onFileChange,
  onUpload,
  inputVersion,
}) {
  const warnings = asArray(status?.warnings);
  const errors = asArray(status?.errors);
  const resultMessages = extractBackendMessages(result || {});

  return (
    <div className="manual-import-source admin-card">
      <div className="manual-import-source-head">
        <div>
          <h4>{source.title}</h4>
          <span className={`status-badge compact ${statusTone(status?.status)}`}>
            {manualImportStatusLabel(status?.status)}
          </span>
        </div>
        <strong>{Number(status?.imported_rows || 0)} righe</strong>
      </div>

      <div className="manual-import-summary">
        <div className="manual-import-summary-item">
          <span>Ultimo import</span>
          <strong>{formatDateTime(status?.last_import_at)}</strong>
        </div>
        <div className="manual-import-summary-item">
          <span>File</span>
          <strong>{cleanText(status?.original_filename)}</strong>
        </div>
        <div className="manual-import-summary-item">
          <span>Righe</span>
          <strong>{Number(status?.imported_rows || 0)}</strong>
        </div>
      </div>

      {!status ? (
        <p className="muted manual-import-empty">Nessun import manuale eseguito</p>
      ) : null}

      {warnings.length || errors.length ? (
        <details className="admin-details">
          <summary>Esito ultimo import</summary>
          <div className="admin-details-body">
            <StatusList title="Warning" items={warnings} kind="warning" />
            <StatusList title="Errori" items={errors} kind="error" />
          </div>
        </details>
      ) : null}

      <div className="manual-import-upload">
        <input
          key={`${source.key}-${inputVersion}`}
          type="file"
          accept=".csv,.xlsx"
          disabled={disabled || uploading}
          onChange={(event) => onFileChange(source.key, event.target.files?.[0] || null)}
        />
        <button
          className="ghost"
          onClick={() => onUpload(source)}
          disabled={disabled || uploading || !selectedFile}
        >
          {uploading ? "Caricamento..." : source.buttonLabel}
        </button>
      </div>

      {selectedFile ? (
        <p className="muted manual-import-selected">Selezionato: {selectedFile.name}</p>
      ) : null}

      {result ? (
        <div className={`manual-import-result ${result.status === "ok" ? "ok" : "error"}`}>
          <strong>{result.status === "ok" ? "Import completato" : "Import non attivato"}</strong>
          {result.imported_rows !== undefined ? (
            <span>{Number(result.imported_rows || 0)} righe elaborate</span>
          ) : null}
          {resultMessages.warnings.length || resultMessages.errors.length ? (
            <details className="admin-details">
              <summary>Dettaglio risposta backend</summary>
              <div className="admin-details-body">
                <StatusList title="Warning" items={resultMessages.warnings} kind="warning" />
                <StatusList title="Errori" items={resultMessages.errors} kind="error" />
              </div>
            </details>
          ) : null}
        </div>
      ) : null}

      {status?.stored_path || status?.activated_path ? (
        <details className="admin-details">
          <summary>Dettagli tecnici</summary>
          <div className="admin-details-body">
            <p>Stored path: {cleanText(status?.stored_path)}</p>
            <p>Activated path: {cleanText(status?.activated_path)}</p>
          </div>
        </details>
      ) : null}
    </div>
  );
}

export default function ManualImportPanel({
  API_BASE,
  fetchWithAuth,
  dataStatus,
  productModeStatus,
  onReloadDataStatus,
}) {
  const [files, setFiles] = useState({ rose: null, quotazioni: null });
  const [uploading, setUploading] = useState({ rose: false, quotazioni: false });
  const [results, setResults] = useState({});
  const [inputVersions, setInputVersions] = useState({ rose: 0, quotazioni: 0 });

  const manualImports = dataStatus?.manual_imports || {};
  const product = useMemo(
    () => ({
      product_mode: productModeStatus?.product_mode || dataStatus?.product?.product_mode || "manual_import",
      data_import_mode: productModeStatus?.data_import_mode || dataStatus?.product?.data_import_mode || "manual",
      effective_mode_label:
        productModeStatus?.effective_mode_label ||
        dataStatus?.product?.effective_mode_label ||
        "Safe manual import mode",
      manual_imports_enabled:
        productModeStatus?.manual_imports_enabled ?? dataStatus?.product?.manual_imports_enabled ?? true,
      legacy_remote_imports_enabled:
        productModeStatus?.legacy_remote_imports_enabled ??
        dataStatus?.product?.legacy_remote_imports_enabled ??
        false,
    }),
    [dataStatus?.product, productModeStatus]
  );
  const manualImportsEnabled = Boolean(product.manual_imports_enabled);

  const handleFileChange = (sourceKey, file) => {
    setFiles((prev) => ({ ...prev, [sourceKey]: file }));
    setResults((prev) => ({ ...prev, [sourceKey]: null }));
  };

  const handleUpload = async (source) => {
    const file = files[source.key];
    if (!file || uploading[source.key] || !manualImportsEnabled) return;

    setUploading((prev) => ({ ...prev, [source.key]: true }));
    setResults((prev) => ({ ...prev, [source.key]: null }));

    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetchWithAuth(`${API_BASE}${source.endpoint}`, {
        method: "POST",
        body: formData,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const messages = extractBackendMessages(payload);
        setResults((prev) => ({
          ...prev,
          [source.key]: {
            status: "error",
            errors: messages.errors.length ? messages.errors : ["Upload non riuscito"],
            warnings: messages.warnings,
          },
        }));
      } else {
        setResults((prev) => ({ ...prev, [source.key]: payload }));
        if (payload?.status === "ok") {
          setFiles((prev) => ({ ...prev, [source.key]: null }));
          setInputVersions((prev) => ({ ...prev, [source.key]: Number(prev[source.key] || 0) + 1 }));
        }
      }
    } catch {
      setResults((prev) => ({
        ...prev,
        [source.key]: {
          status: "error",
          errors: ["Errore di rete durante upload"],
          warnings: [],
        },
      }));
    } finally {
      setUploading((prev) => ({ ...prev, [source.key]: false }));
      if (typeof onReloadDataStatus === "function") {
        await onReloadDataStatus();
      }
    }
  };

  return (
    <section className="panel admin-section manual-import-panel">
      <div className="admin-section-header">
        <div>
          <p className="eyebrow">Import manuale</p>
          <h3>Import Manuale Dati</h3>
          <p className="muted">
            {product.legacy_remote_imports_enabled
              ? "Modalita privata analyzer attiva. Gli import manuali restano disponibili insieme ai job legacy abilitati via env."
              : "Carica solo file che hai diritto di usare. FantaPortoscuso lavora in modalita import manuale e non effettua scraping da piattaforme terze."}
          </p>
        </div>
      </div>

      <div className="manual-import-mode">
        <span>Mode: {cleanText(product.effective_mode_label)}</span>
        <span>Product mode: {cleanText(product.product_mode)}</span>
        <span>Data import: {cleanText(product.data_import_mode)}</span>
        <span>Manual imports: {manualImportsEnabled ? "attivi" : "disattivati"}</span>
        <span>Legacy remote: {product.legacy_remote_imports_enabled ? "attivi" : "disattivati"}</span>
      </div>

      {!manualImportsEnabled ? (
        <div className="manual-import-disabled">Import manuali disattivati</div>
      ) : null}

      <div className="manual-import-grid">
        {SOURCES.map((source) => (
          <SourceBlock
            key={source.key}
            source={source}
            status={manualImports?.[source.key] || null}
            selectedFile={files[source.key]}
            uploading={Boolean(uploading[source.key])}
            result={results[source.key]}
            disabled={!manualImportsEnabled}
            onFileChange={handleFileChange}
            onUpload={handleUpload}
            inputVersion={inputVersions[source.key]}
          />
        ))}
      </div>
    </section>
  );
}
