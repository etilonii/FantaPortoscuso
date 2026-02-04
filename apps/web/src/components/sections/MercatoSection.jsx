export default function MercatoSection({
  marketUpdatedAt,
  marketCountdown,
  isAdmin,
  marketPreview,
  setMarketPreview,
  marketItems,
  formatInt,
  suggestions,
  formatDecimal,
  openPlayer,
  runSuggest,
  suggestLoading,
  suggestError,
  suggestHasRun,
  manualOuts,
  setManualOuts,
  suggestPayload,
  manualResult,
  manualBudgetSummary,
  manualSwapMap,
  manualDislikes,
  setManualDislikes,
  computeManualSuggestions,
  resetManual,
  manualLoading,
  manualError,
  normalizeName,
}) {
  return (
    <section className="dashboard">
      <div className="dashboard-header">
        <div>
          <p className="eyebrow">Mercato</p>
          <h2>Mercato Aperto</h2>
          <p className="muted">Placeholder trasferimenti visibili a tutti.</p>
          {marketUpdatedAt ? (
            <div className="market-update-badge">
              Aggiornamento mercato: {marketUpdatedAt}
            </div>
          ) : null}
        </div>
      </div>

      <div className="panel market-panel">
        <div className="market-warning">
          <div className="market-warning-badge">Live</div>
          <h3>Placeholder mercato</h3>
          <p className="muted">
            Ultimi cambi registrati, aggiornati in base ai dati disponibili.
          </p>

          <div className="market-countdown-inline">
            <span>Aggiornamento in</span>
            <strong>{marketCountdown}</strong>
          </div>
          {marketItems.length ? (
              <div className="list market-preview-list">
                {marketItems.map((item, idx) => (
                  <div key={`${item.team}-${idx}`} className="list-item player-card">
                    <div>
                      <p className="rank-title">
                        <span className="team-name">{item.team}</span>
                      </p>
                      <div className="market-swap-card">
                        <div className="market-swap-col">
                          <span className="market-swap-label">Svincolo</span>
                          <span className="market-swap-name">{item.out || "-"}</span>
                          <span className="market-swap-meta-row">
                            <span className="market-swap-meta">
                              {(item.out_ruolo || "-")} · {(item.out_squadra || "-")}
                            </span>
                            <span className="market-swap-value-badge">
                              {formatInt(item.out_value)}
                            </span>
                          </span>
                        </div>
                        <span className="market-swap-arrow">→</span>
                        <div className="market-swap-col">
                          <span className="market-swap-label">Acquisto</span>
                          <span className="market-swap-name">{item.in || "-"}</span>
                          <span className="market-swap-meta-row">
                            <span className="market-swap-meta">
                              {(item.in_ruolo || "-")} · {(item.in_squadra || "-")}
                            </span>
                            <span className="market-swap-value-badge">
                              {formatInt(item.in_value)}
                            </span>
                          </span>
                        </div>
                        <div
                          className={`market-swap-delta-card ${
                            Number(item.delta) >= 0 ? "pos" : "neg"
                          }`}
                        >
                          <span className="delta-label">Saldo</span>
                          <span className="delta-value">{formatInt(item.delta)}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
          ) : (
            <p className="muted">Nessun placeholder disponibile.</p>
          )}
        </div>
      </div>

      <div className="panel suggest-shell">
        <div className="suggest-header">
          <div>
            <div className="suggest-title-row">
              <h3>Consigli Mercato</h3>
              <span className="suggest-badge">Top 3</span>
            </div>
            <span className="muted">
              Calcola i migliori cambi consigliati in base al budget della tua rosa.
            </span>
          </div>
        </div>

        <div className={`suggest-list ${suggestions.length === 0 ? "is-empty" : ""}`}>
          {suggestions.length === 0 ? null : (
            suggestions.map((sol, idx) => (
              <article key={`sol-${idx}`} className="suggest-card">
                <div className="suggest-card-header">
                  <span className="rank-badge">#{idx + 1}</span>
                  <div>
                    <p className="suggest-title">
                      Gain algoritmico {formatDecimal(sol.total_gain, 2)}
                    </p>
                    <span className="muted">
                      Budget iniziale {formatDecimal(sol.budget_initial, 2)} - Crediti
                      residui {formatDecimal(sol.budget_final, 2)}
                    </span>
                  </div>
                </div>

                <div className="team-tags">
                  {(sol.warnings || [])
                    .filter((w) => !String(w).startsWith("Titolare incerto"))
                    .map((w) => (
                      <span key={w} className="team-tag">
                        {w}
                      </span>
                    ))}
                </div>

                <div className="suggest-swaps">
                  {(sol.swaps || []).map((s, i) => (
                    <div key={`${idx}-${i}`} className="swap-row">
                      <div>
                        <p className="swap-title">
                          <button
                            type="button"
                            className="link-button"
                            onClick={(e) => {
                              e.stopPropagation();
                              if (s.out) openPlayer(s.out);
                            }}
                          >
                            {s.out || "-"}
                          </button>{" "}
                          -{" "}
                          <button
                            type="button"
                            className="link-button"
                            onClick={(e) => {
                              e.stopPropagation();
                              if (s.in) openPlayer(s.in);
                            }}
                          >
                            {s.in || "-"}
                          </button>
                        </p>
                        <span className="muted">
                          Guadagno {formatInt(s.qa_out)} - Spesa {formatInt(s.qa_in)}
                        </span>
                      </div>
                      <strong className="swap-gain">
                        {formatInt((Number(s.qa_out) || 0) - (Number(s.qa_in) || 0))}
                      </strong>
                    </div>
                  ))}
                </div>
              </article>
            ))
          )}
        </div>

        <div className="suggest-footer">
          <button className="primary suggest-cta" onClick={runSuggest} disabled={suggestLoading}>
            {suggestLoading ? "Calcolo..." : "Calcola"}
          </button>
          {suggestError ? <span className="muted suggest-error">{suggestError}</span> : null}
          {suggestHasRun && !suggestLoading && suggestions.length === 0 && !suggestError ? (
            <span className="muted">Nessuna soluzione disponibile.</span>
          ) : null}
        </div>
      </div>

      <div className="panel suggest-guided">
        <div className="suggest-guided-header">
          <div>
            <h3>Trasferimenti personalizzati</h3>
            <span className="muted">
              Scegli chi svincolare e scopri i migliori acquisti secondo l'algoritmo.
            </span>
          </div>
          <div className="suggest-actions">
            {manualError ? <span className="muted suggest-error">{manualError}</span> : null}
          </div>
        </div>

        {suggestPayload ? (
          <div className="guided-budget">
            <span className="muted">
              Crediti residui:{" "}
              {formatInt(
                manualResult ? manualBudgetSummary.budgetFinal : manualBudgetSummary.credits
              )}
            </span>
            <span className="muted">Svincolati: {formatInt(manualBudgetSummary.outSum)}</span>
            <span className="muted">
              Budget Totale: {formatInt(manualBudgetSummary.maxBudget)}
            </span>
            {manualResult ? (
              <span className="muted">
                Costo d'Acquisto: {formatInt(manualBudgetSummary.spent)}
              </span>
            ) : null}
          </div>
        ) : null}

        <div className="guided-grid">
          {(manualOuts || []).map((value, i) => {
            const usedNames = new Set(
              manualOuts
                .filter((_, idx) => idx !== i)
                .map((name) => String(name || "").trim())
                .filter(Boolean)
            );

            const swap = manualSwapMap.get(normalizeName(value));

            return (
              <div key={`out-${i}`} className="guided-card">
                <div className="guided-out">
                  <p>Svincolo #{i + 1}</p>
                  <select
                    className="select out-select"
                    value={value}
                    onChange={(e) => {
                      const next = [...manualOuts];
                      next[i] = e.target.value;
                      setManualOuts(next);
                    }}
                  >
                    <option value="">(Nessuno)</option>
                    {(suggestPayload?.user_squad || []).map((p) => {
                      const name = p.nome || p.Giocatore;
                      const disabled =
                        String(name || "").trim() !== String(value || "").trim() &&
                        usedNames.has(String(name || "").trim());

                      return (
                        <option key={`${name}-${i}`} value={name} disabled={disabled}>
                          {name} ({p.Ruolo || p.ruolo_base || "-"})
                        </option>
                      );
                    })}
                  </select>
                </div>

                <div className="guided-in">
                  {swap ? (
                    <div>
                      <p className="guided-title">Acquisto suggerito</p>
                      <p>
                        <button
                          type="button"
                          className="link-button"
                          onClick={(e) => {
                            e.stopPropagation();
                            if (swap.in) openPlayer(swap.in);
                          }}
                        >
                          {swap.in || "-"}
                        </button>
                      </p>
                      <span className="muted">
                        Guadagno {formatInt(swap.qa_out)} - Spesa {formatInt(swap.qa_in)}
                      </span>
                      <label className="guided-dislike">
                        <input
                          type="checkbox"
                          checked={manualDislikes.has(normalizeName(swap.in))}
                          onChange={(e) => {
                            const key = normalizeName(swap.in);
                            setManualDislikes((prev) => {
                              const next = new Set(prev);
                              if (e.target.checked) {
                                next.add(key);
                              } else {
                                next.delete(key);
                              }
                              return next;
                            });
                          }}
                        />
                        Non mi piace
                      </label>
                    </div>
                  ) : (
                    <p className="muted">Seleziona un OUT e calcola.</p>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        <div className="guided-footer">
          <button className="primary" onClick={computeManualSuggestions} disabled={manualLoading}>
            {manualLoading
              ? "Calcolo..."
              : manualDislikes.size > 0
              ? "Ricalcola"
              : "Calcola"}
          </button>
          <button className="ghost" onClick={resetManual}>
            Reset
          </button>
        </div>
      </div>
    </section>
  );
}
