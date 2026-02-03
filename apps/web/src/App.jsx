          </section>
        </div>
      ) : (
        <div className="app-shell">
          <aside className="sidebar" aria-label="Menu principale">
            <div className="brand">
              <span className="eyebrow">FantaPortoscuso</span>
              <h2>Menù</h2>
            </div>

            <nav className="menu">
              <button
                className={activeMenu === "home" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("home");
                  setMenuOpen(false);
                }}
              >
                Home
              </button>

              <button
                className={activeMenu === "stats" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("stats");
                  setMenuOpen(false);
                }}
              >
                Statistiche Giocatori
              </button>

              <button
                className={activeMenu === "rose" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("rose");
                  setMenuOpen(false);
                }}
              >
                Rose
              </button>

              <button
                className={
                  activeMenu === "plusvalenze" ? "menu-item active" : "menu-item"
                }
                onClick={() => {
                  setActiveMenu("plusvalenze");
                  setMenuOpen(false);
                }}
              >
                Plusvalenze
              </button>

              <button
                className={activeMenu === "listone" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("listone");
                  setMenuOpen(false);
                }}
              >
                Listone
              </button>

              <button
                className={
                  activeMenu === "top-acquisti" ? "menu-item active" : "menu-item"
                }
                onClick={() => {
                  setActiveMenu("top-acquisti");
                  setMenuOpen(false);
                }}
              >
                Giocatori più acquistati
              </button>
              <button
                className={activeMenu === "mercato" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("mercato");
                  setMenuOpen(false);
                }}
              >
                Mercato
              </button>

              {isAdmin && (
                <button
                  className={activeMenu === "admin" ? "menu-item active" : "menu-item"}
                  onClick={() => {
                    setActiveMenu("admin");
                    setMenuOpen(false);
                  }}
                >
                  Admin
                </button>
              )}
            </nav>
          </aside>

          <header className="mobile-topbar">
            <button
              className="burger"
              onClick={() =>
                setMenuOpen(!document.body.classList.contains("menu-open"))
              }
              aria-label="Apri menu"
            >
              <span />
              <span />
              <span />
            </button>

            <div>
              <p className="eyebrow">FantaPortoscuso</p>
              <strong>
                {activeMenu === "home"
                  ? "Home"
                  : activeMenu === "stats"
                  ? "Statistiche"
                  : activeMenu === "rose"
                  ? "Rose"
                  : activeMenu === "plusvalenze"
                  ? "Plusvalenze"
                  : activeMenu === "listone"
                  ? "Listone"
                  : activeMenu === "top-acquisti"
                  ? "Top Acquisti"
                  : activeMenu === "mercato"
                  ? "Mercato"
                  : activeMenu === "player"
                  ? "Scheda giocatore"
                  : "Admin"}
              </strong>
            </div>

            <button className="ghost theme-toggle" onClick={toggleTheme}>
              {theme === "dark" ? "Dark" : "Light"}
            </button>
          </header>

          <div className="menu-overlay" onClick={() => setMenuOpen(false)} />

          <main className="content">
            {/* ===========================
                HOME (placeholder minimale)
            =========================== */}
            {activeMenu === "home" && (
              <HomeSection
                summary={summary}
                activeTab={activeTab}
                setActiveTab={setActiveTab}
                query={query}
                setQuery={setQuery}
                hasSearched={hasSearched}
                aggregatedRoseResults={aggregatedRoseResults}
                expandedRose={expandedRose}
                setExpandedRose={setExpandedRose}
                quoteSearchResults={quoteSearchResults}
                formatInt={formatInt}
                openPlayer={openPlayer}
                topTab={topTab}
                setTopTab={setTopTab}
                topQuotes={topQuotes}
                topPlusvalenze={topPlusvalenze}
                topStats={topStats}
                statsTab={statsTab}
                setStatsTab={setStatsTab}
                statColumn={statColumn}
                goToTeam={goToTeam}
                setActiveMenu={setActiveMenu}
              />
            )}

            {/* ===========================
                STATS
            =========================== */}
            {activeMenu === "stats" && (
              <StatsSection
                statsTab={statsTab}
                setStatsTab={setStatsTab}
                statsQuery={statsQuery}
                setStatsQuery={setStatsQuery}
                filteredStatsItems={filteredStatsItems}
                slugify={slugify}
                openPlayer={openPlayer}
                tabToColumn={tabToColumn}
              />
            )}

            {/* ===========================
                PLUSVALENZE
            =========================== */}
            {activeMenu === "plusvalenze" && (
              <PlusvalenzeSection
                plusvalenzePeriod={plusvalenzePeriod}
                setPlusvalenzePeriod={setPlusvalenzePeriod}
                plusvalenzeQuery={plusvalenzeQuery}
                setPlusvalenzeQuery={setPlusvalenzeQuery}
                filteredPlusvalenze={filteredPlusvalenze}
                formatInt={formatInt}
                goToTeam={goToTeam}
              />
            )}

            {/* ===========================
                ROSE
            =========================== */}
            {activeMenu === "rose" && (
              <RoseSection
                teams={teams}
                selectedTeam={selectedTeam}
                setSelectedTeam={setSelectedTeam}
                rosterQuery={rosterQuery}
                setRosterQuery={setRosterQuery}
                roleFilter={roleFilter}
                setRoleFilter={setRoleFilter}
                squadraFilter={squadraFilter}
                setSquadraFilter={setSquadraFilter}
                roster={roster}
                formatInt={formatInt}
                openPlayer={openPlayer}
              />
            )}

            {/* ===========================
                LISTONE
            =========================== */}
            {activeMenu === "listone" && (
              <ListoneSection
                quoteRole={quoteRole}
                setQuoteRole={setQuoteRole}
                quoteTeam={quoteTeam}
                setQuoteTeam={setQuoteTeam}
                quoteOrder={quoteOrder}
                setQuoteOrder={setQuoteOrder}
                quoteList={quoteList}
                listoneQuery={listoneQuery}
                setListoneQuery={setListoneQuery}
                formatInt={formatInt}
                slugify={slugify}
                openPlayer={openPlayer}
              />
            )}

            {/* ===========================
                TOP ACQUISTI
            =========================== */}
            {activeMenu === "top-acquisti" && (
              <TopAcquistiSection
                activeTopRole={activeTopRole}
                setActiveTopRole={setActiveTopRole}
                aggregatesLoading={aggregatesLoading}
                topAcquistiQuery={topAcquistiQuery}
                setTopAcquistiQuery={setTopAcquistiQuery}
                filteredTopAcquisti={filteredTopAcquisti}
                openPlayer={openPlayer}
                formatInt={formatInt}
              />
            )}

            {activeMenu === "mercato" && (
              <MercatoSection
                marketUpdatedAt={marketUpdatedAt}
                marketCountdown={marketCountdown}
                isAdmin={isAdmin}
                marketPreview={marketPreview}
                setMarketPreview={setMarketPreview}
                marketItems={marketItems}
                formatInt={formatInt}
                suggestions={suggestions}
                formatDecimal={formatDecimal}
                openPlayer={openPlayer}
                runSuggest={runSuggest}
                suggestLoading={suggestLoading}
                suggestError={suggestError}
                suggestHasRun={suggestHasRun}
                manualOuts={manualOuts}
                setManualOuts={setManualOuts}
                suggestPayload={suggestPayload}
                manualResult={manualResult}
                manualBudgetSummary={manualBudgetSummary}
                manualSwapMap={manualSwapMap}
                manualDislikes={manualDislikes}
                setManualDislikes={setManualDislikes}
                computeManualSuggestions={computeManualSuggestions}
                resetManual={resetManual}
                manualLoading={manualLoading}
                manualError={manualError}
                normalizeName={normalizeName}
              />
            )}

            {/* ===========================
                PLAYER
            =========================== */}
            {activeMenu === "player" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Scheda giocatore</p>
                    <h2>{selectedPlayer || "Giocatore"}</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="list">
                    <div
                      className="list-item player-card"
                      onClick={() => goToSquadra(playerProfile?.Squadra, playerProfile?.Ruolo)}
                    >
                      <div>
                        <p>Profilo</p>
                        <span className="muted">Squadra · Ruolo</span>
                      </div>
                      <strong>
                        {playerProfile?.Squadra || "-"} · {playerProfile?.Ruolo || "-"}
                      </strong>
                    </div>

                    <div
                      className="list-item player-card"
                      onClick={() =>
                        jumpToId(
                          `listone-${playerSlug}`,
                          "listone",
                          () => setListoneQuery(selectedPlayer || "")
                        )
                      }
                    >
                      <div>
                        <p>Prezzo attuale</p>
                        <span className="muted">Quotazione</span>
                      </div>
                      <strong>{formatInt(playerProfile?.PrezzoAttuale)}</strong>
                    </div>

                    <div className="list-item player-card">
                      <div>
                        <p>Teams</p>
                        <span className="muted">Nella lega</span>
                      </div>
                      <strong>{playerTeamCount}</strong>
                    </div>
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h3>Statistiche</h3>
                  </div>

                  {playerStats ? (
                    <div className="list">
                      {[
                        ["gol", "Gol", playerStats.Gol],
                        ["assist", "Assist", playerStats.Assist],
                        ["ammonizioni", "Ammonizioni", playerStats.Ammonizioni],
                        ["cleansheet", "Cleansheet", playerStats.Cleansheet],
                        ["espulsioni", "Espulsioni", playerStats.Espulsioni],
                        ["autogol", "Autogol", playerStats.Autogol],
                      ].map(([key, label, value]) => (
                        <div
                          key={key}
                          className="list-item player-card"
                          onClick={() =>
                            jumpToId(
                              `stat-${key}-${playerSlug}`,
                              "stats",
                              () => setStatsTab(key)
                            )
                          }
                        >
                          <div>
                            <p>{label}</p>
                            <span className="muted">Stagione</span>
                          </div>
                          <strong>{value ?? "-"}</strong>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">Statistiche non disponibili.</p>
                  )}
                </div>
              </section>
            )}

            {/* ===========================
                ADMIN
            =========================== */}
            {activeMenu === "admin" && isAdmin && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Admin</p>
                    <h2>Gestione Key</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="admin-actions">
                    <button className="primary" onClick={createNewKey}>
                      Genera nuova key
                    </button>

                    {newKey ? (
                      <div className="new-key">
                        <span>Nuova key:</span>
                        <strong>{String(newKey || "").toUpperCase()}</strong>
                      </div>
                    ) : null}
                    {adminNotice ? <div className="new-key">{adminNotice}</div> : null}
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h3>Stato dati</h3>
                    <button className="ghost" onClick={loadAdminStatus}>
                      Aggiorna
                    </button>
                  </div>
                  <div className="list">
                    <div className="list-item player-card">
                      <div>
                        <p>Rose &amp; Quotazioni</p>
                        <span className="muted">Ultimo update</span>
                      </div>
                      <strong>
                        {adminStatus?.data?.last_update?.last_signature ? "OK" : "N/A"}
                      </strong>
                    </div>
                    <div className="list-item player-card">
                      <div>
                        <p>Statistiche</p>
                        <span className="muted">Ultimo update</span>
                      </div>
                      <strong>
                        {adminStatus?.data?.last_stats_update?.last_signature ? "OK" : "N/A"}
                      </strong>
                    </div>
                    <div className="list-item player-card">
                      <div>
                        <p>Mercato</p>
                        <span className="muted">Ultima data</span>
                      </div>
                      <strong>{adminStatus?.market?.latest_date || "-"}</strong>
                    </div>
                    <div className="list-item player-card">
                      <div>
                        <p>Device online</p>
                        <span className="muted">Ultimi 5 minuti</span>
                      </div>
                      <strong>{adminStatus?.auth?.online_devices ?? 0}</strong>
                    </div>
                  </div>
                  <div className="admin-actions">
                    <button className="ghost" onClick={refreshMarketAdmin}>
                      Force refresh mercato
                    </button>
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h3>Operazioni Admin</h3>
                  </div>
                  <div className="admin-actions">
                    <div className="admin-row">
                      <input
                        className="input"
                        placeholder="Key da rendere ADMIN"
                        value={adminSetAdminKey}
                        onChange={(e) => setAdminSetAdminKey(e.target.value)}
                      />
                      <button className="ghost" onClick={setAdminForKey}>
                        Rendi admin
                      </button>
                    </div>

                    <div className="admin-row">
                      <input
                        className="input"
                        placeholder="Key da associare"
                        value={adminTeamKey}
                        onChange={(e) => setAdminTeamKey(e.target.value)}
                      />
                      <input
                        className="input"
                        placeholder="Team (es. Pi-Ciaccio)"
                        value={adminTeamName}
                        onChange={(e) => setAdminTeamName(e.target.value)}
                      />
                      <button className="ghost" onClick={assignTeamKey}>
                        Associa team
                      </button>
                    </div>

                    <div className="admin-row">
                      <input
                        className="input"
                        placeholder="Key da resettare"
                        value={adminResetKey}
                        onChange={(e) => setAdminResetKey(e.target.value)}
                      />
                      <button className="ghost" onClick={resetKeyAdmin}>
                        Reset key
                      </button>
                    </div>

                    <div className="admin-row admin-row-stacked">
                      <p className="muted">Le associazioni key-team si gestiscono sopra.</p>
                    </div>
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h3>Lista Key</h3>
                    <button className="ghost" onClick={loadAdminKeys}>
                      Aggiorna
                    </button>
                  </div>

                  <div className="list">
                    {adminKeys.length === 0 ? (
                      <p className="muted">Nessuna key disponibile.</p>
                    ) : (
                      adminKeys.map((item) => (
                        <div key={item.key} className="list-item player-card">
                          <div>
                            <p>{String(item.key || "").toUpperCase()}</p>
                            <span className="muted">
                              {item.is_admin ? "ADMIN" : "USER"} - {item.used ? "Attivata" : "Non usata"}
                            </span>
                            <span className="muted">Team: {item.team || "-"}</span>
                            <span className="muted">
                              Ultimo accesso:{" "}
                              {item.online
                                ? "Online"
                                : formatLastAccess(item.last_seen_at || item.used_at)}
                            </span>
                          </div>
                          <button
                            className="ghost"
                            onClick={() => deleteTeamKeyAdmin(item.key)}
                          >
                            Elimina
                          </button>
                        </div>
                      ))
                    )}
                  </div>
                </div>

              </section>
            )}
          </main>
        </div>
      )}
    </div>
  );
}

