const suggestionFromProfile = ({ profile, stats, teamCount }) => {
  const fantamedia = Number(stats?.Fantamedia);
  const goals = Number(stats?.Gol || 0);
  const assists = Number(stats?.Assist || 0);
  const cards = Number(stats?.Ammonizioni || 0) + Number(stats?.Espulsioni || 0);

  if (Number.isFinite(fantamedia) && fantamedia >= 7 && teamCount >= 2) {
    return { tone: "success", label: "Hold", text: "Rendimento e diffusione in lega da tenere stretti." };
  }
  if (Number.isFinite(fantamedia) && fantamedia >= 6.7 && teamCount === 0) {
    return { tone: "info", label: "Watchlist", text: "Profilo interessante: monitoralo per il prossimo giro." };
  }
  if ((goals + assists) >= 5 && teamCount <= 1) {
    return { tone: "info", label: "Buy", text: "Produzione offensiva buona rispetto alla diffusione in lega." };
  }
  if (Number.isFinite(fantamedia) && fantamedia < 6 && cards >= 4) {
    return { tone: "warning", label: "Sell", text: "Trend disciplinare e rendimento da rivalutare." };
  }
  if (profile || stats) {
    return { tone: "info", label: "Hold", text: "Profilo stabile: servono altri dati per una mossa netta." };
  }
  return null;
};

export default function PlayerProfileCard({
  selectedPlayer,
  playerProfile,
  playerStats,
  playerTeamCount,
  formatInt,
  goToSquadra,
  jumpToId,
  playerSlug,
  setListoneQuery,
  setStatsTab,
}) {
  const suggestion = suggestionFromProfile({
    profile: playerProfile,
    stats: playerStats,
    teamCount: playerTeamCount,
  });

  const statCards = [
    ["Partite", playerStats?.Partite],
    ["Gol", playerStats?.Gol],
    ["Assist", playerStats?.Assist],
    ["Fantamedia", playerStats?.Fantamedia],
    ["Mediavoto", playerStats?.Mediavoto],
    ["Ammonizioni", playerStats?.Ammonizioni],
  ];

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Scheda giocatore</p>
          <h2>{selectedPlayer || "Giocatore"}</h2>
        </div>
        {suggestion ? (
          <span className={`status-badge ${suggestion.tone === "success" ? "is-ok" : suggestion.tone === "warning" ? "is-warning" : "is-neutral"}`}>
            {suggestion.label}
          </span>
        ) : null}
      </div>

      <div className="player-profile-grid">
        <button
          type="button"
          className="admin-card player-profile-card"
          onClick={() => goToSquadra(playerProfile?.Squadra, playerProfile?.Ruolo)}
        >
          <span>Squadra e ruolo</span>
          <strong>
            {playerProfile?.Squadra || "-"} · {playerProfile?.Ruolo || "-"}
          </strong>
        </button>

        <button
          type="button"
          className="admin-card player-profile-card"
          onClick={() =>
            jumpToId(`listone-${playerSlug}`, "listone", () => setListoneQuery(selectedPlayer || ""))
          }
        >
          <span>Quotazione attuale</span>
          <strong>{formatInt(playerProfile?.PrezzoAttuale)}</strong>
        </button>

        <div className="admin-card player-profile-card">
          <span>FVM</span>
          <strong>{formatInt(playerProfile?.FVM || playerProfile?.QA)}</strong>
        </div>

        <div className="admin-card player-profile-card">
          <span>Presenza in lega</span>
          <strong>{playerTeamCount}</strong>
        </div>
      </div>

      {suggestion ? (
        <div className="panel player-profile-note">
          <strong>{suggestion.label}</strong>
          <p className="muted">{suggestion.text}</p>
        </div>
      ) : null}

      <div className="panel">
        <div className="panel-header">
          <h3>Statistiche stagionali</h3>
        </div>
        {playerStats ? (
          <div className="player-profile-stats">
            {statCards.map(([label, value]) => (
              <button
                key={label}
                type="button"
                className="summary-card clickable"
                onClick={() => {
                  const tabKey =
                    label === "Gol"
                      ? "gol"
                      : label === "Assist"
                      ? "assist"
                      : label === "Ammonizioni"
                      ? "ammonizioni"
                      : null;
                  if (tabKey) {
                    jumpToId(`stat-${tabKey}-${playerSlug}`, "stats", () => setStatsTab(tabKey));
                  }
                }}
              >
                <span>{label}</span>
                <strong>{value ?? "-"}</strong>
              </button>
            ))}
          </div>
        ) : (
          <p className="muted">Statistiche non disponibili.</p>
        )}
      </div>
    </section>
  );
}
