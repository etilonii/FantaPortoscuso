import { useEffect, useMemo, useState } from "react";

const makeRowKey = (team, player) => `${team}::${player}`;

const EVENT_COLUMNS = [
  { key: "goal", label: "G", title: "Gol segnati" },
  { key: "assist", label: "A", title: "Assist" },
  { key: "assist_da_fermo", label: "A*", title: "Assist da fermo" },
  { key: "rigore_segnato", label: "RS", title: "Rigori segnati" },
  { key: "rigore_parato", label: "RP", title: "Rigori parati" },
  { key: "rigore_sbagliato", label: "R-", title: "Rigori sbagliati" },
  { key: "gol_subito_portiere", label: "GS", title: "Gol subiti (portiere)" },
  { key: "ammonizione", label: "AMM", title: "Ammonizioni" },
  { key: "espulsione", label: "ESP", title: "Espulsioni" },
  { key: "autogol", label: "AG", title: "Autogol" },
  { key: "gol_vittoria", label: "GV", title: "Gol vittoria" },
  { key: "gol_pareggio", label: "GP", title: "Gol pareggio" },
];

const toEditNumber = (value, fallback = "6") => {
  if (value === undefined || value === null || value === "") return fallback;
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return String(n);
};

const toEditCount = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "0";
  return String(Math.floor(n));
};

const formatLiveNumber = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  if (Math.abs(n - Math.round(n)) < 0.0001) return String(Math.round(n));
  return n.toFixed(2).replace(/\.?0+$/, "").replace(".", ",");
};

export default function LiveSection({
  liveData,
  liveLoading,
  liveError,
  liveSavingKey,
  onReload,
  onRoundChange,
  onToggleSixPolitico,
  onSavePlayer,
}) {
  const fixtures = Array.isArray(liveData?.fixtures) ? liveData.fixtures : [];
  const teams = Array.isArray(liveData?.teams) ? liveData.teams : [];
  const availableRounds = Array.isArray(liveData?.available_rounds)
    ? liveData.available_rounds
    : [];
  const bonusMap =
    liveData?.bonus_malus && typeof liveData.bonus_malus === "object"
      ? liveData.bonus_malus
      : {};
  const activeRound = liveData?.round ? String(liveData.round) : "";

  const [edits, setEdits] = useState({});

  useEffect(() => {
    const next = {};
    teams.forEach((team) => {
      const players = Array.isArray(team?.players) ? team.players : [];
      players.forEach((player) => {
        const key = makeRowKey(team.team, player.name);
        const payload = {
          vote: toEditNumber(player.vote, "6"),
          is_sv: Boolean(player.is_sv),
        };
        EVENT_COLUMNS.forEach((column) => {
          const eventValue =
            player?.events?.[column.key] ??
            player?.[column.key] ??
            0;
          payload[column.key] = toEditCount(eventValue);
        });
        next[key] = payload;
      });
    });
    setEdits(next);
  }, [teams, liveData?.round]);

  const teamCards = useMemo(() => {
    return teams.map((team) => {
      const players = Array.isArray(team?.players) ? team.players : [];
      return { ...team, players };
    });
  }, [teams]);

  const updateEdit = (key, patch) => {
    setEdits((prev) => {
      const base = prev[key] || { vote: "6", is_sv: false };
      return {
        ...prev,
        [key]: {
          ...base,
          ...patch,
        },
      };
    });
  };

  const computePreviewFantavote = (row, fallbackLabel) => {
    if (row?.is_sv) return "SV";
    const voteNumber = Number(row?.vote);
    if (!Number.isFinite(voteNumber)) return fallbackLabel || "-";

    let total = voteNumber;
    EVENT_COLUMNS.forEach((column) => {
      const count = Number(row?.[column.key]);
      const coeff = Number(bonusMap?.[column.key] ?? 0);
      if (!Number.isFinite(count) || count <= 0 || !Number.isFinite(coeff)) return;
      total += Math.floor(count) * coeff;
    });
    return formatLiveNumber(total);
  };

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Live</p>
          <h2>{activeRound ? `Giornata ${activeRound}` : "Giornata corrente"}</h2>
          <p className="muted">
            Inserisci voto base + eventi bonus/malus. Il fantavoto viene calcolato automaticamente.
          </p>
        </div>
      </div>

      <div className="panel">
        <div className="filters inline centered">
          <label className="field">
            <span>Giornata</span>
            <select
              className="select"
              value={activeRound}
              onChange={(e) => onRoundChange(e.target.value)}
              disabled={liveLoading || availableRounds.length === 0}
            >
              {availableRounds.length === 0 ? (
                <option value="">-</option>
              ) : (
                availableRounds.map((roundValue) => (
                  <option key={roundValue} value={String(roundValue)}>
                    {roundValue}
                  </option>
                ))
              )}
            </select>
          </label>
          <button type="button" className="ghost" onClick={onReload} disabled={liveLoading}>
            Aggiorna
          </button>
        </div>

        {liveError ? <p className="error">{liveError}</p> : null}

        <div className="live-fixtures">
          {fixtures.length === 0 ? (
            <p className="muted">Nessuna partita disponibile per questa giornata.</p>
          ) : (
            fixtures.map((fixture) => (
              <div key={fixture.match_id} className="live-fixture-row">
                <div>
                  <strong>
                    {fixture.home_team} - {fixture.away_team}
                  </strong>
                </div>
                <label className="live-switch">
                  <input
                    type="checkbox"
                    checked={Boolean(fixture.six_politico)}
                    onChange={(e) =>
                      onToggleSixPolitico({
                        round: Number(liveData?.round || 0),
                        home_team: fixture.home_team,
                        away_team: fixture.away_team,
                        six_politico: e.target.checked,
                      })
                    }
                    disabled={liveLoading}
                  />
                  <span>6 politico</span>
                </label>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="live-team-grid">
        {teamCards.map((team) => (
          <article key={`${team.match_id}-${team.team}`} className="panel live-team-card">
            <div className="live-team-head">
              <div>
                <h3>{team.team}</h3>
                <p className="muted">
                  {team.home_away === "H" ? "Casa" : "Trasferta"} vs {team.opponent}
                </p>
              </div>
              {team.six_politico ? <span className="badge">6 politico</span> : null}
            </div>

            {team.players.length === 0 ? (
              <p className="muted">Nessun giocatore trovato per questa squadra.</p>
            ) : (
              <div className="live-table-wrap">
                <table className="live-table">
                  <thead>
                    <tr>
                      <th>Giocatore</th>
                      <th>Ruolo</th>
                      <th>V</th>
                      {EVENT_COLUMNS.map((column) => {
                        const bonusValue = Number(bonusMap?.[column.key]);
                        const bonusLabel = Number.isFinite(bonusValue)
                          ? `${bonusValue >= 0 ? "+" : ""}${formatLiveNumber(bonusValue)}`
                          : "";
                        return (
                          <th key={column.key} title={`${column.title}${bonusLabel ? ` (${bonusLabel})` : ""}`}>
                            {column.label}
                          </th>
                        );
                      })}
                      <th>FV</th>
                      <th>SV</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {team.players.map((player) => {
                      const rowKey = makeRowKey(team.team, player.name);
                      const row = edits[rowKey] || { vote: "6", is_sv: false };
                      const rowSaving = liveSavingKey === rowKey;
                      const previewFV = computePreviewFantavote(row, player.fantavote_label || "-");

                      return (
                        <tr key={rowKey}>
                          <td>{player.name}</td>
                          <td>{player.role || "-"}</td>
                          <td>
                            <input
                              className="input live-input"
                              value={row.vote}
                              onChange={(e) => updateEdit(rowKey, { vote: e.target.value })}
                              disabled={team.six_politico || row.is_sv}
                            />
                          </td>
                          {EVENT_COLUMNS.map((column) => (
                            <td key={column.key}>
                              <input
                                className="input live-input live-input-xs"
                                value={row[column.key] || "0"}
                                onChange={(e) => updateEdit(rowKey, { [column.key]: e.target.value })}
                                disabled={team.six_politico || row.is_sv}
                              />
                            </td>
                          ))}
                          <td>
                            <span className="live-fv-preview">{team.six_politico ? player.fantavote_label : previewFV}</span>
                          </td>
                          <td>
                            <input
                              type="checkbox"
                              checked={Boolean(row.is_sv)}
                              onChange={(e) => updateEdit(rowKey, { is_sv: e.target.checked })}
                              disabled={team.six_politico}
                            />
                          </td>
                          <td>
                            <button
                              type="button"
                              className="ghost"
                              onClick={() => {
                                const payload = {
                                  rowKey,
                                  round: Number(liveData?.round || 0),
                                  team: team.team,
                                  player: player.name,
                                  role: player.role || "",
                                  vote: row.vote,
                                  is_sv: Boolean(row.is_sv),
                                };
                                EVENT_COLUMNS.forEach((column) => {
                                  payload[column.key] = row[column.key] || "0";
                                });
                                onSavePlayer(payload);
                              }}
                              disabled={team.six_politico || rowSaving}
                            >
                              {rowSaving ? "Salvo..." : "Salva"}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}
