import { useEffect, useMemo, useState } from "react";

const makeRowKey = (team, player) => `${team}::${player}`;

const normalizeText = (value) => String(value || "").trim().toLowerCase();

const ROLE_ORDER = ["P", "D", "C", "A"];

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

const LIVE_LEGEND = [
  { key: "V", title: "Voto base" },
  { key: "FV", title: "Fantavoto" },
  { key: "X", title: "Non presente (assenza)" },
  { key: "SV", title: "Senza voto" },
  ...EVENT_COLUMNS.map((item) => ({ key: item.label, title: item.title })),
];

const roleRank = (role) => {
  const raw = String(role || "").toUpperCase();
  for (const token of ROLE_ORDER) {
    if (raw.includes(token)) return ROLE_ORDER.indexOf(token);
  }
  return ROLE_ORDER.length;
};

const sortPlayers = (players) => {
  const rows = Array.isArray(players) ? players.slice() : [];
  rows.sort((a, b) => {
    const diff = roleRank(a?.role) - roleRank(b?.role);
    if (diff !== 0) return diff;
    return String(a?.name || "").localeCompare(String(b?.name || ""), "it", {
      sensitivity: "base",
    });
  });
  return rows;
};

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
  liveImporting,
  onReload,
  onImportVotes,
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
      const players = sortPlayers(team?.players);
      players.forEach((player) => {
        const key = makeRowKey(team.team, player.name);
        const payload = {
          vote: toEditNumber(player.vote, "6"),
          is_sv: Boolean(player.is_sv),
          is_absent: Boolean(player.is_absent),
        };
        EVENT_COLUMNS.forEach((column) => {
          const eventValue = player?.events?.[column.key] ?? player?.[column.key] ?? 0;
          payload[column.key] = toEditCount(eventValue);
        });
        next[key] = payload;
      });
    });
    setEdits(next);
  }, [teams, liveData?.round]);

  const teamCards = useMemo(
    () =>
      teams.map((team) => ({
        ...team,
        players: sortPlayers(team?.players),
      })),
    [teams]
  );

  const matchCards = useMemo(() => {
    const grouped = new Map();
    teamCards.forEach((team) => {
      const key = String(
        team?.match_id || `${normalizeText(team?.team)}::${normalizeText(team?.opponent)}`
      );
      const rows = grouped.get(key) || [];
      rows.push(team);
      grouped.set(key, rows);
    });

    const matches = [];
    fixtures.forEach((fixture, index) => {
      const matchKey = String(fixture?.match_id || "");
      const candidates = grouped.get(matchKey) || [];
      const homeTeamName = String(fixture?.home_team || "").trim();
      const awayTeamName = String(fixture?.away_team || "").trim();
      const findTeam = (name) =>
        candidates.find((team) => normalizeText(team?.team) === normalizeText(name));

      const homeTeam = findTeam(homeTeamName) || candidates[0] || null;
      const awayTeam =
        findTeam(awayTeamName) ||
        candidates.find((team) => team !== homeTeam) ||
        candidates[1] ||
        null;

      matches.push({
        match_id: matchKey || `${normalizeText(homeTeamName)}-${normalizeText(awayTeamName)}-${index}`,
        order: index,
        home_team: homeTeamName || homeTeam?.team || "N/D",
        away_team: awayTeamName || awayTeam?.team || "N/D",
        six_politico: Boolean(fixture?.six_politico),
        teams: [homeTeam, awayTeam].filter(Boolean),
      });

      if (matchKey) grouped.delete(matchKey);
    });

    let offset = fixtures.length;
    grouped.forEach((rows, key) => {
      const first = rows[0] || null;
      const second = rows[1] || null;
      matches.push({
        match_id: key,
        order: offset++,
        home_team: first?.team || "N/D",
        away_team: second?.team || first?.opponent || "N/D",
        six_politico: Boolean(first?.six_politico || second?.six_politico),
        teams: [first, second].filter(Boolean),
      });
    });

    matches.sort((a, b) => a.order - b.order);
    return matches;
  }, [fixtures, teamCards]);

  const updateEdit = (key, patch) => {
    setEdits((prev) => {
      const base = prev[key] || { vote: "6", is_sv: false, is_absent: false };
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
    if (row?.is_absent) return "X";
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

  const buildSavePayload = (rowKey, teamName, playerName, role, row) => {
    const payload = {
      rowKey,
      round: Number(liveData?.round || 0),
      team: teamName,
      player: playerName,
      role: role || "",
      vote: row.vote,
      is_sv: Boolean(row.is_sv),
      is_absent: Boolean(row.is_absent),
    };
    EVENT_COLUMNS.forEach((column) => {
      payload[column.key] = row[column.key] || "0";
    });
    return payload;
  };

  return (
    <section className="dashboard">
      <div className="dashboard-header left row">
        <div>
          <p className="eyebrow">Live</p>
          <h2>{activeRound ? `Giornata ${activeRound}` : "Giornata corrente"}</h2>
          <p className="muted">Inserimento rapido voti ed eventi, organizzato per partita.</p>
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
          <button
            type="button"
            className="ghost"
            onClick={onImportVotes}
            disabled={liveLoading || liveImporting || !activeRound}
          >
            {liveImporting ? "Import in corso..." : "Importa voti auto"}
          </button>
        </div>

        <div className="live-legend" aria-label="Legenda live">
          {LIVE_LEGEND.map((item) => (
            <span key={item.key} className="live-legend-item">
              <strong>{item.key}</strong>
              <span>{item.title}</span>
            </span>
          ))}
        </div>

        {liveError ? <p className="error">{liveError}</p> : null}

        <div className="live-match-list">
          {matchCards.length === 0 ? (
            <p className="muted">Nessuna partita disponibile per questa giornata.</p>
          ) : (
            matchCards.map((match) => {
              const columns =
                match.teams.length >= 2
                  ? match.teams.slice(0, 2)
                  : [
                      ...match.teams,
                      {
                        team: match.away_team,
                        opponent: match.home_team,
                        players: [],
                        six_politico: match.six_politico,
                      },
                    ].slice(0, 2);

              return (
                <article key={match.match_id} className="live-match-card">
                  <header className="live-match-head">
                    <strong>
                      {match.home_team} - {match.away_team}
                    </strong>
                    <label className="live-switch compact">
                      <input
                        type="checkbox"
                        checked={Boolean(match.six_politico)}
                        onChange={(e) =>
                          onToggleSixPolitico({
                            round: Number(liveData?.round || 0),
                            home_team: match.home_team,
                            away_team: match.away_team,
                            six_politico: e.target.checked,
                          })
                        }
                        disabled={liveLoading}
                      />
                      <span>6 politico</span>
                    </label>
                  </header>

                  <div className="live-match-columns">
                    {columns.map((team, index) => {
                      const teamName = String(team?.team || "").trim();
                      const players = sortPlayers(team?.players);
                      const isTeamSix = Boolean(match.six_politico || team?.six_politico);
                      return (
                        <section key={`${match.match_id}-${teamName || index}`} className="live-team-column">
                          <h3 className="live-team-title">{teamName || "N/D"}</h3>
                          {players.length === 0 ? (
                            <p className="muted compact">Nessun giocatore disponibile.</p>
                          ) : (
                            <div className="live-player-list">
                              {players.map((player) => {
                                const rowKey = makeRowKey(teamName, player.name);
                                const row = edits[rowKey] || {
                                  vote: "6",
                                  is_sv: false,
                                  is_absent: false,
                                };
                                const rowSaving = liveSavingKey === rowKey;
                                const previewFV = isTeamSix
                                  ? player.fantavote_label
                                  : computePreviewFantavote(row, player.fantavote_label || "-");

                                return (
                                  <div
                                    key={rowKey}
                                    className={`live-player-row${row.is_absent ? " absent" : ""}`}
                                  >
                                    <div className="live-player-main">
                                      <span className="live-player-name" title={player.name}>
                                        {player.name}
                                      </span>
                                      <span className="role-pill tiny">{player.role || "-"}</span>
                                      <input
                                        className="input live-input live-input-compact"
                                        value={row.vote}
                                        onChange={(e) => updateEdit(rowKey, { vote: e.target.value })}
                                        disabled={isTeamSix || row.is_sv || row.is_absent}
                                      />
                                      <span className="live-fv-preview">{previewFV}</span>
                                      <label className="live-absent-toggle" title="Segna non presenza">
                                        <input
                                          type="checkbox"
                                          checked={Boolean(row.is_absent)}
                                          onChange={(e) =>
                                            updateEdit(rowKey, {
                                              is_absent: e.target.checked,
                                              is_sv: e.target.checked ? false : row.is_sv,
                                            })
                                          }
                                          disabled={isTeamSix}
                                        />
                                        <span>X</span>
                                      </label>
                                      <label className="live-sv-toggle">
                                        <input
                                          type="checkbox"
                                          checked={Boolean(row.is_sv)}
                                          onChange={(e) =>
                                            updateEdit(rowKey, {
                                              is_sv: e.target.checked,
                                              is_absent: e.target.checked ? false : row.is_absent,
                                            })
                                          }
                                          disabled={isTeamSix}
                                        />
                                        <span>SV</span>
                                      </label>
                                      <button
                                        type="button"
                                        className="ghost live-save-btn"
                                        onClick={() =>
                                          onSavePlayer(
                                            buildSavePayload(
                                              rowKey,
                                              teamName,
                                              player.name,
                                              player.role,
                                              row
                                            )
                                          )
                                        }
                                        disabled={isTeamSix || rowSaving}
                                      >
                                        {rowSaving ? "..." : "Salva"}
                                      </button>
                                    </div>
                                    <div className="live-player-events">
                                      {EVENT_COLUMNS.map((column) => {
                                        const bonusValue = Number(bonusMap?.[column.key]);
                                        const bonusLabel = Number.isFinite(bonusValue)
                                          ? `${bonusValue >= 0 ? "+" : ""}${formatLiveNumber(bonusValue)}`
                                          : "";
                                        return (
                                          <label
                                            key={`${rowKey}-${column.key}`}
                                            className="live-event-cell"
                                            title={`${column.title}${bonusLabel ? ` (${bonusLabel})` : ""}`}
                                          >
                                            <span>{column.label}</span>
                                            <input
                                              className="input live-input live-input-xs"
                                              value={row[column.key] || "0"}
                                              onChange={(e) =>
                                                updateEdit(rowKey, { [column.key]: e.target.value })
                                              }
                                              disabled={isTeamSix || row.is_sv || row.is_absent}
                                            />
                                          </label>
                                        );
                                      })}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          )}
                        </section>
                      );
                    })}
                  </div>
                </article>
              );
            })
          )}
        </div>
      </div>
    </section>
  );
}
