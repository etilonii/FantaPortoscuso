import { useEffect, useMemo, useState } from "react";
import ReportSection from "./ReportSection";

const toNumber = (value) => {
  const parsed = Number(String(value ?? "").replace(",", "."));
  return Number.isFinite(parsed) ? parsed : null;
};

const formatNumber = (value, digits = 2) => {
  const parsed = toNumber(value);
  if (parsed === null) return "-";
  return parsed.toFixed(digits).replace(".", ",");
};

const formatSignedNumber = (value, digits = 2) => {
  const parsed = toNumber(value);
  if (parsed === null) return "-";
  const abs = Math.abs(parsed).toFixed(digits).replace(".", ",");
  const sign = parsed > 0 ? "+" : parsed < 0 ? "-" : "";
  return `${sign}${abs}`;
};

const resolveLiveDelta = (row) => {
  const explicitLive = toNumber(row?.live_total);
  if (explicitLive !== null) return explicitLive;
  const pointsLive = toNumber(row?.points_live ?? row?.points);
  const pointsBase = toNumber(row?.points_base);
  if (pointsLive === null || pointsBase === null) return null;
  return pointsLive - pointsBase;
};

const resolvePositionDelta = (row) => {
  const livePos = toNumber(row?.live_pos ?? row?.pos);
  const basePos = toNumber(row?.base_pos ?? row?.played_base_pos ?? row?.pos);
  if (!Number.isFinite(livePos) || !Number.isFinite(basePos) || livePos <= 0 || basePos <= 0) {
    return null;
  }
  return Math.trunc(basePos) - Math.trunc(livePos);
};

const buildLiveTrendMeta = (liveDelta, average) => {
  const value = toNumber(liveDelta);
  if (value === null) {
    return { tierClass: "neutral", arrow: "→", pctDiff: null };
  }
  const safeAverage = toNumber(average);
  if (safeAverage === null || Math.abs(safeAverage) < 0.0001) {
    return { tierClass: "neutral", arrow: value >= 0 ? "↑" : "↓", pctDiff: null };
  }

  const pctDiff = ((value - safeAverage) / Math.abs(safeAverage)) * 100;
  let tierClass = "neutral";
  if (pctDiff <= -25) tierClass = "down-5";
  else if (pctDiff <= -20) tierClass = "down-4";
  else if (pctDiff <= -15) tierClass = "down-3";
  else if (pctDiff <= -10) tierClass = "down-2";
  else if (pctDiff < -5) tierClass = "down-1";
  else if (pctDiff >= 25) tierClass = "up-5";
  else if (pctDiff >= 20) tierClass = "up-4";
  else if (pctDiff >= 15) tierClass = "up-3";
  else if (pctDiff >= 10) tierClass = "up-2";
  else if (pctDiff > 5) tierClass = "up-1";

  return {
    tierClass,
    arrow: value > 0 ? "↑" : value < 0 ? "↓" : "→",
    pctDiff,
  };
};

const buildPositionTrendMeta = (positionDelta) => {
  const value = toNumber(positionDelta);
  if (value === null || !Number.isFinite(value)) {
    return { tierClass: "neutral", arrow: "-", value: 0 };
  }
  if (value > 0) {
    return { tierClass: "up", arrow: "↑", value: Math.trunc(value) };
  }
  if (value < 0) {
    return { tierClass: "down", arrow: "↓", value: Math.trunc(value) };
  }
  return { tierClass: "neutral", arrow: "-", value: 0 };
};

const normalizeFormToken = (token) => {
  const value = String(token ?? "").trim().toUpperCase();
  if (!value) return "";
  const first = value[0];
  if (first === "W" || first === "V") return "W";
  if (first === "D" || first === "N" || first === "X") return "D";
  if (first === "L" || first === "P" || first === "S") return "L";
  return "";
};

const parseLastFiveResults = (value) => {
  const raw = String(value ?? "").trim();
  if (!raw) return [];
  const spacedTokens = raw
    .split(/[\s,;|/]+/)
    .map((item) => normalizeFormToken(item))
    .filter(Boolean);
  if (spacedTokens.length > 0) {
    return spacedTokens.slice(-5);
  }
  return (raw.match(/[A-Za-z]/g) || [])
    .map((item) => normalizeFormToken(item))
    .filter(Boolean)
    .slice(-5);
};

export default function PremiumInsightsSection({
  mode,
  insights,
  loading,
  error,
  onReload,
  leagueStandings,
  openPlayer,
}) {
  const playerTiers = Array.isArray(insights?.player_tiers) ? insights.player_tiers : [];
  const serieaTable = Array.isArray(insights?.seriea_current_table) ? insights.seriea_current_table : [];
  const serieaLiveTable = Array.isArray(insights?.seriea_live_table) ? insights.seriea_live_table : [];
  const serieaFixtures = Array.isArray(insights?.seriea_fixtures) ? insights.seriea_fixtures : [];
  const serieaRoundValue = Number(insights?.seriea_round);
  const serieaRounds = Array.isArray(insights?.seriea_rounds) ? insights.seriea_rounds : [];

  const rounds = useMemo(() => {
    const values = new Set();
    serieaRounds.forEach((row) => {
      const round = Number(
        typeof row === "object" && row !== null ? row.round : row
      );
      if (Number.isFinite(round)) values.add(round);
    });
    serieaFixtures.forEach((row) => {
      const round = Number(row?.round);
      if (Number.isFinite(round)) values.add(round);
    });
    if (Number.isFinite(serieaRoundValue)) values.add(serieaRoundValue);
    return Array.from(values).sort((a, b) => a - b);
  }, [serieaRounds, serieaFixtures, serieaRoundValue]);

  const [selectedRound, setSelectedRound] = useState("");
  useEffect(() => {
    if (!rounds.length) {
      if (selectedRound !== "") setSelectedRound("");
      return;
    }
    const current = Number(selectedRound);
    if (!Number.isFinite(current) || !rounds.includes(current)) {
      const preferred = Number.isFinite(serieaRoundValue) && rounds.includes(serieaRoundValue)
        ? serieaRoundValue
        : rounds[0];
      setSelectedRound(String(preferred));
    }
  }, [rounds, selectedRound, serieaRoundValue]);

  const currentRound = Number(selectedRound);
  const activeSerieaFixtures = useMemo(() => {
    if (!Number.isFinite(currentRound)) return serieaFixtures;
    return serieaFixtures.filter((row) => Number(row?.round) === currentRound);
  }, [serieaFixtures, currentRound]);

  if (mode === "tier-list") {
    const rows = playerTiers.slice(0, 220);
    return (
      <ReportSection
        eyebrow="Premium"
        title="Tier List"
        description="Classifica calcolata automaticamente dai dati locali."
        loading={loading}
        error={error}
        onReload={onReload}
        rows={rows}
        columns={[
          { key: "rank", label: "#", render: (_row, index) => `#${index + 1}` },
          {
            key: "name",
            label: "Giocatore",
            render: (row) => (
              <button type="button" className="link-button" onClick={() => openPlayer?.(row?.name)}>
                {row?.name || "-"}
              </button>
            ),
          },
          { key: "team", label: "Squadra" },
          { key: "role", label: "Ruolo" },
          { key: "tier", label: "Tier" },
          { key: "score_auto", label: "Score", render: (row) => formatNumber(row?.score_auto, 3) },
          { key: "weight", label: "Weight", render: (row) => formatNumber(row?.weight, 3) },
          { key: "partite", label: "PG", render: (row) => formatNumber(row?.partite, 0) },
        ]}
      />
    );
  }

  if (mode === "classifica-lega") {
    const rows = Array.isArray(leagueStandings) ? leagueStandings : [];
    const liveValues = rows
      .map((row) => resolveLiveDelta(row))
      .filter((value) => Number.isFinite(value));
    const liveAverage =
      liveValues.length > 0
        ? liveValues.reduce((sum, value) => sum + Number(value), 0) / liveValues.length
        : null;
    return (
      <ReportSection
        eyebrow="Lega"
        title="Classifica Lega"
        description="Classifica ufficiale FantaPortoscuso."
        loading={false}
        error=""
        onReload={null}
        rows={rows}
        columns={[
          { key: "pos", label: "Pos", render: (row) => row?.pos ?? "-" },
          { key: "team", label: "Team" },
          { key: "points", label: "Pt Tot", render: (row) => formatNumber(row?.points, 2) },
          {
            key: "live_delta",
            label: "Live Δ",
            render: (row) => {
              const delta = resolveLiveDelta(row);
              if (!Number.isFinite(delta)) return "-";
              const trend = buildLiveTrendMeta(delta, liveAverage);
              const averageLabel = formatNumber(liveAverage, 2);
              const diffLabel =
                trend.pctDiff === null ? "-" : `${formatSignedNumber(trend.pctDiff, 1)}%`;
              return (
                <span
                  className={`live-trend-pill ${trend.tierClass}`}
                  title={`Live ${formatSignedNumber(delta, 2)} | Media giornata ${averageLabel} | Scarto ${diffLabel}`}
                >
                  <span className="live-trend-arrow">{trend.arrow}</span>
                  <span className="live-trend-value">{formatSignedNumber(delta, 2)}</span>
                </span>
              );
            },
          },
          {
            key: "position_delta",
            label: "Pos Δ",
            render: (row) => {
              const delta = resolvePositionDelta(row);
              const trend = buildPositionTrendMeta(delta);
              const basePos = toNumber(row?.base_pos);
              const livePos = toNumber(row?.live_pos ?? row?.pos);
              const title =
                Number.isFinite(basePos) && Number.isFinite(livePos)
                  ? `Posizione ${Math.trunc(basePos)} → ${Math.trunc(livePos)}`
                  : "Variazione posizione live";
              const signed = trend.value > 0 ? `+${trend.value}` : String(trend.value);
              return (
                <span className={`position-trend-pill ${trend.tierClass}`} title={title}>
                  <span className="position-trend-arrow">{trend.arrow}</span>
                  <span className="position-trend-value">{signed}</span>
                </span>
              );
            },
          },
          { key: "played", label: "PG", render: (row) => formatNumber(row?.played, 0) },
          { key: "pts_avg", label: "Media", render: (row) => formatNumber(row?.pts_avg, 2) },
        ]}
      />
    );
  }

  if (mode === "classifica-fixtures-seriea") {
    const liveRows = Array.isArray(serieaLiveTable) ? serieaLiveTable : [];
    const last5ByTeam = new Map();
    serieaTable.forEach((row) => {
      const key = String(row?.Squad || row?.Team || row?.team || "").trim().toLowerCase();
      if (!key) return;
      const value = String(row?.Last5 || row?.last5 || "").trim();
      if (value) last5ByTeam.set(key, value);
    });
    const selectedRoundLabel =
      Number.isFinite(currentRound) && currentRound > 0
        ? currentRound
        : Number.isFinite(serieaRoundValue) && serieaRoundValue > 0
        ? serieaRoundValue
        : null;
    const fixtureRows = activeSerieaFixtures;
    const fixtureStateLabel = (stateValue) => {
      const value = String(stateValue || "").trim().toLowerCase();
      if (value === "live") return "Live";
      if (value === "finished") return "Finale";
      return "Programmata";
    };
    const fixtureStateClass = (stateValue) => {
      const value = String(stateValue || "").trim().toLowerCase();
      if (value === "live") return "running";
      if (value === "finished") return "ok";
      return "pending";
    };

    return (
      <section className="dashboard">
        <div className="dashboard-header left row">
          <div>
            <p className="eyebrow">Premium</p>
            <h2>Serie A</h2>
            <p className="muted">
              Classifica e fixtures Serie A live
              {selectedRoundLabel ? ` (Giornata ${selectedRoundLabel})` : ""}.
            </p>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Classifica live Serie A</h3>
            {typeof onReload === "function" ? (
              <button className="ghost" type="button" onClick={onReload} disabled={Boolean(loading)}>
                {loading ? "Aggiorno..." : "Aggiorna"}
              </button>
            ) : null}
          </div>
          {error ? <p className="error">{error}</p> : null}
          <div className="report-table-wrap">
            <table className="report-table">
              <thead>
                <tr>
                  <th>Pos</th>
                  <th>Squadra</th>
                  <th>Pt</th>
                  <th>Last 5</th>
                  <th>Pos Î”</th>
                  <th>PG</th>
                  <th>GF</th>
                  <th>GA</th>
                  <th>Diff</th>
                </tr>
              </thead>
              <tbody>
                {(liveRows.length > 0 ? liveRows : serieaTable).map((row, index) => {
                  const posDelta = resolvePositionDelta(row);
                  const posTrend = buildPositionTrendMeta(posDelta);
                  const posValue = toNumber(row?.live_pos ?? row?.Pos ?? row?.pos);
                  const teamName = row?.team || row?.Squad || row?.Team || "-";
                  const teamKey = String(teamName || "").trim().toLowerCase();
                  const formRaw = row?.last5 ?? row?.Last5 ?? last5ByTeam.get(teamKey) ?? "";
                  const formTokens = parseLastFiveResults(formRaw);
                  const latestFormIndex = formTokens.length - 1;
                  const pointsValue = toNumber(row?.points_live ?? row?.Pts ?? row?.points);
                  const playedValue = toNumber(row?.played_live ?? row?.MP ?? row?.played);
                  const gfValue = toNumber(row?.gf_live ?? row?.GF);
                  const gaValue = toNumber(row?.ga_live ?? row?.GA);
                  const gdValue =
                    toNumber(row?.gd_live) ??
                    (Number.isFinite(gfValue) && Number.isFinite(gaValue) ? gfValue - gaValue : toNumber(row?.GD));

                  return (
                    <tr key={`${teamName}-${index}`}>
                      <td>{Number.isFinite(posValue) ? Math.trunc(posValue) : index + 1}</td>
                      <td>{teamName}</td>
                      <td>{Number.isFinite(pointsValue) ? formatNumber(pointsValue, 0) : "-"}</td>
                      <td>
                        {formTokens.length > 0 ? (
                          <span className="last5-track" title={`Ultime 5: ${formTokens.join(" ")}`}>
                            {formTokens.map((token, tokenIndex) => {
                              const tone = token === "W" ? "win" : token === "D" ? "draw" : "loss";
                              const latestClass = tokenIndex === latestFormIndex ? "latest" : "";
                              return (
                                <span
                                  key={`${teamName}-last5-${tokenIndex}`}
                                  className={`last5-pill ${tone} ${latestClass}`.trim()}
                                >
                                  {token}
                                </span>
                              );
                            })}
                          </span>
                        ) : (
                          <span className="muted">-</span>
                        )}
                      </td>
                      <td>
                        <span className={`position-trend-pill ${posTrend.tierClass}`}>
                          <span className="position-trend-arrow">{posTrend.arrow}</span>
                          <span className="position-trend-value">
                            {posTrend.value > 0 ? `+${posTrend.value}` : String(posTrend.value)}
                          </span>
                        </span>
                      </td>
                      <td>{Number.isFinite(playedValue) ? Math.trunc(playedValue) : "-"}</td>
                      <td>{Number.isFinite(gfValue) ? Math.trunc(gfValue) : "-"}</td>
                      <td>{Number.isFinite(gaValue) ? Math.trunc(gaValue) : "-"}</td>
                      <td>{Number.isFinite(gdValue) ? Math.trunc(gdValue) : "-"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Fixtures Serie A</h3>
            {rounds.length > 1 ? (
              <label className="field inline-field">
                <span>Giornata</span>
                <select
                  className="select"
                  value={selectedRound}
                  onChange={(event) => setSelectedRound(event.target.value)}
                >
                  {rounds.map((round) => (
                    <option key={round} value={String(round)}>
                      {round}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
          </div>
          <div className="report-table-wrap">
            <table className="report-table">
              <thead>
                <tr>
                  <th>Match</th>
                  <th>Ris</th>
                  <th>Stato</th>
                  <th>Data/Ora</th>
                </tr>
              </thead>
              <tbody>
                {fixtureRows.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="muted">
                      Nessuna fixture Serie A disponibile.
                    </td>
                  </tr>
                ) : (
                  fixtureRows.map((row, index) => {
                    const state = String(row?.state || "").toLowerCase();
                    const hs = toNumber(row?.home_score);
                    const as = toNumber(row?.away_score);
                    const hasScore = Number.isFinite(hs) && Number.isFinite(as);
                    const showScore = hasScore && !(state === "scheduled" && hs === 0 && as === 0);
                    const scoreLabel = showScore ? `${Math.trunc(hs)} - ${Math.trunc(as)}` : "-";
                    const kickoff = String(row?.kickoff_iso || "").trim();

                    return (
                      <tr key={`${row?.home_team}-${row?.away_team}-${index}`}>
                        <td>{`${row?.home_team || "-"} - ${row?.away_team || "-"}`}</td>
                        <td>{scoreLabel}</td>
                        <td>
                          <span className={`status-badge compact ${fixtureStateClass(state)}`}>
                            {fixtureStateLabel(state)}
                          </span>
                        </td>
                        <td>{kickoff || "-"}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    );
  }

  return (
    <ReportSection
      eyebrow="Premium"
      title="Insight"
      description="Sezione non disponibile."
      loading={false}
      error=""
      onReload={null}
      rows={[]}
      columns={[]}
    />
  );
}
