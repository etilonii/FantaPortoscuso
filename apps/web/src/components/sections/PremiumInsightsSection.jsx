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

const formatPercent = (value) => {
  const parsed = toNumber(value);
  if (parsed === null) return "-";
  return `${(parsed * 100).toFixed(1).replace(".", ",")}%`;
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
  const teamStrengthTotal = Array.isArray(insights?.team_strength_total)
    ? insights.team_strength_total
    : [];
  const teamStrengthStarting = Array.isArray(insights?.team_strength_starting)
    ? insights.team_strength_starting
    : [];
  const serieaTable = Array.isArray(insights?.seriea_current_table) ? insights.seriea_current_table : [];
  const serieaPredictions = Array.isArray(insights?.seriea_predictions) ? insights.seriea_predictions : [];

  const rounds = useMemo(() => {
    const values = new Set();
    serieaPredictions.forEach((row) => {
      const round = Number(row?.round);
      if (Number.isFinite(round)) values.add(round);
    });
    return Array.from(values).sort((a, b) => a - b);
  }, [serieaPredictions]);

  const [selectedRound, setSelectedRound] = useState("");
  const [powerSortBy, setPowerSortBy] = useState("forza_tot");

  useEffect(() => {
    if (!rounds.length) {
      if (selectedRound !== "") setSelectedRound("");
      return;
    }
    const current = Number(selectedRound);
    if (!Number.isFinite(current) || !rounds.includes(current)) {
      setSelectedRound(String(rounds[0]));
    }
  }, [rounds, selectedRound]);

  const currentRound = Number(selectedRound);
  const roundPredictions = useMemo(() => {
    if (!Number.isFinite(currentRound)) return [];
    return serieaPredictions.filter((row) => Number(row?.round) === currentRound);
  }, [serieaPredictions, currentRound]);

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

  if (mode === "classifica-potenza") {
    const startingMap = new Map();
    teamStrengthStarting.forEach((row) => {
      const key = String(row?.Team || "").trim().toLowerCase();
      if (!key) return;
      startingMap.set(key, row);
    });

    const combined = [];
    const seen = new Set();

    teamStrengthTotal.forEach((row) => {
      const key = String(row?.Team || "").trim().toLowerCase();
      if (!key) return;
      const startingRow = startingMap.get(key);
      combined.push({
        Team: row?.Team || startingRow?.Team || "-",
        ForzaSquadra: row?.ForzaSquadra ?? null,
        ForzaMediaGiocatore: row?.ForzaMediaGiocatore ?? null,
        ForzaTitolari: startingRow?.ForzaTitolari ?? null,
      });
      seen.add(key);
    });

    teamStrengthStarting.forEach((row) => {
      const key = String(row?.Team || "").trim().toLowerCase();
      if (!key || seen.has(key)) return;
      combined.push({
        Team: row?.Team || "-",
        ForzaSquadra: null,
        ForzaMediaGiocatore: null,
        ForzaTitolari: row?.ForzaTitolari ?? null,
      });
    });

    const sortValue = (row) => {
      if (powerSortBy === "forza_xi") return toNumber(row?.ForzaTitolari);
      return toNumber(row?.ForzaSquadra);
    };

    const rows = combined
      .slice()
      .sort((a, b) => {
        const av = sortValue(a);
        const bv = sortValue(b);
        const an = av === null ? Number.NEGATIVE_INFINITY : av;
        const bn = bv === null ? Number.NEGATIVE_INFINITY : bv;
        return bn - an;
      })
      .map((row, index) => ({
        ...row,
        Pos: index + 1,
      }));

    return (
      <ReportSection
        eyebrow="Premium"
        title="Classifica Potenza"
        description="Forza totale rosa, media e forza XI (ordinabile)."
        loading={loading}
        error={error}
        onReload={onReload}
        headerControls={
          <div className="filters inline centered">
            <div className="field">
              <span>Ordina per</span>
              <select
                className="select"
                value={powerSortBy}
                onChange={(event) => setPowerSortBy(event.target.value)}
              >
                <option value="forza_tot">Forza Tot</option>
                <option value="forza_xi">Forza XI</option>
              </select>
            </div>
          </div>
        }
        rows={rows}
        columns={[
          { key: "Pos", label: "Pos", render: (row) => row?.Pos || "-" },
          { key: "Team", label: "Team" },
          { key: "ForzaSquadra", label: "Forza Tot", render: (row) => formatNumber(row?.ForzaSquadra, 2) },
          {
            key: "ForzaMediaGiocatore",
            label: "Media",
            render: (row) => formatNumber(row?.ForzaMediaGiocatore, 2),
          },
          { key: "ForzaTitolari", label: "Forza XI", render: (row) => formatNumber(row?.ForzaTitolari, 2) },
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
    return (
      <section className="dashboard">
        <div className="dashboard-header left row">
          <div>
            <p className="eyebrow">Premium</p>
            <h2>Classifica + Fixtures Serie A</h2>
            <p className="muted">Classifica corrente Serie A e fixtures previste per giornata.</p>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Classifica corrente</h3>
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
                  <th>Ultimi 5</th>
                  <th>Pt</th>
                  <th>PG</th>
                  <th>GF</th>
                  <th>GA</th>
                  <th>Diff</th>
                </tr>
              </thead>
              <tbody>
                {serieaTable.map((row, index) => (
                  <tr key={`${row?.Squad || row?.Team || index}`}>
                    <td>{index + 1}</td>
                    <td>{row?.Squad || row?.Team || "-"}</td>
                    <td>{String(row?.Last5 ?? row?.["Last5"] ?? row?.["Last 5"] ?? "").trim() || "-"}</td>
                    <td>{row?.Pts || "-"}</td>
                    <td>{row?.MP || "-"}</td>
                    <td>{row?.GF || "-"}</td>
                    <td>{row?.GA || "-"}</td>
                    <td>{row?.GD || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Fixtures previste</h3>
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
          </div>
          <div className="report-table-wrap">
            <table className="report-table">
              <thead>
                <tr>
                  <th>Match</th>
                  <th>Pred</th>
                  <th>1</th>
                  <th>X</th>
                  <th>2</th>
                  <th>xG</th>
                </tr>
              </thead>
              <tbody>
                {roundPredictions.map((row, index) => (
                  <tr key={`${row?.home_team}-${row?.away_team}-${index}`}>
                    <td>{`${row?.home_team || "-"} - ${row?.away_team || "-"}`}</td>
                    <td>{row?.pred_score || "-"}</td>
                    <td>{formatPercent(row?.home_win_prob)}</td>
                    <td>{formatPercent(row?.draw_prob)}</td>
                    <td>{formatPercent(row?.away_win_prob)}</td>
                    <td>{`${formatNumber(row?.home_xg, 2)} - ${formatNumber(row?.away_xg, 2)}`}</td>
                  </tr>
                ))}
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
