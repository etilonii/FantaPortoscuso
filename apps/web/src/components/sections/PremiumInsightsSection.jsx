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
  const serieaFinalTable = Array.isArray(insights?.seriea_final_table) ? insights.seriea_final_table : [];

  const rounds = useMemo(() => {
    const values = new Set();
    serieaPredictions.forEach((row) => {
      const round = Number(row?.round);
      if (Number.isFinite(round)) values.add(round);
    });
    return Array.from(values).sort((a, b) => a - b);
  }, [serieaPredictions]);

  const [selectedRound, setSelectedRound] = useState("");

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

  if (mode === "potenza-squadra-titolari") {
    return (
      <ReportSection
        eyebrow="Premium"
        title="Potenza Squadra Titolari"
        description="Ranking dei migliori XI della lega."
        loading={loading}
        error={error}
        onReload={onReload}
        rows={teamStrengthStarting}
        columns={[
          { key: "Pos", label: "Pos", render: (row) => row?.Pos || "-" },
          { key: "Team", label: "Team" },
          {
            key: "ForzaTitolari",
            label: "Forza XI",
            render: (row) => formatNumber(row?.ForzaTitolari, 2),
          },
          { key: "ModuloMigliore", label: "Modulo" },
        ]}
      />
    );
  }

  if (mode === "potenza-squadra-totale") {
    return (
      <ReportSection
        eyebrow="Premium"
        title="Potenza Squadra Totale"
        description="Valutazione complessiva rosa per team."
        loading={loading}
        error={error}
        onReload={onReload}
        rows={teamStrengthTotal}
        columns={[
          { key: "Pos", label: "Pos", render: (row) => row?.Pos || "-" },
          { key: "Team", label: "Team" },
          { key: "ForzaSquadra", label: "Forza", render: (row) => formatNumber(row?.ForzaSquadra, 2) },
          {
            key: "ForzaMediaGiocatore",
            label: "Media",
            render: (row) => formatNumber(row?.ForzaMediaGiocatore, 2),
          },
          { key: "Top", label: "Top", render: (row) => formatNumber(row?.Top, 0) },
          { key: "SemiTop", label: "SemiTop", render: (row) => formatNumber(row?.SemiTop, 0) },
          { key: "Starter", label: "Starter", render: (row) => formatNumber(row?.Starter, 0) },
        ]}
      />
    );
  }

  if (mode === "classifica-potenza") {
    const xiRankMap = new Map();
    teamStrengthStarting.forEach((row) => {
      const team = String(row?.Team || "").trim();
      if (!team) return;
      xiRankMap.set(team.toLowerCase(), row?.Pos || "-");
    });
    const rows = teamStrengthTotal.map((row) => {
      const team = String(row?.Team || "").trim().toLowerCase();
      return {
        ...row,
        xi_pos: xiRankMap.get(team) || "-",
      };
    });
    return (
      <ReportSection
        eyebrow="Premium"
        title="Classifica Potenza"
        description="Confronto ranking rosa totale e XI titolare."
        loading={loading}
        error={error}
        onReload={onReload}
        rows={rows}
        columns={[
          { key: "Pos", label: "Pos Tot", render: (row) => row?.Pos || "-" },
          { key: "Team", label: "Team" },
          { key: "ForzaSquadra", label: "Forza Tot", render: (row) => formatNumber(row?.ForzaSquadra, 2) },
          { key: "xi_pos", label: "Pos XI" },
          {
            key: "ForzaTitolari",
            label: "Forza XI",
            render: (row) => formatNumber(row?.ForzaTitolari, 2),
          },
        ]}
      />
    );
  }

  if (mode === "classifica-reale-lega") {
    const rows = Array.isArray(leagueStandings) ? leagueStandings : [];
    return (
      <ReportSection
        eyebrow="Premium"
        title="Classifica Reale Lega"
        description="Classifica ufficiale FantaPortoscuso."
        loading={false}
        error=""
        onReload={null}
        rows={rows}
        columns={[
          { key: "pos", label: "Pos", render: (row) => row?.pos ?? "-" },
          { key: "team", label: "Team" },
          { key: "points", label: "Pt Tot", render: (row) => formatNumber(row?.points, 2) },
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

  if (mode === "predictions-campionato-fixtures") {
    return (
      <section className="dashboard">
        <div className="dashboard-header left row">
          <div>
            <p className="eyebrow">Premium</p>
            <h2>Predictions Campionato + Fixtures</h2>
            <p className="muted">Proiezione classifica finale Serie A e match previsti per giornata.</p>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Classifica finale proiettata</h3>
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
                  <th>GF</th>
                  <th>GA</th>
                  <th>Diff</th>
                </tr>
              </thead>
              <tbody>
                {serieaFinalTable.map((row, index) => (
                  <tr key={`${row?.squad || row?.team || index}`}>
                    <td>{row?.rank || index + 1}</td>
                    <td>{row?.squad || row?.team || "-"}</td>
                    <td>{formatNumber(row?.projected_pts, 2)}</td>
                    <td>{formatNumber(row?.projected_gf, 2)}</td>
                    <td>{formatNumber(row?.projected_ga, 2)}</td>
                    <td>{formatNumber(row?.projected_gd, 2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header spaced">
            <h3>Predizioni match</h3>
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
                  <th>Marcatori probabili</th>
                  <th>Assist probabili</th>
                  <th>CS</th>
                </tr>
              </thead>
              <tbody>
                {roundPredictions.map((row, index) => (
                  <tr key={`${row?.home_team}-${row?.away_team}-${index}`}>
                    <td>{`${row?.home_team || "-"} - ${row?.away_team || "-"}`}</td>
                    <td>{row?.pred_score || "-"}</td>
                    <td>
                      {String(row?.home_scorers || "").trim() || "-"}
                      {" | "}
                      {String(row?.away_scorers || "").trim() || "-"}
                    </td>
                    <td>
                      {String(row?.home_assists || "").trim() || "-"}
                      {" | "}
                      {String(row?.away_assists || "").trim() || "-"}
                    </td>
                    <td>
                      H {formatPercent(row?.home_clean_sheet_prob)} / A{" "}
                      {formatPercent(row?.away_clean_sheet_prob)}
                    </td>
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
