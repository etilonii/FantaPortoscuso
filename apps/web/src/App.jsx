import { useEffect, useMemo, useState } from "react";
import { useMarketPlaceholder } from "./hooks/useMarketPlaceholder";
import ListoneSection from "./components/sections/ListoneSection";
import HomeSection from "./components/sections/HomeSection";
import MercatoSection from "./components/sections/MercatoSection";
import PlusvalenzeSection from "./components/sections/PlusvalenzeSection";
import RoseSection from "./components/sections/RoseSection";
import StatsSection from "./components/sections/StatsSection";
import TopAcquistiSection from "./components/sections/TopAcquistiSection";

/* ===========================
   DEVICE ID
=========================== */
const getDeviceId = () => {
  const key = "fp_device_id";
  try {
    const existing = localStorage.getItem(key);
    if (existing) return existing;
  } catch {}

  let id = "";
  try {
    if (globalThis.crypto?.randomUUID) {
      id = globalThis.crypto.randomUUID();
    }
  } catch {}

  if (!id) {
    id = `fp-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 10)}`;
  }

  try {
    localStorage.setItem(key, id);
  } catch {}

  return id;
};

/* ===========================
   HELPERS
=========================== */
const normalizeName = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const slugify = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const expandTeamName = (value, teamSet) => {
  const raw = String(value || "").trim();
  if (!raw || !teamSet || teamSet.size === 0) return raw;
  const rawNorm = normalizeName(raw);
  if (!rawNorm) return raw;

  for (const team of teamSet) {
    if (normalizeName(team) === rawNorm) return team;
  }

  if (rawNorm.length <= 4) {
    const matches = [];
    for (const team of teamSet) {
      if (normalizeName(team).startsWith(rawNorm)) matches.push(team);
    }
    if (matches.length === 1) return matches[0];
  }

  return raw;
};

const formatInt = (value) => {
  if (value === undefined || value === null || value === "") return "-";
  const n = Number(value);
  return Number.isNaN(n) ? "-" : Math.round(n).toString();
};

const formatDecimal = (value, digits = 2) => {
  if (value === undefined || value === null) return "-";
  const n = Number(value);
  if (Number.isNaN(n)) return "-";
  return n.toFixed(digits).replace(".", ",");
};

const formatLastAccess = (value) => {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "-";
  const time = d.toLocaleTimeString("it-IT", {
    timeZone: "Europe/Rome",
    hour: "2-digit",
    minute: "2-digit",
  });
  const date = d.toLocaleDateString("it-IT", {
    timeZone: "Europe/Rome",
    day: "2-digit",
    month: "2-digit",
    year: "2-digit",
  });
  return `${time} ${date}`;
};

/* ===========================
   CONFIG
=========================== */
const KEY_STORAGE = "fp_access_key";
const API_BASE =
  import.meta.env.VITE_API_BASE || "http://localhost:8001";

/* ===========================
   APP
=========================== */
export default function App() {
  const deviceId = useMemo(() => getDeviceId(), []);

  /* ===== AUTH ===== */
  const [accessKey, setAccessKey] = useState("");
  const [rememberKey, setRememberKey] = useState(false);
  const [loggedIn, setLoggedIn] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [status, setStatus] = useState("idle");
  const [error, setError] = useState("");

  /* ===== UI ===== */
  const [theme, setTheme] = useState("dark");
  const [activeMenu, setActiveMenu] = useState("home");

  /* ===== DASHBOARD ===== */
  const [summary, setSummary] = useState({ teams: 0, players: 0 });

  /* ===== SEARCH ===== */
  const [activeTab, setActiveTab] = useState("rose");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [expandedRose, setExpandedRose] = useState(new Set());
  const [hasSearched, setHasSearched] = useState(false);

  const aggregatedRoseResults = useMemo(() => {
    if (activeTab !== "rose" || !results?.length) return [];
    const map = new Map();
    results.forEach((row) => {
      const player = String(row.Giocatore || row.nome || "").trim();
      if (!player) return;
      const key = player.toLowerCase();
      const entry = map.get(key) || { name: player, teams: new Set() };
      const teamName = String(row.Team || row.team || "").trim();
      if (teamName) {
        entry.teams.add(teamName);
      }
      map.set(key, entry);
    });
    return Array.from(map.values()).slice(0, 30).map((entry) => {
      const teamsAll = Array.from(entry.teams);
      return {
        name: entry.name,
        teamsAll,
        teamsList: teamsAll.slice(0, 10),
        teamCount: entry.teams.size,
        hasMore: teamsAll.length > 10,
      };
    });
  }, [results, activeTab]);

  const quoteSearchResults = useMemo(() => {
    if (activeTab !== "quotazioni" || !results?.length) return [];
    return results.slice(0, 30).map((row) => ({
      name: String(row.Giocatore || row.nome || "").trim(),
      role: String(row.Ruolo || row.ruolo || row.role || "").trim().toUpperCase(),
      team: String(row.Squadra || row.club || row.team || "").trim(),
      price:
        row.PrezzoAttuale ??
        row.QuotazioneAttuale ??
        row.Prezzo ??
        row.QA ??
        row.PrezzoAcquisto ??
        "-",
    }));
  }, [results, activeTab]);

  /* ===== STATS ===== */
  const [statsTab, setStatsTab] = useState("gol");
  const [statsItems, setStatsItems] = useState([]);
  const [statsQuery, setStatsQuery] = useState("");
  const [topTab, setTopTab] = useState("quotazioni");

  /* ===== TEAMS / ROSE ===== */
  const [teams, setTeams] = useState([]);
  const [selectedTeam, setSelectedTeam] = useState("");
  const [roster, setRoster] = useState([]);
  const [roleFilter, setRoleFilter] = useState("all");
  const [squadraFilter, setSquadraFilter] = useState("all");
  const [rosterQuery, setRosterQuery] = useState("");

  /* ===== QUOTAZIONI ===== */
  const [quoteRole, setQuoteRole] = useState("P");
  const [quoteOrder, setQuoteOrder] = useState("price_desc");
  const [quoteTeam, setQuoteTeam] = useState("all");
  const [quoteList, setQuoteList] = useState([]);
  const [topQuotesAll, setTopQuotesAll] = useState([]);
  const [listoneQuery, setListoneQuery] = useState("");

  /* ===== PLUSVALENZE ===== */
  const [plusvalenze, setPlusvalenze] = useState([]);
  const [allPlusvalenze, setAllPlusvalenze] = useState([]);
  const [plusvalenzePeriod, setPlusvalenzePeriod] = useState("december");
  const [plusvalenzeQuery, setPlusvalenzeQuery] = useState("");

  /* ===== TOP ACQUISTI ===== */
  const [topPlayersByRole, setTopPlayersByRole] = useState({
    P: [],
    D: [],
    C: [],
    A: [],
  });
  const [activeTopRole, setActiveTopRole] = useState("P");
  const [topAcquistiQuery, setTopAcquistiQuery] = useState("");

  /* ===== PLAYER ===== */
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [playerProfile, setPlayerProfile] = useState(null);
  const [playerStats, setPlayerStats] = useState(null);
  const [playerTeamCount, setPlayerTeamCount] = useState(0);

  /* ===== ADMIN ===== */
  const [adminKeys, setAdminKeys] = useState([]);
  const [newKey, setNewKey] = useState("");
  const [adminNotice, setAdminNotice] = useState("");
  const [adminSetAdminKey, setAdminSetAdminKey] = useState("");
  const [adminTeamKey, setAdminTeamKey] = useState("");
  const [adminTeamName, setAdminTeamName] = useState("");
  const [adminResetKey, setAdminResetKey] = useState("");
  const [adminImportKeys, setAdminImportKeys] = useState("");
  const [adminImportIsAdmin, setAdminImportIsAdmin] = useState(false);
  const [adminImportTeamKeys, setAdminImportTeamKeys] = useState("");
  const [adminStatus, setAdminStatus] = useState(null);
  const [adminTeamKeys, setAdminTeamKeys] = useState([]);

  /* ===== MERCATO + SUGGEST ===== */
  const [marketView, setMarketView] = useState("players");

  const {
    marketCountdown,
    marketItems,
    marketTeams,
    marketPreview,
    setMarketPreview,
    marketUpdatedAt,
    loadMarket,
  } = useMarketPlaceholder(API_BASE, loggedIn);

const [suggestPayload, setSuggestPayload] = useState(null);
const [suggestTeam, setSuggestTeam] = useState("");
const [suggestions, setSuggestions] = useState([]);
const [suggestError, setSuggestError] = useState("");
const [suggestLoading, setSuggestLoading] = useState(false);
const [suggestHasRun, setSuggestHasRun] = useState(false);

const [manualOuts, setManualOuts] = useState([]);
const [manualResult, setManualResult] = useState(null);
const [manualError, setManualError] = useState("");
const [manualLoading, setManualLoading] = useState(false);
const [manualDislikes, setManualDislikes] = useState(new Set());
const [manualExcludedIns, setManualExcludedIns] = useState(new Set());

  /* ===========================
     THEME
  =========================== */
  const toggleTheme = () => {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    try {
      localStorage.setItem("fp_theme", next);
    } catch {}
    document.body.classList.toggle("theme-light", next === "light");
  };

  /* ===========================
     LOGIN
  =========================== */
  const handleLogin = async () => {
    if (!accessKey.trim()) {
      setError("Inserisci una key valida");
      return;
    }

    setStatus("loading");
    setError("");

    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          key: accessKey.trim(),
          device_id: deviceId,
        }),
      });

      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || "Accesso non consentito");
      }

      const data = await res.json();
      setLoggedIn(true);
      setIsAdmin(!!data.is_admin);
      setStatus("success");

      if (rememberKey) {
        localStorage.setItem(
          KEY_STORAGE,
          accessKey.trim().toUpperCase()
        );
      } else {
        localStorage.removeItem(KEY_STORAGE);
      }
    } catch (err) {
      setStatus("error");
      setError(err.message || "Errore login");
    }
  };
  /* ===========================
     LOADERS (FETCH)
  =========================== */
  const loadSummary = async () => {
    try {
      const res = await fetch(`${API_BASE}/data/summary`);
      if (!res.ok) return;
      const data = await res.json();
      setSummary(data);
    } catch {}
  };

  const loadTeams = async () => {
    try {
      const res = await fetch(`${API_BASE}/data/teams`);
      if (!res.ok) return;
      const data = await res.json();
      const items = (data.items || [])
        .slice()
        .sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" }));
      setTeams(items);
      if (items.length && !selectedTeam) setSelectedTeam(items[0]);
    } catch {}
  };

  const loadRoster = async (teamName) => {
    if (!teamName) return;
    try {
      const res = await fetch(
        `${API_BASE}/data/team/${encodeURIComponent(teamName)}`
      );
      if (!res.ok) return;
      const data = await res.json();
      setRoster(data.items || []);
    } catch {}
  };

  const loadListone = async () => {
    try {
      const res = await fetch(
        `${API_BASE}/data/listone?ruolo=${encodeURIComponent(
          quoteRole
        )}&order=${encodeURIComponent(quoteOrder)}&limit=200`
      );
      if (!res.ok) return;
      const data = await res.json();
      setQuoteList(data.items || []);
    } catch {}
  };

  const loadTopQuotesAllRoles = async () => {
    try {
      const roles = ["P", "D", "C", "A"];
      const responses = await Promise.all(
        roles.map((role) =>
          fetch(
            `${API_BASE}/data/listone?ruolo=${encodeURIComponent(
              role
            )}&order=price_desc&limit=200`
          )
        )
      );
      const items = [];
      for (const res of responses) {
        if (!res.ok) continue;
        const data = await res.json();
        items.push(...(data.items || []));
      }
      items.sort(
        (a, b) => Number(b.PrezzoAttuale || 0) - Number(a.PrezzoAttuale || 0)
      );
      setTopQuotesAll(items);
    } catch {}
  };

  const teamNameSet = useMemo(() => {
    const set = new Set();
    const source = topQuotesAll.length ? topQuotesAll : quoteList;
    source.forEach((item) => {
      const team = String(item?.Squadra || "").trim();
      if (team) set.add(team);
    });
    return set;
  }, [topQuotesAll, quoteList]);

  const rosterDisplay = useMemo(
    () =>
      (roster || []).map((it) => ({
        ...it,
        Squadra: expandTeamName(it.Squadra, teamNameSet),
      })),
    [roster, teamNameSet]
  );

  const loadPlusvalenze = async () => {
    try {
      const res = await fetch(
        `${API_BASE}/data/stats/plusvalenze?limit=5&include_negatives=false&period=${encodeURIComponent(
          plusvalenzePeriod
        )}`
      );
      if (!res.ok) return;
      const data = await res.json();
      setPlusvalenze(data.items || []);
    } catch {}
  };

  const loadAllPlusvalenze = async () => {
    try {
      const res = await fetch(
        `${API_BASE}/data/stats/plusvalenze?limit=200&include_negatives=true&period=${encodeURIComponent(
          plusvalenzePeriod
        )}`
      );
      if (!res.ok) return;
      const data = await res.json();
      setAllPlusvalenze(data.items || []);
    } catch {}
  };

  const tabToColumn = (tab) => {
    const map = {
      gol: "Gol",
      assist: "Assist",
      ammonizioni: "Ammonizioni",
      espulsioni: "Espulsioni",
      cleansheet: "Cleansheet",
    };
    return map[tab] || tab;
  };

  const statColumn = useMemo(() => tabToColumn(statsTab), [statsTab]);

  const rankedStatsItems = useMemo(
    () =>
      (statsItems || []).map((item, index) => ({
        ...item,
        rank: index + 1,
      })),
    [statsItems]
  );

  const filteredStatsItems = useMemo(() => {
    const q = String(statsQuery || "").trim().toLowerCase();
    if (!q) return rankedStatsItems;
    return (rankedStatsItems || []).filter((item) =>
      String(item.Giocatore || "")
        .toLowerCase()
        .includes(q)
    );
  }, [rankedStatsItems, statsQuery]);

  const loadStatList = async (tab) => {
    try {
      const res = await fetch(
        `${API_BASE}/data/stats/${encodeURIComponent(tab)}?limit=300`
      );
      if (!res.ok) return;
      const data = await res.json();
      setStatsItems(data.items || []);
    } catch {}
  };

  const runSearch = async (searchValue) => {
    const value = String(searchValue ?? query).trim();
    if (!value) {
      setResults([]);
      setHasSearched(false);
      return;
    }

    try {
      setHasSearched(true);
      if (activeTab === "quotazioni") {
        const [quotRes, playersRes] = await Promise.all([
          fetch(`${API_BASE}/data/quotazioni?q=${encodeURIComponent(value)}`),
          fetch(`${API_BASE}/data/players?q=${encodeURIComponent(value)}&limit=200`),
        ]);
        if (!quotRes.ok) {
          setResults([]);
          return;
        }
        const quotData = await quotRes.json();
        const quotItems = quotData.items || [];
        let playerMap = new Map();
        if (playersRes.ok) {
          const playersData = await playersRes.json();
          (playersData.items || []).forEach((row) => {
            const name = String(row.Giocatore || "").trim();
            if (!name || playerMap.has(name)) return;
            playerMap.set(name, row);
          });
        }
        const merged = quotItems.map((row) => {
          const name = String(row.Giocatore || row.nome || "").trim();
          const meta = playerMap.get(name) || {};
          return {
            ...row,
            Ruolo: row.Ruolo || meta.Ruolo,
            Squadra: row.Squadra || meta.Squadra,
          };
        });
        setResults(merged);
        return;
      }

      const res = await fetch(`${API_BASE}/data/players?q=${encodeURIComponent(value)}`);
      if (!res.ok) {
        setResults([]);
        return;
      }
      const data = await res.json();
      setResults(data.items || []);
    } catch {
      setHasSearched(true);
      setResults([]);
    }
  };

  /* ===========================
     TOP PLAYERS ACQUISTATI (AGGREGATI)
     - calcola quante squadre possiedono ogni giocatore per ruolo
  =========================== */
  const [aggregatesLoading, setAggregatesLoading] = useState(false);

  const loadLeagueAggregates = async () => {
    if (!teams.length || aggregatesLoading) return;

    setAggregatesLoading(true);
    try {
      const responses = await Promise.all(
        teams.map(async (team) => {
          try {
            const res = await fetch(
              `${API_BASE}/data/team/${encodeURIComponent(team)}`
            );
            if (!res.ok) return { team, items: [] };
            const data = await res.json();
            return { team, items: data.items || [] };
          } catch {
            return { team, items: [] };
          }
        })
      );

      const byRole = { P: new Map(), D: new Map(), C: new Map(), A: new Map() };

      responses.forEach(({ team, items }) => {
        items.forEach((p) => {
          const name = String(p.Giocatore || p.nome || "").trim();
          const role = String(p.Ruolo || p.ruolo_base || "").trim().toUpperCase();
          if (!name || !["P", "D", "C", "A"].includes(role)) return;

          if (!byRole[role].has(name)) {
            byRole[role].set(name, { name, teams: new Set(), count: 0, squadra: p.Squadra || "-" });
          }
          const entry = byRole[role].get(name);
          entry.teams.add(team);
          entry.count = entry.teams.size;
          entry.squadra = p.Squadra || entry.squadra || "-";
        });
      });

      const out = { P: [], D: [], C: [], A: [] };
      (["P", "D", "C", "A"]).forEach((r) => {
        out[r] = Array.from(byRole[r].values())
          .map((e) => ({
            name: e.name,
            squadra: e.squadra,
            teams: Array.from(e.teams).sort((a, b) => a.localeCompare(b)),
            count: e.count,
          }))
          .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
      });

      setTopPlayersByRole(out);
    } finally {
      setAggregatesLoading(false);
    }
  };

  /* ===========================
     PLAYER PROFILE
  =========================== */
  const openPlayer = async (name) => {
    if (!name) return;
    setSelectedPlayer(name);
    setActiveMenu("player");

    try {
      const [profileRes, statsRes] = await Promise.all([
        fetch(`${API_BASE}/data/players?q=${encodeURIComponent(name)}&limit=200`),
        fetch(`${API_BASE}/data/stats/player?name=${encodeURIComponent(name)}`),
      ]);

      if (profileRes.ok) {
        const data = await profileRes.json();
        const items = data.items || [];

        const exact = items.filter(
          (it) =>
            String(it.Giocatore || "")
              .trim()
              .toLowerCase() === String(name).trim().toLowerCase()
        );

        const chosen = exact[0] || items[0] || null;
        setPlayerProfile(chosen);

        const teamSet = new Set(
          exact.map((it) => String(it.Team || "").trim()).filter(Boolean)
        );
        setPlayerTeamCount(teamSet.size);
      }

      if (statsRes.ok) {
        const data = await statsRes.json();
        setPlayerStats(data.item || null);
      }
    } catch {}
  };

  /* ===========================
     NAV HELPERS (menu jumps)
  =========================== */
  const jumpToId = (id, menu, after = () => {}) => {
    if (menu) setActiveMenu(menu);
    after();

    setTimeout(() => {
      const el = document.getElementById(id);
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "center" });
        el.classList.add("flash-highlight");
        setTimeout(() => el.classList.remove("flash-highlight"), 1200);
      }
    }, 120);
  };

  const goToTeam = (team) => {
    if (!team) return;
    setActiveMenu("rose");
    setSelectedTeam(team);
  };

  const goToSquadra = (squadra, role) => {
    if (!squadra) return;
    setActiveMenu("listone");
    if (role && ["P", "D", "C", "A"].includes(role)) setQuoteRole(role);
    setQuoteTeam(squadra);
    setListoneQuery("");
  };

  /* ===========================
     MERCATO + SUGGEST (MEMO)
  =========================== */
  const manualSwapMap = useMemo(() => {
    const map = new Map();
    (manualResult?.swaps || []).forEach((s) => {
      map.set(normalizeName(s.out), s);
    });
    return map;
  }, [manualResult]);

  const qaOfPlayer = (player) =>
    Number(player?.QA ?? player?.PrezzoAttuale ?? player?.prezzo_attuale ?? 0) || 0;

  const manualBudgetSummary = useMemo(() => {
    const credits = Number(suggestPayload?.credits_residui ?? 0) || 0;
    const squad = suggestPayload?.user_squad || [];
    const outNames = (manualOuts || [])
      .map((v) => String(v || "").trim())
      .filter(Boolean);
    const outMap = new Map(
      squad.map((p) => [normalizeName(p.nome || p.Giocatore), p])
    );
    const outSum = outNames.reduce((sum, name) => {
      const player = outMap.get(normalizeName(name));
      return sum + (player ? qaOfPlayer(player) : 0);
    }, 0);
    const maxBudget = credits + outSum;
    const spent = (manualResult?.swaps || []).reduce(
      (sum, s) => sum + (Number(s.qa_in) || 0),
      0
    );
    const budgetFinal = maxBudget - spent;
    return {
      credits,
      outSum,
      maxBudget,
      spent,
      budgetFinal,
    };
  }, [suggestPayload, manualOuts, manualResult]);

  const topQuotes = useMemo(() => (topQuotesAll || []).slice(0, 5), [topQuotesAll]);
  const topPlusvalenze = useMemo(() => (plusvalenze || []).slice(0, 5), [plusvalenze]);
  const topStats = useMemo(() => (statsItems || []).slice(0, 5), [statsItems]);

  const rankedPlusvalenze = useMemo(
    () =>
      (allPlusvalenze || []).map((item, index) => ({
        ...item,
        rank: index + 1,
      })),
    [allPlusvalenze]
  );

  const filteredPlusvalenze = useMemo(() => {
    const q = String(plusvalenzeQuery || "").trim().toLowerCase();
    if (!q) return rankedPlusvalenze || [];
    return (rankedPlusvalenze || []).filter((item) =>
      String(item.team || "")
        .toLowerCase()
        .includes(q)
    );
  }, [rankedPlusvalenze, plusvalenzeQuery]);

  const filteredTopAcquisti = useMemo(() => {
    const q = String(topAcquistiQuery || "").trim().toLowerCase();
    const list = (topPlayersByRole[activeTopRole] || []).map((item, index) => ({
      ...item,
      rank: index + 1,
    }));
    if (!q) return list;
    return list.filter((p) =>
      String(p.name || "")
        .toLowerCase()
        .includes(q)
    );
  }, [topPlayersByRole, activeTopRole, topAcquistiQuery]);

  /* ===========================
     MERCATO: placeholder
  =========================== */

  /* ===========================
     SUGGEST: payload
  =========================== */
  const loadSuggestPayload = async (force = false) => {
    if (!accessKey.trim()) return false;
    if (!force && suggestPayload) return true;

    try {
      const res = await fetch(`${API_BASE}/data/market/payload`, {
        headers: {
          "X-Access-Key": accessKey.trim().toLowerCase(),
        },
      });
      if (!res.ok) return false;

      const data = await res.json();
      const payload = data.payload || data;

      setSuggestTeam(data.team || "");
      setSuggestPayload(payload);
      return true;
    } catch {
      return false;
    }
  };

  /* ===========================
     SUGGEST: run
  =========================== */
  const runSuggest = async () => {
    setSuggestError("");
    setSuggestions([]);
    setSuggestHasRun(true);

    if (!suggestPayload) {
      setSuggestError("Payload non disponibile. Riprova il login o aggiorna i dati.");
      return;
    }

    setSuggestLoading(true);

    const payloadToSend = { ...suggestPayload };
    if (!payloadToSend.teams_data || !Object.keys(payloadToSend.teams_data).length) {
      const clubs = new Set(
        (payloadToSend.user_squad || [])
          .map((p) => String(p.Squadra || "").trim())
          .filter(Boolean)
      );
      const fallbackTeams = {};
      clubs.forEach((club) => {
        fallbackTeams[club] = {
          PPG_S: 0,
          PPG_R8: 0,
          GFpg_S: 0,
          GFpg_R8: 0,
          GApg_S: 0,
          GApg_R8: 0,
          MoodTeam: 0.5,
          CoachStyle_P: 0.5,
          CoachStyle_D: 0.5,
          CoachStyle_C: 0.5,
          CoachStyle_A: 0.5,
          CoachStability: 0.5,
          CoachBoost: 0.5,
          GamesRemaining: 0,
        };
      });
      payloadToSend.teams_data = fallbackTeams;
    }

    try {
      const res = await fetch(`${API_BASE}/data/market/suggest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payloadToSend),
      });

      if (!res.ok) {
        setSuggestError("Errore nel motore consigli.");
        return;
      }

      const data = await res.json();
      const rawSolutions = data.solutions || [];
      setSuggestions(rawSolutions);

      if (rawSolutions.length === 0) {
        setSuggestError("Nessuna soluzione disponibile.");
      } else if (rawSolutions.length < 3) {
        setSuggestError(`Solo ${rawSolutions.length} soluzioni disponibili.`);
      }
    } catch {
      setSuggestError("Errore di connessione al motore consigli.");
    } finally {
      setSuggestLoading(false);
    }
  };

  /* ===========================
     GUIDED: max outs (stelle)
  =========================== */
  const getStarCount = (squad = []) =>
    squad.filter((p) => String(p.nome || p.Giocatore || "").trim().endsWith(" *")).length;

  const maxManualOuts = useMemo(() => {
    if (!suggestPayload?.user_squad) return 5;
    return 5 + getStarCount(suggestPayload.user_squad);
  }, [suggestPayload]);

  /* ===========================
     GUIDED: init outs
  =========================== */
  useEffect(() => {
    setManualOuts(Array.from({ length: maxManualOuts }, () => ""));
    setManualResult(null);
    setManualError("");
    setManualDislikes(new Set());
    setManualExcludedIns(new Set());
  }, [maxManualOuts, suggestPayload]);

  const resetManual = () => {
    setManualOuts(Array.from({ length: maxManualOuts }, () => ""));
    setManualResult(null);
    setManualError("");
    setManualDislikes(new Set());
    setManualExcludedIns(new Set());
  };

  /* ===========================
     GUIDED: compute (client-side)
  =========================== */
  const computeManualSuggestions = async () => {
    if (!suggestPayload) {
      setManualError("Payload non disponibile.");
      return;
    }

    const outs = (manualOuts || [])
      .map((v) => String(v || "").trim())
      .filter(Boolean);

    if (!outs.length) {
      setManualError("Seleziona almeno un OUT.");
      setManualResult(null);
      return;
    }

    setManualError("");
    setManualLoading(true);
    const previousResult = manualResult;

    try {
      const fixedSwaps =
        manualResult?.swaps
          ?.filter((s) => s?.in && !manualDislikes.has(normalizeName(s.in)))
          .map((s) => [s.out, s.in]) || [];

      const params = {
        ...(suggestPayload.params || {}),
        required_outs: outs,
        exclude_ins: Array.from(
          new Set([...(manualDislikes || []), ...(manualExcludedIns || [])])
        ),
        fixed_swaps: fixedSwaps,
        max_changes: outs.length,
      };

      const res = await fetch(`${API_BASE}/data/market/suggest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...suggestPayload,
          params,
        }),
      });

      if (!res.ok) {
        setManualError("Errore nel motore consigli.");
        setManualResult(null);
        return;
      }

      const data = await res.json();
      const solutions = data.solutions || [];
      if (!solutions.length) {
        setManualError("Nessun IN trovato con i vincoli dati.");
        setManualResult(null);
        return;
      }

      let next = solutions[0];
      if (next && previousResult?.swaps?.length && fixedSwaps.length) {
        const prevMap = new Map(
          previousResult.swaps.map((s) => [normalizeName(s.out), s])
        );
        const fixedSet = new Set(fixedSwaps.map((s) => normalizeName(s[0])));
        const merged = [];
        const used = new Set();
        (next.swaps || []).forEach((s) => {
          const outKey = normalizeName(s.out);
          if (fixedSet.has(outKey) && prevMap.has(outKey)) {
            merged.push(prevMap.get(outKey));
          } else {
            merged.push(s);
          }
          used.add(outKey);
        });
        fixedSet.forEach((outKey) => {
          if (!used.has(outKey) && prevMap.has(outKey)) {
            merged.push(prevMap.get(outKey));
          }
        });
        next = { ...next, swaps: merged };
      }
      setManualResult(next);
      setManualExcludedIns((prev) => {
        const nextSet = new Set(prev);
        (next?.swaps || []).forEach((s) => {
          if (s?.in) {
            nextSet.add(normalizeName(s.in));
          }
        });
        manualDislikes.forEach((name) => nextSet.add(name));
        return nextSet;
      });
      setManualDislikes(new Set());
    } catch {
      setManualError("Errore durante il calcolo degli IN.");
      setManualResult(null);
    } finally {
      setManualLoading(false);
    }
  };

  /* ===========================
     ADMIN KEYS
  =========================== */
  const loadAdminKeys = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/keys`, {
        headers: { "X-Admin-Key": accessKey.trim().toLowerCase() },
      });
      if (!res.ok) return;
      const data = await res.json();
      setAdminKeys(data || []);
    } catch {}
  };

  const createNewKey = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/keys`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
      });
      if (!res.ok) return;
      const data = await res.json();
      setNewKey(data.key || "");
      setAdminNotice("Key creata.");
      loadAdminKeys();
    } catch {}
  };

  const loadAdminStatus = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/status`, {
        headers: { "X-Admin-Key": accessKey.trim().toLowerCase() },
      });
      if (!res.ok) return;
      const data = await res.json();
      setAdminStatus(data || null);
    } catch {}
  };

  const loadAdminTeamKeys = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/team-keys`, {
        headers: { "X-Admin-Key": accessKey.trim().toLowerCase() },
      });
      if (!res.ok) return;
      const data = await res.json();
      setAdminTeamKeys(data || []);
    } catch {}
  };

  const refreshMarketAdmin = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetch(`${API_BASE}/data/admin/market/refresh`, {
        method: "POST",
        headers: { "X-Admin-Key": accessKey.trim().toLowerCase() },
      });
      if (!res.ok) return;
      setAdminNotice("Mercato aggiornato.");
      loadAdminStatus();
      loadMarket();
    } catch {}
  };

  const setAdminForKey = async () => {
    if (!isAdmin) return;
    const key = adminSetAdminKey.trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/set-admin`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ key, is_admin: true }),
      });
      if (!res.ok) return;
      setAdminNotice(`Key ${key.toUpperCase()} promossa ad admin.`);
      setAdminSetAdminKey("");
      loadAdminKeys();
    } catch {}
  };

  const assignTeamKey = async () => {
    if (!isAdmin) return;
    const key = adminTeamKey.trim().toLowerCase();
    const team = adminTeamName.trim();
    if (!key || !team) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/team-key`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ key, team }),
      });
      if (!res.ok) return;
      setAdminNotice(`Key ${key.toUpperCase()} associata a ${team}.`);
      setAdminTeamKey("");
      setAdminTeamName("");
      loadAdminTeamKeys();
    } catch {}
  };

  const resetKeyAdmin = async () => {
    if (!isAdmin) return;
    const key = adminResetKey.trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/reset-key`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ key }),
      });
      if (!res.ok) return;
      setAdminNotice(`Key ${key.toUpperCase()} resettata.`);
      setAdminResetKey("");
      loadAdminKeys();
    } catch {}
  };

  const importKeysAdmin = async () => {
    if (!isAdmin) return;
    const raw = adminImportKeys
      .split(/[\s,;]+/)
      .map((k) => k.trim())
      .filter(Boolean);
    if (!raw.length) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/import-keys`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ keys: raw, is_admin: adminImportIsAdmin }),
      });
      if (!res.ok) return;
      setAdminNotice(`Importate ${raw.length} key.`);
      setAdminImportKeys("");
      loadAdminKeys();
    } catch {}
  };

  const importTeamKeysAdmin = async () => {
    if (!isAdmin) return;
    const items = adminImportTeamKeys
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const [key, ...teamParts] = line.split(/[;,]/).map((p) => p.trim());
        const team = teamParts.join(" ").trim();
        return key && team ? { key, team } : null;
      })
      .filter(Boolean);
    if (!items.length) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/import-team-keys`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ items }),
      });
      if (!res.ok) return;
      setAdminNotice(`Importate ${items.length} associazioni team.`);
      setAdminImportTeamKeys("");
      loadAdminTeamKeys();
    } catch {}
  };

  const deleteTeamKeyAdmin = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetch(`${API_BASE}/auth/admin/team-key`, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
          "X-Admin-Key": accessKey.trim().toLowerCase(),
        },
        body: JSON.stringify({ key }),
      });
      if (!res.ok) return;
      setAdminNotice(`Associazione rimossa: ${key.toUpperCase()}.`);
      loadAdminTeamKeys();
    } catch {}
  };

  /* ===========================
     EFFECTS
  =========================== */
  useEffect(() => {
    try {
      const saved = localStorage.getItem(KEY_STORAGE);
      if (saved) {
        setAccessKey(saved);
        setRememberKey(true);
      }
    } catch {}
  }, []);

  useEffect(() => {
    try {
      const stored = localStorage.getItem("fp_theme");
      const next = stored === "light" ? "light" : "dark";
      setTheme(next);
      document.body.classList.toggle("theme-light", next === "light");
    } catch {}
  }, []);

  useEffect(() => {
    loadSummary();
    loadTeams();
    loadPlusvalenze();
    loadAllPlusvalenze();
    loadListone();
    loadTopQuotesAllRoles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    loadStatList(statsTab);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statsTab]);

  useEffect(() => {
    if (!loggedIn) return;

    loadAdminKeys();
    loadAdminStatus();
    loadAdminTeamKeys();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn]);

  useEffect(() => {
    if (!loggedIn || !accessKey.trim()) return;
    const ping = async () => {
      try {
        await fetch(`${API_BASE}/auth/ping`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ key: accessKey.trim(), device_id: deviceId }),
        });
      } catch {}
    };
    ping();
    const timer = setInterval(ping, 60000);
    return () => clearInterval(timer);
  }, [loggedIn, accessKey, deviceId]);

  useEffect(() => {
    if (!loggedIn || !teams.length) return;
    loadLeagueAggregates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, teams]);

  useEffect(() => {
    if (!loggedIn) return;
    loadRoster(selectedTeam);
    setRoleFilter("all");
    setSquadraFilter("all");
    setRosterQuery("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, selectedTeam]);

  useEffect(() => {
    if (!loggedIn) return;
    loadPlusvalenze();
    loadAllPlusvalenze();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, plusvalenzePeriod]);

  useEffect(() => {
    if (!loggedIn) return;
    loadStatList(statsTab);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, statsTab]);

  useEffect(() => {
    if (!loggedIn) return;
    const h = setTimeout(() => runSearch(query), 250);
    return () => clearTimeout(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, activeTab, loggedIn]);

  useEffect(() => {
    if (loggedIn && activeMenu === "listone") loadListone();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu, quoteRole, quoteOrder]);

  /* ===========================
     MENU OPEN (mobile)
  =========================== */
  const setMenuOpen = (open) => {
    if (open) document.body.classList.add("menu-open");
    else document.body.classList.remove("menu-open");
  };

  useEffect(() => {
    if (!loggedIn) setMenuOpen(false);
  }, [loggedIn]);

  /* ===========================
     PLAYER SLUG
  =========================== */
  const playerSlug = slugify(selectedPlayer);

useEffect(() => {
  if (!loggedIn) {
    setSuggestPayload(null);
    setSuggestTeam("");
    setSuggestions([]);
    setSuggestError("");
    setSuggestHasRun(false);
    return;
  }
  loadSuggestPayload();
  // eslint-disable-next-line react-hooks/exhaustive-deps
}, [loggedIn]);

  /* ===========================
     RENDER
  =========================== */
  return (
    <div className="login-page">
      {!loggedIn ? (
        <div className="login-shell">
          <header className="login-header">
            <p className="eyebrow">FantaPortoscuso</p>
            <p className="muted">
              Key monouso con blocco dispositivo. Tutto il resto si sblocca dopo
              il login.
            </p>
          </header>

          <section className="login-card">
            <h3>Accedi con Key</h3>

            <label className="field">
              <span>Key</span>
              <input
                value={accessKey}
                onChange={(e) => setAccessKey(e.target.value.toUpperCase())}
                placeholder="A1B2C3D4"
                maxLength={32}
              />
            </label>

            <label className="field">
              <span>Device ID</span>
              <input value={deviceId} readOnly />
            </label>

            <label className="checkbox">
              <input
                type="checkbox"
                checked={rememberKey}
                onChange={(e) => setRememberKey(e.target.checked)}
              />
              <span>Ricorda questa key su questo dispositivo</span>
            </label>

            {error ? <p className="error">{error}</p> : null}

            <button
              className="primary"
              onClick={handleLogin}
              disabled={status === "loading"}
            >
              {status === "loading" ? "Verifica in corso..." : "Verifica e Accedi"}
            </button>

            <p className="hint">
              Se hai problemi con la key, contatta l&apos;amministratore della lega.
            </p>
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
                roster={rosterDisplay}
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

