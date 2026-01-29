import { useEffect, useMemo, useState } from "react";

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

/* ===========================
   CONFIG
=========================== */
const KEY_STORAGE = "fp_access_key";
const API_BASE =
  import.meta.env.VITE_API_BASE || "http://localhost:8000";

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

  /* ===== STATS ===== */
  const [statsTab, setStatsTab] = useState("gol");
  const [statsItems, setStatsItems] = useState([]);

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
  const [listoneQuery, setListoneQuery] = useState("");

  /* ===== PLUSVALENZE ===== */
  const [plusvalenze, setPlusvalenze] = useState([]);
  const [allPlusvalenze, setAllPlusvalenze] = useState([]);
  const [plusvalenzePeriod, setPlusvalenzePeriod] = useState("december");

  /* ===== TOP ACQUISTI ===== */
  const [topPlayersByRole, setTopPlayersByRole] = useState({
    P: [],
    D: [],
    C: [],
    A: [],
  });
  const [activeTopRole, setActiveTopRole] = useState("P");

  /* ===== PLAYER ===== */
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [playerProfile, setPlayerProfile] = useState(null);
  const [playerStats, setPlayerStats] = useState(null);
  const [playerTeamCount, setPlayerTeamCount] = useState(0);

  /* ===== ADMIN ===== */
  const [adminKeys, setAdminKeys] = useState([]);
  const [newKey, setNewKey] = useState("");

  /* ===== MERCATO + SUGGEST ===== */
const [marketCountdown, setMarketCountdown] = useState("");
const [marketItems, setMarketItems] = useState([]);
const [marketTeams, setMarketTeams] = useState([]);
const [marketView, setMarketView] = useState("players");
const [marketPreview, setMarketPreview] = useState(false);

const [suggestPayload, setSuggestPayload] = useState(null);
const [suggestTeam, setSuggestTeam] = useState("");
const [suggestions, setSuggestions] = useState([]);
const [suggestError, setSuggestError] = useState("");
const [suggestLoading, setSuggestLoading] = useState(false);

const [manualOuts, setManualOuts] = useState([]);
const [manualResult, setManualResult] = useState(null);
const [manualError, setManualError] = useState("");
const [manualLoading, setManualLoading] = useState(false);

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
      const items = data.items || [];
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
      return;
    }

    const url =
      activeTab === "quotazioni"
        ? `${API_BASE}/data/quotazioni?q=${encodeURIComponent(value)}`
        : `${API_BASE}/data/players?q=${encodeURIComponent(value)}`;

    try {
      const res = await fetch(url);
      if (!res.ok) {
        setResults([]);
        return;
      }
      const data = await res.json();
      setResults(data.items || []);
    } catch {
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

  /* ===========================
     MERCATO: countdown
  =========================== */
  const getMarketCountdown = () => {
    const target = new Date("2026-02-03T08:00:00");
    const now = new Date();
    const diff = target.getTime() - now.getTime();
    if (Number.isNaN(diff) || diff <= 0) return "Apertura imminente";
    const totalSeconds = Math.floor(diff / 1000);
    const days = Math.floor(totalSeconds / (24 * 3600));
    const hours = Math.floor((totalSeconds % (24 * 3600)) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${days}g ${hours}h ${minutes}m ${seconds}s`;
  };

  /* ===========================
     MERCATO: load placeholder
  =========================== */
  const loadMarket = async () => {
    try {
      const res = await fetch(`${API_BASE}/data/market`);
      if (!res.ok) return;
      const data = await res.json();
      setMarketItems(data.items || []);
      setMarketTeams(data.teams || []);
    } catch {}
  };

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

    if (!suggestPayload) {
      setSuggestError("Payload non disponibile. Riprova il login o aggiorna i dati.");
      return;
    }

    setSuggestLoading(true);

    try {
      const res = await fetch(`${API_BASE}/data/market/suggest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(suggestPayload),
      });

      if (!res.ok) {
        setSuggestError("Errore nel motore consigli.");
        return;
      }

      const data = await res.json();
      const rawSolutions = data.solutions || [];
      setSuggestions(rawSolutions);

      if (rawSolutions.length < 3) {
        setSuggestError("Impossibile generare 3 soluzioni diverse con i vincoli attuali.");
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
  }, [maxManualOuts, suggestPayload]);

  const resetManual = () => {
    setManualOuts(Array.from({ length: maxManualOuts }, () => ""));
    setManualResult(null);
    setManualError("");
  };

  /* ===========================
     GUIDED: compute (client-side)
  =========================== */
  const computeManualSuggestions = () => {
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

    try {
      const squad = suggestPayload.user_squad || [];
      const pool = suggestPayload.players_pool || [];

      const normalize = (value) =>
        String(value || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "");

      const roleOf = (player) =>
        String(player?.ruolo_base || player?.Ruolo || "")
          .trim()
          .toUpperCase();

      const qaOf = (player) =>
        Number(player?.QA ?? player?.PrezzoAttuale ?? player?.prezzo_attuale ?? 0) || 0;

      const teamOf = (player) => String(player?.Squadra || player?.squadra || "");

      const squadMap = new Map(
        squad.map((p) => [normalize(p.nome || p.Giocatore), p])
      );

      const outSet = new Set(outs.map((name) => normalize(name)));

      const teamCounts = new Map();
      squad.forEach((p) => {
        const team = teamOf(p);
        if (!team) return;
        const key = normalize(p.nome || p.Giocatore);
        if (outSet.has(key)) return;
        teamCounts.set(team, (teamCounts.get(team) || 0) + 1);
      });

      const usedIn = new Set();
      const swaps = [];

      const scorePlayer = (candidate) => {
        const pv = Number(candidate?.PV_S ?? candidate?.Pv_S ?? 0) || 0;
        const efp = Number(candidate?.Efp ?? candidate?.EFP ?? candidate?.Bonus ?? 0) || 0;
        const qa = qaOf(candidate);
        return pv + efp - qa * 0.05;
      };

      const pickCandidate = (outRole) => {
        const strict = pool
          .filter((p) => {
            const name = normalize(p.nome || p.Giocatore);
            if (!name || usedIn.has(name)) return false;
            if (squadMap.has(name) && !outSet.has(name)) return false;
            if (roleOf(p) !== outRole) return false;
            const team = teamOf(p);
            const count = teamCounts.get(team) || 0;
            return count < 3;
          })
          .sort((a, b) => scorePlayer(b) - scorePlayer(a));

        if (strict.length) return strict[0];

        const relaxed = pool
          .filter((p) => {
            const name = normalize(p.nome || p.Giocatore);
            if (!name || usedIn.has(name)) return false;
            if (squadMap.has(name) && !outSet.has(name)) return false;
            if (roleOf(p) !== outRole) return false;
            return true;
          })
          .sort((a, b) => scorePlayer(b) - scorePlayer(a));

        return relaxed[0] || null;
      };

      outs.forEach((outName) => {
        const outKey = normalize(outName);
        const outPlayer = squadMap.get(outKey);
        if (!outPlayer) return;

        const outRole = roleOf(outPlayer);
        const outQa = qaOf(outPlayer);

        const inPlayer = pickCandidate(outRole);
        if (!inPlayer) return;

        const inName = inPlayer.nome || inPlayer.Giocatore;
        const inKey = normalize(inName);

        usedIn.add(inKey);

        const team = teamOf(inPlayer);
        if (team) teamCounts.set(team, (teamCounts.get(team) || 0) + 1);

        const inQa = qaOf(inPlayer);
        swaps.push({
          out: outName,
          in: inName,
          qa_out: outQa,
          qa_in: inQa,
          gain: outQa - inQa,
          cost_net: inQa - outQa,
        });
      });

      if (!swaps.length) {
        setManualError("Nessun IN trovato con i vincoli dati.");
        setManualResult(null);
      } else {
        setManualResult({ swaps });
      }
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
      loadAdminKeys();
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
    if (!loggedIn) return;

    loadSummary();
    loadTeams();
    loadPlusvalenze();
    loadAllPlusvalenze();
    loadListone();
    loadStatList(statsTab);
    loadAdminKeys();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn]);

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
  const update = () => setMarketCountdown(getMarketCountdown());
  update();
  const timer = setInterval(update, 1000);
  return () => clearInterval(timer);
}, []);

useEffect(() => {
  if (!loggedIn) {
    setSuggestPayload(null);
    setSuggestTeam("");
    setSuggestions([]);
    setSuggestError("");
    return;
  }
  loadMarket();
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
                className={activeMenu === "medie" ? "menu-item active" : "menu-item"}
                onClick={() => {
                  setActiveMenu("medie");
                  setMenuOpen(false);
                }}
              >
                Medie
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
                Top Giocatori Acquistati
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
                  : activeMenu === "medie"
                  ? "Medie"
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
              <section className="dashboard">
                <div className="dashboard-header left row">
                  <div>
                    <p className="eyebrow">Home</p>
                    <h2>Panoramica Lega</h2>
                  </div>

                  <div className="summary">
                    <button
                      type="button"
                      className="summary-card clickable"
                      onClick={() => setActiveMenu("rose")}
                    >
                      <span>Squadre</span>
                      <strong>{summary.teams}</strong>
                    </button>

                    <button
                      type="button"
                      className="summary-card clickable"
                      onClick={() => setActiveMenu("listone")}
                    >
                      <span>Giocatori</span>
                      <strong>{summary.players}</strong>
                    </button>
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h3>Ricerca rapida</h3>
                    <div className="tabs">
                      {["Rose", "Quotazioni"].map((tab) => (
                        <button
                          key={tab}
                          className={
                            activeTab === tab.toLowerCase() ? "tab active" : "tab"
                          }
                          onClick={() => setActiveTab(tab.toLowerCase())}
                        >
                          {tab}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="search-box">
                    <input
                      placeholder={`Cerca in ${activeTab}...`}
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                    />
                    <button className="ghost" onClick={() => runSearch(query)}>
                      Filtra
                    </button>
                  </div>

                  <div className="list">
                    {results.map((item, index) => (
                      <div
                        key={`${item.Giocatore}-${index}`}
                        className="list-item player-card"
                        onClick={() => openPlayer(item.Giocatore)}
                      >
                        <div>
                          <p>
                            <button
                              type="button"
                              className="link-button"
                              onClick={(e) => {
                                e.stopPropagation();
                                openPlayer(item.Giocatore);
                              }}
                            >
                              {item.Giocatore}
                            </button>{" "}
                            <span className="muted">· {item.Squadra || "-"}</span>
                          </p>
                          <span className="muted">Team: {item.Team || "-"}</span>
                        </div>
                        <strong>{formatInt(item.PrezzoAttuale || item.QuotazioneAttuale)}</strong>
                      </div>
                    ))}
                  </div>
                </div>
              </section>
            )}

            {/* ===========================
                STATS
            =========================== */}
            {activeMenu === "stats" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Statistiche Giocatori</p>
                    <h2>Classifiche per statistica</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="role-switch">
                    {[
                      { key: "gol", label: "Gol" },
                      { key: "assist", label: "Assist" },
                      { key: "ammonizioni", label: "Ammonizioni" },
                      { key: "espulsioni", label: "Espulsioni" },
                      { key: "cleansheet", label: "Clean Sheet" },
                    ].map((tab) => (
                      <button
                        key={tab.key}
                        className={`role-pill ${statsTab === tab.key ? "active" : ""}`}
                        onClick={() => setStatsTab(tab.key)}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>

                  <div className="list">
                    {statsItems.length === 0 ? (
                      <p className="muted">Nessun dato disponibile.</p>
                    ) : (
                      statsItems.map((item, index) => {
                        const itemSlug = slugify(item.Giocatore);
                        return (
                          <div
                            key={`${item.Giocatore}-${index}`}
                            id={`stat-${statsTab}-${itemSlug}`}
                            className="list-item player-card"
                            onClick={() => openPlayer(item.Giocatore)}
                          >
                            <div>
                              <p className="rank-title">
                                <span className="rank-badge">#{index + 1}</span>
                                <button
                                  type="button"
                                  className="link-button"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    openPlayer(item.Giocatore);
                                  }}
                                >
                                  {item.Giocatore}
                                </button>
                              </p>
                              <span className="muted">{item.Squadra || "-"}</span>
                            </div>
                            <strong>
                              {item[tabToColumn(statsTab)] ?? item[statsTab] ?? "-"}
                            </strong>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>
              </section>
            )}

            {/* ===========================
                PLUSVALENZE
            =========================== */}
            {activeMenu === "plusvalenze" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Plusvalenze</p>
                    <h2>Classifica completa</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="filters inline">
                    <label className="field">
                      <span>Periodo</span>
                      <select
                        className="select"
                        value={plusvalenzePeriod}
                        onChange={(e) => setPlusvalenzePeriod(e.target.value)}
                      >
                        <option value="december">Da Dicembre</option>
                        <option value="start">Dall&apos;inizio</option>
                      </select>
                    </label>
                  </div>

                  <div className="list">
                    {allPlusvalenze.length === 0 ? (
                      <p className="muted">Nessun dato disponibile.</p>
                    ) : (
                      allPlusvalenze.map((item, index) => (
                        <div
                          key={item.team}
                          className="list-item player-card"
                          onClick={() => goToTeam(item.team)}
                        >
                          <div>
                            <p className="rank-title">
                              <span className="rank-badge">#{index + 1}</span>
                              <span className="team-name">{item.team}</span>
                            </p>
                            <span className="muted">
                              Acquisto {formatInt(item.acquisto)} · Attuale{" "}
                              {formatInt(item.attuale)}
                            </span>
                          </div>
                          <strong>
                            {formatInt(item.plusvalenza)} ({item.percentuale}%)
                          </strong>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </section>
            )}

            {/* ===========================
                ROSE
            =========================== */}
            {activeMenu === "rose" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Rose</p>
                    <h2>Rosa squadra</h2>
                  </div>
                </div>

                <div className="panel">
                  <label className="field">
                    <span>Seleziona squadra</span>
                    <select
                      className="select"
                      value={selectedTeam}
                      onChange={(e) => setSelectedTeam(e.target.value)}
                    >
                      {teams.map((team) => (
                        <option key={team} value={team}>
                          {team}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="field">
                    <span>Cerca giocatore</span>
                    <input
                      placeholder="Es. Lautaro, Di Lorenzo..."
                      value={rosterQuery}
                      onChange={(e) => setRosterQuery(e.target.value)}
                    />
                  </label>

                  <div className="filters">
                    <label className="field">
                      <span>Ruolo</span>
                      <select
                        className="select"
                        value={roleFilter}
                        onChange={(e) => setRoleFilter(e.target.value)}
                      >
                        <option value="all">Tutti</option>
                        <option value="P">Portieri</option>
                        <option value="D">Difensori</option>
                        <option value="C">Centrocampisti</option>
                        <option value="A">Attaccanti</option>
                      </select>
                    </label>

                    <label className="field">
                      <span>Squadra reale</span>
                      <select
                        className="select"
                        value={squadraFilter}
                        onChange={(e) => setSquadraFilter(e.target.value)}
                      >
                        <option value="all">Tutte</option>
                        {[...new Set(roster.map((r) => r.Squadra).filter(Boolean))].map(
                          (sq) => (
                            <option key={sq} value={sq}>
                              {sq}
                            </option>
                          )
                        )}
                      </select>
                    </label>
                  </div>
                </div>

                <div className="panel">
                  {roster.length === 0 ? (
                    <p className="muted">Nessun dato disponibile.</p>
                  ) : (
                    ["P", "D", "C", "A"].map((role) => {
                      const roleLabel =
                        role === "P"
                          ? "Portieri"
                          : role === "D"
                          ? "Difensori"
                          : role === "C"
                          ? "Centrocampisti"
                          : "Attaccanti";

                      const filtered = roster
                        .filter((it) => (roleFilter === "all" ? true : it.Ruolo === roleFilter))
                        .filter((it) => it.Ruolo === role)
                        .filter((it) => (squadraFilter === "all" ? true : it.Squadra === squadraFilter))
                        .filter((it) =>
                          rosterQuery.trim()
                            ? String(it.Giocatore || "")
                                .toLowerCase()
                                .includes(rosterQuery.trim().toLowerCase())
                            : true
                        );

                      if (!filtered.length) return null;

                      const totalSpesa = filtered.reduce((sum, it) => {
                        const v = Number(it.PrezzoAcquisto || 0);
                        return Number.isNaN(v) ? sum : sum + v;
                      }, 0);

                      return (
                        <details key={role} className="accordion" open>
                          <summary>
                            <span>{roleLabel}</span>
                            <strong>Spesa: {formatInt(totalSpesa)}</strong>
                          </summary>

                          <div className="list">
                            {filtered.map((it, idx) => (
                              <div
                                key={`${it.Giocatore}-${idx}`}
                                className="list-item player-card"
                                onClick={() => openPlayer(it.Giocatore)}
                              >
                                <div>
                                  <p>
                                    <button
                                      type="button"
                                      className="link-button"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        openPlayer(it.Giocatore);
                                      }}
                                    >
                                      {it.Giocatore}
                                    </button>{" "}
                                    <span className="muted">· {it.Squadra || "-"}</span>
                                  </p>
                                  <span className="muted">
                                    Acquisto {formatInt(it.PrezzoAcquisto)} · Attuale{" "}
                                    {formatInt(it.PrezzoAttuale)}
                                  </span>
                                </div>
                                <strong>{it.Ruolo}</strong>
                              </div>
                            ))}
                          </div>
                        </details>
                      );
                    })
                  )}
                </div>
              </section>
            )}

            {/* ===========================
                LISTONE
            =========================== */}
            {activeMenu === "listone" && (
              <section className="dashboard">
                <div className="dashboard-header left row">
                  <div>
                    <p className="eyebrow">Listone</p>
                    <h2>Quotazioni giocatori</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="filters inline">
                    <label className="field">
                      <span>Ruolo</span>
                      <select
                        className="select"
                        value={quoteRole}
                        onChange={(e) => setQuoteRole(e.target.value)}
                      >
                        <option value="P">Portieri</option>
                        <option value="D">Difensori</option>
                        <option value="C">Centrocampisti</option>
                        <option value="A">Attaccanti</option>
                      </select>
                    </label>

                    <label className="field">
                      <span>Squadra</span>
                      <select
                        className="select"
                        value={quoteTeam}
                        onChange={(e) => setQuoteTeam(e.target.value)}
                      >
                        <option value="all">Tutte</option>
                        {[...new Set(quoteList.map((q) => q.Squadra).filter(Boolean))].map((sq) => (
                          <option key={sq} value={sq}>
                            {sq}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="field">
                      <span>Ordine</span>
                      <select
                        className="select"
                        value={quoteOrder}
                        onChange={(e) => setQuoteOrder(e.target.value)}
                      >
                        <option value="price_desc">Quotazione (decrescente)</option>
                        <option value="price_asc">Quotazione (crescente)</option>
                        <option value="alpha">Alfabetico (A-Z)</option>
                        <option value="alpha_desc">Alfabetico (Z-A)</option>
                      </select>
                    </label>
                  </div>

                  <label className="field">
                    <span>Cerca giocatore</span>
                    <input
                      placeholder="Es. Maignan, Barella..."
                      value={listoneQuery}
                      onChange={(e) => setListoneQuery(e.target.value)}
                    />
                  </label>

                  <div className="list">
                    {quoteList
                      .filter((it) => (quoteTeam === "all" ? true : it.Squadra === quoteTeam))
                      .filter((it) =>
                        listoneQuery.trim()
                          ? String(it.Giocatore || "")
                              .toLowerCase()
                              .includes(listoneQuery.trim().toLowerCase())
                          : true
                      )
                      .map((it, idx) => {
                        const itemSlug = slugify(it.Giocatore);
                        return (
                          <div
                            key={`${it.Giocatore}-${idx}`}
                            id={`listone-${itemSlug}`}
                            className="list-item boxed player-card"
                            onClick={() => openPlayer(it.Giocatore)}
                          >
                            <div>
                              <p>
                                <button
                                  type="button"
                                  className="link-button"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    openPlayer(it.Giocatore);
                                  }}
                                >
                                  {it.Giocatore}
                                </button>
                              </p>
                              <span className="muted">{it.Squadra || "-"}</span>
                            </div>
                            <strong>{formatInt(it.PrezzoAttuale)}</strong>
                          </div>
                        );
                      })}
                  </div>
                </div>
              </section>
            )}

            {/* ===========================
                TOP ACQUISTI
            =========================== */}
            {activeMenu === "top-acquisti" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Top Giocatori Acquistati</p>
                    <h2>Per ruolo</h2>
                  </div>
                </div>

                <div className="panel">
                  <div className="role-switch">
                    {[
                      { key: "P", label: "Portieri" },
                      { key: "D", label: "Difensori" },
                      { key: "C", label: "Centrocampisti" },
                      { key: "A", label: "Attaccanti" },
                    ].map((role) => (
                      <button
                        key={role.key}
                        className={`role-pill ${activeTopRole === role.key ? "active" : ""}`}
                        onClick={() => setActiveTopRole(role.key)}
                      >
                        {role.label}
                      </button>
                    ))}
                  </div>

                  <div className="list">
                    {aggregatesLoading ? (
                      <p className="muted">Caricamento...</p>
                    ) : (topPlayersByRole[activeTopRole] || []).length ? (
                      (topPlayersByRole[activeTopRole] || []).map((p, idx) => (
                        <div
                          key={`${p.name}-${idx}`}
                          className="list-item player-card"
                          onClick={() => openPlayer(p.name)}
                        >
                          <div>
                            <p className="rank-title">
                              <span className="rank-badge">#{idx + 1}</span>
                              <button
                                type="button"
                                className="link-button"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  openPlayer(p.name);
                                }}
                              >
                                {p.name}
                              </button>
                            </p>
                            <span className="muted">
                              Squadra: {p.squadra || "-"} · Teams: {p.count}
                            </span>
                            <div className="team-tags">
                              {(p.teams || []).map((t) => (
                                <span key={`${p.name}-${t}`} className="team-tag">
                                  {t}
                                </span>
                              ))}
                            </div>
                          </div>
                          <strong>{p.count}</strong>
                        </div>
                      ))
                    ) : (
                      <p className="muted">Nessun dato disponibile.</p>
                    )}
                  </div>
                </div>
              </section>
            )}

            {activeMenu === "mercato" && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Mercato</p>
                    <h2>Coming Soon</h2>
                    <p className="muted">Apertura mercato: 3 Febbraio 2026, ore 08:00.</p>
                  </div>
                </div>

                <div className="panel market-panel">
                  <div className="market-warning">
                    <div className="market-warning-badge">Coming Soon</div>
                    <h3>Il mercato apre a breve</h3>
                    <p className="muted">
                      Stiamo preparando il pannello con tutte le operazioni quotidiane.
                    </p>

                    <div className="market-countdown-inline">
                      <span>Start tra</span>
                      <strong>{marketCountdown}</strong>
                    </div>              
                  </div>
                </div>


                {/* ===========================
                    MOTORE CONSIGLI MERCATO
                =========================== */}
                <div className="panel">
                  <div className="panel-header">
                    <h3>Motore Consigli Mercato</h3>
                  </div>

                  <div className="admin-actions">
                    <button
                      className="primary"
                      onClick={runSuggest}
                      disabled={suggestLoading}
                    >
                      {suggestLoading ? "Calcolo..." : "Calcola Top 3"}
                    </button>

                    {suggestError ? <span className="muted">{suggestError}</span> : null}
                  </div>

                  <div className="list">
                    {suggestions.length === 0 ? (
                      <p className="muted">Nessuna soluzione disponibile.</p>
                    ) : (
                      suggestions.map((sol, idx) => (
                        <div key={`sol-${idx}`} className="list-item player-card">
                          <div>
                            <p className="rank-title">
                              <span className="rank-badge">#{idx + 1}</span>
                              <span className="team-name">
                                Total Gain {formatDecimal(sol.total_gain, 2)}
                              </span>
                            </p>

                            <span className="muted">
                              Budget iniziale {formatDecimal(sol.budget_initial, 2)} · Finale{" "}
                              {formatDecimal(sol.budget_final, 2)}
                            </span>

                            <div className="team-tags">
                              {(sol.warnings || []).map((w) => (
                                <span key={w} className="team-tag">
                                  {w}
                                </span>
                              ))}
                            </div>

                            <div className="list">
                              {(sol.swaps || []).map((s, i) => (
                                <div
                                  key={`${idx}-${i}`}
                                  className="list-item boxed player-card"
                                >
                                  <div>
                                    <p>
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
                                      →{" "}
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
                                      QA OUT {formatDecimal(s.qa_out, 2)} · QA IN{" "}
                                      {formatDecimal(s.qa_in, 2)}
                                    </span>
                                  </div>
                                  <strong>{formatDecimal(s.gain, 2)}</strong>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>
                      ))
                    )}
                  </div>

                  {/* ===========================
                      SOLUZIONE GUIDATA (OUT GUIDATI)
                  =========================== */}
                  <div className="panel" style={{ marginTop: 16 }}>
                    <div className="panel-header">
                      <div>
                        <h3>Soluzione guidata (OUT guidati)</h3>
                        <span className="muted">
                          Seleziona gli OUT e calcola i migliori IN.
                        </span>
                      </div>
                    </div>

                    <div className="admin-actions">
                      <button className="ghost" onClick={resetManual}>
                        Reset selezione
                      </button>
                      <button
                        className="primary"
                        onClick={computeManualSuggestions}
                        disabled={manualLoading}
                      >
                        {manualLoading ? "Calcolo..." : "Calcola IN"}
                      </button>
                      {manualError ? <span className="muted">{manualError}</span> : null}
                    </div>

                    <div className="list guided-list">
                      {(manualOuts || []).map((value, i) => {
                        const usedNames = new Set(
                          manualOuts
                            .filter((_, idx) => idx !== i)
                            .map((name) => String(name || "").trim())
                            .filter(Boolean)
                        );

                        const swap = manualSwapMap.get(normalizeName(value));

                        return (
                          <div
                            key={`out-${i}`}
                            className="list-item boxed player-card guided-row"
                          >
                            <div className="guided-out">
                              <p>OUT #{i + 1}</p>
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
                                    String(name || "").trim() !==
                                      String(value || "").trim() &&
                                    usedNames.has(String(name || "").trim());

                                  return (
                                    <option key={`${name}-${i}`} value={name} disabled={disabled}>
                                      {name} ({p.Ruolo || p.ruolo_base || "-"})
                                    </option>
                                  );
                                })}
                              </select>
                              <span className="muted">Slot opzionale</span>
                            </div>

                            <div className="guided-in">
                              {swap ? (
                                <div>
                                  <p className="guided-title">IN suggerito</p>
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
                                    QA OUT {formatDecimal(swap.qa_out, 2)} · QA IN{" "}
                                    {formatDecimal(swap.qa_in, 2)}
                                  </span>
                                </div>
                              ) : (
                                <p className="muted">Seleziona un OUT e calcola.</p>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </section>
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
                              {item.is_admin ? "ADMIN" : "USER"} ·{" "}
                              {item.used ? "Attivata" : "Non usata"}
                            </span>
                          </div>
                          <strong>{item.device_id ? "1+ device" : "-"}</strong>
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
