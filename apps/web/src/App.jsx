import { useEffect, useMemo, useRef, useState } from "react";
import { useMarketPlaceholder } from "./hooks/useMarketPlaceholder";
import ListoneSection from "./components/sections/ListoneSection";
import HomeSection from "./components/sections/HomeSection";
import MercatoSection from "./components/sections/MercatoSection";
import PlusvalenzeSection from "./components/sections/PlusvalenzeSection";
import RoseSection from "./components/sections/RoseSection";
import FormazioniSection from "./components/sections/FormazioniSection";
import LiveSection from "./components/sections/LiveSection";
import PremiumInsightsSection from "./components/sections/PremiumInsightsSection";
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

const formatEuro = (value) => {
  if (value === undefined || value === null) return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${n.toFixed(2).replace(".", ",")}€`;
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

const formatDataStatusDate = (value) => {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString("it-IT", {
    timeZone: "Europe/Rome",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
};

const formatCountdown = (secondsValue) => {
  const total = Number(secondsValue);
  if (!Number.isFinite(total) || total <= 0) return "0m";
  const s = Math.floor(total % 60);
  const m = Math.floor((total / 60) % 60);
  const h = Math.floor((total / 3600) % 24);
  const d = Math.floor(total / 86400);
  if (d > 0) return `${d}g ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m ${s}s`;
};

/* ===========================
   CONFIG
=========================== */
const KEY_STORAGE = "fp_access_key";
const ACCESS_TOKEN_STORAGE = "fp_access_token";
const REFRESH_TOKEN_STORAGE = "fp_refresh_token";
const AUTH_LAST_OK_STORAGE = "fp_auth_last_ok_ts";
const SESSION_TTL_MINUTES = 30;
const SESSION_TTL_MS = SESSION_TTL_MINUTES * 60 * 1000;
const MENU_KEYS = new Set([
  "home",
  "stats",
  "rose",
  "classifica-lega",
  "formazioni",
  "formazione-consigliata",
  "live",
  "plusvalenze",
  "listone",
  "top-acquisti",
  "mercato",
  "classifica-fixtures-seriea",
  "player",
  "admin",
]);
const MENU_SECTION_BY_MENU = {
  listone: "generali",
  stats: "generali",
  "classifica-fixtures-seriea": "generali",
  rose: "lega",
  formazioni: "lega",
  "classifica-lega": "lega",
  "top-acquisti": "lega",
  "formazione-consigliata": "extra",
  mercato: "extra",
  plusvalenze: "extra",
};
const API_BASE =
  import.meta.env.VITE_API_BASE ||
  (import.meta.env.DEV
    ? "http://localhost:8001"
    : "https://fantaportoscuso.up.railway.app");
const INSIGHTS_MENU_KEYS = new Set([
  "classifica-fixtures-seriea",
]);

/* ===========================
   APP
=========================== */
export default function App() {
  const deviceId = useMemo(() => getDeviceId(), []);

  /* ===== AUTH ===== */
  const [accessKey, setAccessKey] = useState("");
  const [accessToken, setAccessToken] = useState("");
  const [refreshToken, setRefreshToken] = useState("");
  const [rememberKey, setRememberKey] = useState(false);
  const [loggedIn, setLoggedIn] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [status, setStatus] = useState("idle");
  const [error, setError] = useState("");
  const [initialDataError, setInitialDataError] = useState("");
  const [initialDataLoading, setInitialDataLoading] = useState(false);
  const [authBootstrapped, setAuthBootstrapped] = useState(false);
  const [authRestoring, setAuthRestoring] = useState(false);
  const menuHistoryReadyRef = useRef(false);

  /* ===== UI ===== */
  const [theme, setTheme] = useState("dark");
  const [activeMenu, setActiveMenu] = useState("home");
  const [menuOpen, setMenuOpenState] = useState(false);
  const [adminMenuOpen, setAdminMenuOpenState] = useState(false);
  const [menuSectionsOpen, setMenuSectionsOpen] = useState({
    generali: false,
    lega: false,
    extra: false,
  });

  /* ===== DASHBOARD ===== */
  const [summary, setSummary] = useState({ teams: 0, players: 0 });
  const [dataStatus, setDataStatus] = useState({
    last_update: "",
    result: "error",
    message: "Nessun aggiornamento dati disponibile",
    season: "",
    matchday: null,
    update_id: "",
    steps: {},
  });
  const [premiumInsights, setPremiumInsights] = useState({
    player_tiers: [],
    team_strength_total: [],
    team_strength_starting: [],
    seriea_current_table: [],
    seriea_round: null,
    seriea_rounds: [],
    seriea_fixtures: [],
    seriea_live_table: [],
    generated_at: "",
  });
  const [premiumInsightsLoading, setPremiumInsightsLoading] = useState(false);
  const [premiumInsightsError, setPremiumInsightsError] = useState("");

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
  const [marketStandings, setMarketStandings] = useState([]);
  const [selectedTeam, setSelectedTeam] = useState("");
  const [roster, setRoster] = useState([]);
  const [formations, setFormations] = useState([]);
  const [formationTeam, setFormationTeam] = useState("all");
  const [formationRound, setFormationRound] = useState("");
  const [formationOrder, setFormationOrder] = useState("classifica");
  const [formationMeta, setFormationMeta] = useState({
    round: null,
    source: "projection",
    availableRounds: [],
    orderBy: "classifica",
    orderAllowed: ["classifica", "live_total"],
    note: "",
  });
  const [formationOptimizer, setFormationOptimizer] = useState(null);
  const [formationOptimizerLoading, setFormationOptimizerLoading] = useState(false);
  const [formationOptimizerError, setFormationOptimizerError] = useState("");
  const [livePayload, setLivePayload] = useState({
    round: null,
    available_rounds: [],
    fixtures: [],
    teams: [],
    event_fields: [],
    bonus_malus: {},
  });
  const [liveError, setLiveError] = useState("");
  const [liveLoading, setLiveLoading] = useState(false);
  const [liveSavingKey, setLiveSavingKey] = useState("");
  const [liveImporting, setLiveImporting] = useState(false);
  const [liveFullSyncing, setLiveFullSyncing] = useState(false);
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
  const [topPosFrom, setTopPosFrom] = useState("");
  const [topPosTo, setTopPosTo] = useState("");

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
  const [adminResetNote, setAdminResetNote] = useState("");
  const [adminResetUsage, setAdminResetUsage] = useState(null);
  const [adminImportKeys, setAdminImportKeys] = useState("");
  const [adminImportIsAdmin, setAdminImportIsAdmin] = useState(false);
  const [adminImportTeamKeys, setAdminImportTeamKeys] = useState("");
  const [adminStatus, setAdminStatus] = useState(null);
  const [adminTeamKeys, setAdminTeamKeys] = useState([]);
  const [adminKeyNotesDraft, setAdminKeyNotesDraft] = useState({});
  const [adminSavingNoteKey, setAdminSavingNoteKey] = useState("");
  const [adminDeletingKey, setAdminDeletingKey] = useState("");
  const [adminBlockingKey, setAdminBlockingKey] = useState("");

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

  const touchAuthSessionTs = () => {
    try {
      localStorage.setItem(AUTH_LAST_OK_STORAGE, String(Date.now()));
    } catch {}
  };

  const clearStoredAuthSession = () => {
    try {
      localStorage.removeItem(ACCESS_TOKEN_STORAGE);
      localStorage.removeItem(REFRESH_TOKEN_STORAGE);
      localStorage.removeItem(AUTH_LAST_OK_STORAGE);
    } catch {}
  };

  const parseMenuFromHash = () => {
    try {
      const rawHash = String(window.location.hash || "").replace(/^#/, "");
      const params = new URLSearchParams(rawHash);
      const menu = String(params.get("m") || "").trim();
      if (MENU_KEYS.has(menu)) return menu;
    } catch {}
    return null;
  };

  const updateMenuHistory = (menuKey, replace = false) => {
    if (!loggedIn || !MENU_KEYS.has(String(menuKey || ""))) return;
    const menu = String(menuKey || "");
    const nextHash = `#m=${encodeURIComponent(menu)}`;
    const nextState = { ...(window.history.state || {}), fpMenu: menu };
    if (replace) {
      window.history.replaceState(nextState, "", nextHash);
      return;
    }
    window.history.pushState(nextState, "", nextHash);
  };

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

  const persistTokens = (nextAccessToken, nextRefreshToken) => {
    const safeAccess = String(nextAccessToken || "").trim();
    const safeRefresh = String(nextRefreshToken || "").trim();
    setAccessToken(safeAccess);
    setRefreshToken(safeRefresh);
    try {
      if (safeAccess) {
        localStorage.setItem(ACCESS_TOKEN_STORAGE, safeAccess);
      } else {
        localStorage.removeItem(ACCESS_TOKEN_STORAGE);
      }
      if (safeRefresh) {
        localStorage.setItem(REFRESH_TOKEN_STORAGE, safeRefresh);
      } else {
        localStorage.removeItem(REFRESH_TOKEN_STORAGE);
      }
      if (!safeAccess && !safeRefresh) {
        localStorage.removeItem(AUTH_LAST_OK_STORAGE);
      }
    } catch {}
  };

  const clearAuthSession = (message = "") => {
    setLoggedIn(false);
    setIsAdmin(false);
    setStatus("idle");
    if (message) {
      setError(message);
    }
    persistTokens("", "");
  };

  const refreshAccessToken = async () => {
    const currentRefresh = String(refreshToken || "").trim();
    if (!currentRefresh) return { ok: false, accessToken: "", refreshToken: "" };
    try {
      const res = await fetch(`${API_BASE}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: currentRefresh }),
      });
      if (!res.ok) return { ok: false, accessToken: "", refreshToken: "" };
      const data = await res.json().catch(() => ({}));
      const nextAccess = String(data?.access_token || "").trim();
      if (!nextAccess) return { ok: false, accessToken: "", refreshToken: "" };
      const nextRefresh = String(data?.refresh_token || currentRefresh).trim();
      persistTokens(nextAccess, nextRefresh);
      touchAuthSessionTs();
      return { ok: true, accessToken: nextAccess, refreshToken: nextRefresh };
    } catch {
      return { ok: false, accessToken: "", refreshToken: "" };
    }
  };

  const fetchWithAuth = async (url, options = {}, retryOn401 = true) => {
    const headers = { ...(options.headers || {}) };
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const response = await fetch(url, { ...options, headers });
    if (response.status !== 401 || !retryOn401) return response;

    const refreshed = await refreshAccessToken();
    if (!refreshed.ok) {
      clearAuthSession("Sessione scaduta. Effettua nuovamente il login.");
      return response;
    }

    const retryHeaders = { ...(options.headers || {}) };
    retryHeaders.Authorization = `Bearer ${refreshed.accessToken}`;
    return fetch(url, { ...options, headers: retryHeaders });
  };

  const waitMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const runWithRetry = async (
    runner,
    { attempts = 3, baseDelayMs = 350, shouldRetry = () => true } = {}
  ) => {
    let lastError = null;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await runner();
      } catch (err) {
        lastError = err;
        const retryAllowed = attempt < attempts && shouldRetry(err);
        if (!retryAllowed) break;
        const delay = baseDelayMs * 2 ** (attempt - 1);
        await waitMs(delay);
      }
    }
    throw lastError || new Error("Richiesta fallita");
  };

  const fetchJsonWithRetry = async (
    url,
    options = {},
    { useAuth = false, retryOn401 = true, attempts = 5, baseDelayMs = 600 } = {}
  ) => {
    const retryableStatuses = new Set([408, 429, 500, 502, 503, 504]);
    return runWithRetry(
      async () => {
        const res = useAuth
          ? await fetchWithAuth(url, options, retryOn401)
          : await fetch(url, options);
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) {
          const detail =
            typeof payload?.detail === "string"
              ? payload.detail
              : String(payload?.message || "").trim();
          const err = new Error(detail || `HTTP ${res.status}`);
          err.status = Number(res.status || 0);
          err.retryable = retryableStatuses.has(err.status);
          throw err;
        }
        return payload;
      },
      {
        attempts,
        baseDelayMs,
        shouldRetry: (err) => {
          if (err?.retryable === false) return false;
          const status = Number(err?.status || 0);
          if (status > 0) return retryableStatuses.has(status);
          return true;
        },
      }
    );
  };

  const applyAuthSessionPayload = (data) => {
    setIsAdmin(Boolean(data?.is_admin));
  };

  const loadAuthSession = async () => {
    const keyValue = accessKey.trim().toLowerCase();
    const headers = buildAuthHeaders({ legacyAccessKey: true });
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/session`, { headers }, true);
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        clearAuthSession(payload?.detail || payload?.message || "Sessione non valida.");
        return false;
      }
      setLoggedIn(true);
      applyAuthSessionPayload(payload || {});
      setError("");
      return true;
    } catch {
      if (keyValue) {
        setError("Errore verifica sessione.");
      }
      return false;
    }
  };

  const buildAuthHeaders = ({
    legacyAccessKey = false,
    legacyAdminKey = false,
    extraHeaders = {},
  } = {}) => {
    const headers = { ...extraHeaders };
    const keyValue = accessKey.trim().toLowerCase();
    if (!accessToken && legacyAccessKey && keyValue) {
      headers["X-Access-Key"] = keyValue;
    }
    if (!accessToken && legacyAdminKey && keyValue) {
      headers["X-Admin-Key"] = keyValue;
    }
    return headers;
  };

  const menuItemClass = (menuKey) => {
    const active = activeMenu === menuKey;
    const classes = ["menu-item"];
    if (active) classes.push("active");
    return classes.join(" ");
  };

  const toggleMenuSection = (sectionKey) => {
    const key = String(sectionKey || "").trim();
    if (!key) return;
    setMenuSectionsOpen((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const openMenuSectionForItem = (menuKey) => {
    const sectionKey = MENU_SECTION_BY_MENU[String(menuKey || "").trim()];
    if (!sectionKey) return;
    setMenuSectionsOpen((prev) =>
      prev[sectionKey] ? prev : { ...prev, [sectionKey]: true }
    );
  };

  const openMenuFeature = (menuKey, _featureName, closeMobile = true, closeAdmin = false) => {
    const normalizedMenu = String(menuKey || "").trim();
    setError("");
    if (MENU_KEYS.has(normalizedMenu)) {
      openMenuSectionForItem(normalizedMenu);
      if (activeMenu !== normalizedMenu) {
        setActiveMenu(normalizedMenu);
        if (menuHistoryReadyRef.current) {
          updateMenuHistory(normalizedMenu, false);
        }
      }
    }
    if (closeMobile) setMenuOpen(false);
    if (closeAdmin) setAdminMenuOpen(false);
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
        const detail = d?.detail;
        if (typeof detail === "string") {
          throw new Error(detail || "Accesso non consentito");
        }
        if (detail && typeof detail === "object") {
          throw new Error(detail?.message || "Accesso non consentito");
        }
        throw new Error(d?.message || "Accesso non consentito");
      }

      const data = await res.json();
      setLoggedIn(true);
      applyAuthSessionPayload(data);
      setStatus("success");
      persistTokens(data?.access_token || "", data?.refresh_token || "");
      touchAuthSessionTs();
      setError("");

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
      persistTokens("", "");
      setError(err.message || "Errore login");
    }
  };
  /* ===========================
     LOADERS (FETCH)
  =========================== */
  const loadSummary = async () => {
    try {
      const data = await fetchJsonWithRetry(`${API_BASE}/data/summary`);
      setSummary(data);
      return true;
    } catch {
      console.warn("loadSummary failed");
      return false;
    }
  };

  const loadDataStatus = async () => {
    try {
      const data = await fetchJsonWithRetry(`${API_BASE}/meta/data-status`);
      const rawMatchday = data?.matchday;
      let parsedMatchday = null;
      if (
        rawMatchday !== undefined &&
        rawMatchday !== null &&
        rawMatchday !== ""
      ) {
        const numericMatchday = Number(rawMatchday);
        if (Number.isFinite(numericMatchday)) {
          parsedMatchday = numericMatchday;
        }
      }
      const normalized = {
        last_update: String(data?.last_update || ""),
        result: (() => {
          const rawResult = String(data?.result || "").trim().toLowerCase();
          return ["ok", "error", "running"].includes(rawResult)
            ? rawResult
            : "error";
        })(),
        message:
          String(data?.message || "").trim() ||
          "Nessun aggiornamento dati disponibile",
        season: String(data?.season || ""),
        matchday: parsedMatchday,
        update_id: String(data?.update_id || ""),
        steps: (() => {
          const rawSteps =
            data?.steps && typeof data.steps === "object" ? data.steps : {};
          const allowed = new Set(["pending", "running", "ok", "error"]);
          const normalizedSteps = {};
          ["rose", "stats", "strength", "quotazioni"].forEach((key) => {
            const value = String(rawSteps[key] || "").trim().toLowerCase();
            if (allowed.has(value)) {
              normalizedSteps[key] = value;
            }
          });
          return normalizedSteps;
        })(),
      };
      setDataStatus(normalized);
      return true;
    } catch {
      setDataStatus((prev) => ({
        ...prev,
        result: "error",
        message: "Errore nel recupero stato dati",
        update_id: "",
        steps: {},
      }));
      return false;
    }
  };

  const loadTeams = async () => {
    try {
      const data = await fetchJsonWithRetry(`${API_BASE}/data/teams`);
      const items = (data.items || [])
        .slice()
        .sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" }));
      setTeams(items);
      if (items.length && !selectedTeam) setSelectedTeam(items[0]);
      return true;
    } catch {
      console.warn("loadTeams failed");
      return false;
    }
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
    } catch {
      console.warn("loadRoster failed");
    }
  };

  const loadMarketStandings = async () => {
    try {
      const data = await fetchJsonWithRetry(`${API_BASE}/data/standings?live=1`);
      setMarketStandings(Array.isArray(data.items) ? data.items : []);
      return true;
    } catch {
      console.warn("loadMarketStandings failed");
      return false;
    }
  };

  const loadFormazioni = async (roundValue = null, orderValue = null) => {
    try {
      const params = new URLSearchParams({ limit: "300" });
      const parsedRound = Number(roundValue);
      if (Number.isFinite(parsedRound) && parsedRound > 0) {
        params.set("round", String(parsedRound));
      }
      const normalizedOrder = String(orderValue || formationOrder || "classifica")
        .trim()
        .toLowerCase();
      if (normalizedOrder === "classifica" || normalizedOrder === "live_total") {
        params.set("order_by", normalizedOrder);
      }
      const data = await fetchJsonWithRetry(
        `${API_BASE}/data/formazioni?${params.toString()}`,
        {
          headers: buildAuthHeaders({ legacyAccessKey: true }),
        },
        { useAuth: true }
      );
      setFormations(Array.isArray(data.items) ? data.items : []);
      const apiRound = Number(data?.round);
      const normalizedRound = Number.isFinite(apiRound) ? apiRound : null;
      const availableRounds = Array.isArray(data?.available_rounds)
        ? data.available_rounds
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value))
            .sort((a, b) => a - b)
        : [];
      setFormationMeta({
        round: normalizedRound,
        source: String(data?.source || "").toLowerCase() === "real" ? "real" : "projection",
        availableRounds,
        orderBy:
          String(data?.order_by || "").toLowerCase() === "live_total"
            ? "live_total"
            : "classifica",
        orderAllowed: Array.isArray(data?.order_allowed)
          ? data.order_allowed
              .map((value) => String(value || "").trim().toLowerCase())
              .filter((value) => value === "classifica" || value === "live_total")
          : ["classifica", "live_total"],
        note: String(data?.note || "").trim(),
      });
      const nextOrder =
        String(data?.order_by || "").toLowerCase() === "live_total"
          ? "live_total"
          : "classifica";
      setFormationOrder(nextOrder);
      if (normalizedRound !== null) {
        setFormationRound(String(normalizedRound));
      } else if (availableRounds.length) {
        setFormationRound(String(availableRounds[availableRounds.length - 1]));
      } else {
        setFormationRound("");
      }
      return true;
    } catch {
      console.warn("loadFormazioni failed");
      return false;
    }
  };

  const onFormationRoundChange = (nextRound) => {
    setFormationRound(nextRound);
    loadFormazioni(nextRound, formationOrder);
  };

  const onFormationOrderChange = (nextOrder) => {
    const normalizedOrder =
      String(nextOrder || "").toLowerCase() === "live_total" ? "live_total" : "classifica";
    setFormationOrder(normalizedOrder);
    loadFormazioni(formationRound || null, normalizedOrder);
  };

  const runFormationOptimizer = async (teamName, roundValue = null) => {
    const safeTeam = String(teamName || "").trim();
    if (!safeTeam || safeTeam.toLowerCase() === "all") {
      setFormationOptimizer(null);
      setFormationOptimizerError("");
      return;
    }
    try {
      setFormationOptimizerLoading(true);
      setFormationOptimizerError("");
      const params = new URLSearchParams({ team: safeTeam });
      const parsedRound = Number(roundValue || formationRound || 0);
      if (Number.isFinite(parsedRound) && parsedRound > 0) {
        params.set("round", String(parsedRound));
      }
      const res = await fetchWithAuth(
        `${API_BASE}/data/formazioni/optimizer?${params.toString()}`,
        { headers: buildAuthHeaders({ legacyAccessKey: true }) }
      );
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        throw new Error(payload?.detail || "Errore calcolo XI ottimizzata");
      }
      const data = await res.json();
      setFormationOptimizer(data || null);
    } catch (err) {
      setFormationOptimizer(null);
      setFormationOptimizerError(err?.message || "Errore calcolo XI ottimizzata");
    } finally {
      setFormationOptimizerLoading(false);
    }
  };

  const loadLivePayload = async (roundValue = null) => {
    if (!loggedIn) return;
    try {
      setLiveLoading(true);
      setLiveError("");
      const params = new URLSearchParams();
      const parsedRound = Number(roundValue);
      if (Number.isFinite(parsedRound) && parsedRound > 0) {
        params.set("round", String(parsedRound));
      }
      const queryString = params.toString();
      const endpoint = `${API_BASE}/data/live/payload${queryString ? `?${queryString}` : ""}`;
      const res = await fetchWithAuth(endpoint, {
        headers: buildAuthHeaders({
          legacyAccessKey: true,
          legacyAdminKey: isAdmin,
        }),
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        throw new Error(payload?.detail || "Errore caricamento dati live");
      }
      const data = await res.json();
      setLivePayload({
        round: Number.isFinite(Number(data?.round)) ? Number(data.round) : null,
        available_rounds: Array.isArray(data?.available_rounds) ? data.available_rounds : [],
        fixtures: Array.isArray(data?.fixtures) ? data.fixtures : [],
        teams: Array.isArray(data?.teams) ? data.teams : [],
        event_fields: Array.isArray(data?.event_fields) ? data.event_fields : [],
        bonus_malus:
          data?.bonus_malus && typeof data.bonus_malus === "object"
            ? data.bonus_malus
            : {},
      });
    } catch (err) {
      setLiveError(err?.message || "Errore caricamento dati live");
    } finally {
      setLiveLoading(false);
    }
  };

  const onLiveRoundChange = async (nextRound) => {
    await loadLivePayload(nextRound);
  };

  const saveLiveMatchSix = async (payload) => {
    if (!loggedIn || !isAdmin) return;
    try {
      setLiveLoading(true);
      setLiveError("");
      const res = await fetchWithAuth(`${API_BASE}/data/live/match-six`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data?.detail || "Errore aggiornamento 6 politico");
      }
      await loadLivePayload(livePayload?.round || payload?.round || null);
      await loadFormazioni(
        formationRound || livePayload?.round || payload?.round || null,
        formationOrder
      );
    } catch (err) {
      setLiveError(err?.message || "Errore aggiornamento 6 politico");
    } finally {
      setLiveLoading(false);
    }
  };

  const saveLivePlayerVote = async (payload) => {
    if (!loggedIn || !isAdmin) return;
    const rowKey = payload?.rowKey || "";
    try {
      setLiveSavingKey(rowKey);
      setLiveError("");
      const eventFields = [
        "goal",
        "assist",
        "assist_da_fermo",
        "rigore_segnato",
        "rigore_parato",
        "rigore_sbagliato",
        "gol_subito_portiere",
        "ammonizione",
        "espulsione",
        "autogol",
        "gol_vittoria",
        "gol_pareggio",
      ];
      const body = {
        round: payload?.round,
        team: payload?.team,
        player: payload?.player,
        role: payload?.role || "",
        vote: payload?.vote,
        is_sv: Boolean(payload?.is_sv),
        is_absent: Boolean(payload?.is_absent),
      };
      eventFields.forEach((field) => {
        body[field] = payload?.[field] ?? 0;
      });
      const res = await fetchWithAuth(`${API_BASE}/data/live/player`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data?.detail || "Errore salvataggio voto live");
      }
      await loadLivePayload(livePayload?.round || payload?.round || null);
      await loadFormazioni(
        formationRound || livePayload?.round || payload?.round || null,
        formationOrder
      );
    } catch (err) {
      setLiveError(err?.message || "Errore salvataggio voto live");
    } finally {
      setLiveSavingKey("");
    }
  };

  const importLiveVotes = async (roundValue = null) => {
    if (!loggedIn || !isAdmin) return;
    try {
      setLiveImporting(true);
      setLiveError("");
      const roundNumber = Number(roundValue || livePayload?.round || formationRound || 0);
      if (!Number.isFinite(roundNumber) || roundNumber <= 0) {
        throw new Error("Giornata non valida per import voti");
      }
      const res = await fetchWithAuth(`${API_BASE}/data/live/import-voti`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ round: roundNumber }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data?.detail || "Errore import voti live");
      }
      await loadLivePayload(roundNumber);
      await loadFormazioni(formationRound || roundNumber, formationOrder);
      await loadMarketStandings();
      await loadPremiumInsights(true);
    } catch (err) {
      setLiveError(err?.message || "Errore import voti live");
    } finally {
      setLiveImporting(false);
    }
  };

  const runFullSyncTotal = async (roundValue = null) => {
    if (!loggedIn || !isAdmin) return;
    try {
      setLiveFullSyncing(true);
      setLiveError("");

      const roundNumber = Number(
        roundValue || livePayload?.round || formationRound || dataStatus?.matchday || 0
      );
      const params = new URLSearchParams({
        run_pipeline: "true",
        fetch_quotazioni: "true",
        fetch_global_stats: "true",
        background: "true",
      });
      if (Number.isFinite(roundNumber) && roundNumber > 0) {
        params.set("formations_matchday", String(roundNumber));
      }

      const res = await fetchWithAuth(
        `${API_BASE}/data/admin/leghe/sync-complete?${params.toString()}`,
        {
          method: "POST",
          headers: buildAuthHeaders({ legacyAdminKey: true }),
        }
      );
      const payload = await res.json().catch(() => ({}));
      if (!res.ok || payload?.ok === false) {
        const detail =
          payload?.detail?.error ||
          payload?.detail ||
          payload?.error ||
          "Errore sync completa totale";
        throw new Error(String(detail));
      }

      if (payload?.queued || payload?.running) {
        setAdminNotice(
          payload?.message || "Sync completa totale avviata in background."
        );
        await loadDataStatus();

        void (async () => {
          const maxChecks = 180;
          const waitMsStep = 5000;
          let lastStatus = null;
          for (let index = 0; index < maxChecks; index += 1) {
            try {
              const status = await fetchJsonWithRetry(`${API_BASE}/meta/data-status`);
              lastStatus = status;
              const resultValue = String(status?.result || "").toLowerCase();
              if (resultValue !== "running") break;
            } catch {}
            await waitMs(waitMsStep);
          }

          await loadDataStatus();
          await loadLivePayload(roundNumber || null);
          await loadFormazioni(formationRound || roundNumber || null, formationOrder);
          await loadMarketStandings();
          await loadPremiumInsights(true);
          await loadListone();
          await loadTopQuotesAllRoles();
          await loadStatList(statsTab);
          await loadPlusvalenze();
          await loadAllPlusvalenze();

          const finalResult = String(lastStatus?.result || "").toLowerCase();
          if (finalResult === "ok") {
            setAdminNotice("Sync completa totale terminata.");
          } else if (finalResult === "error") {
            const msg = String(lastStatus?.message || "").trim();
            setLiveError(msg || "Sync completa totale terminata con errore.");
          }
        })();
        return;
      }

      const syncedRound = Number(payload?.round);
      const effectiveRound =
        Number.isFinite(syncedRound) && syncedRound > 0
          ? syncedRound
          : Number.isFinite(roundNumber) && roundNumber > 0
          ? roundNumber
          : null;
      const liveImportOk = payload?.live_import?.ok !== false;
      const warningText = Array.isArray(payload?.warnings) ? payload.warnings.join(" | ") : "";
      setAdminNotice(
        liveImportOk
          ? `Sync completa totale completata${warningText ? ` (warning: ${warningText})` : ""}.`
          : `Sync completa totale completata con warning live import. ${warningText}`.trim()
      );

      await loadDataStatus();
      await loadLivePayload(effectiveRound);
      await loadFormazioni(formationRound || effectiveRound, formationOrder);
      await loadMarketStandings();
      await loadPremiumInsights(true);
      await loadListone();
      await loadTopQuotesAllRoles();
      await loadStatList(statsTab);
      await loadPlusvalenze();
      await loadAllPlusvalenze();
    } catch (err) {
      setLiveError(err?.message || "Errore sync completa totale");
    } finally {
      setLiveFullSyncing(false);
    }
  };

  const loadListone = async () => {
    try {
      const data = await fetchJsonWithRetry(
        `${API_BASE}/data/listone?ruolo=${encodeURIComponent(
          quoteRole
        )}&order=${encodeURIComponent(quoteOrder)}&limit=200`
      );
      setQuoteList(data.items || []);
      return true;
    } catch {
      console.warn("loadListone failed");
      return false;
    }
  };

  const loadTopQuotesAllRoles = async () => {
    try {
      const roles = ["P", "D", "C", "A"];
      const responses = await Promise.all(
        roles.map((role) =>
          fetchJsonWithRetry(
            `${API_BASE}/data/listone?ruolo=${encodeURIComponent(
              role
            )}&order=price_desc&limit=200`
          )
        )
      );
      const items = [];
      for (const data of responses) {
        items.push(...(data.items || []));
      }
      items.sort(
        (a, b) => Number(b.PrezzoAttuale || 0) - Number(a.PrezzoAttuale || 0)
      );
      setTopQuotesAll(items);
      return true;
    } catch {
      console.warn("loadTopQuotesAllRoles failed");
      return false;
    }
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
      const data = await fetchJsonWithRetry(
        `${API_BASE}/data/stats/plusvalenze?limit=5&include_negatives=false&period=${encodeURIComponent(
          plusvalenzePeriod
        )}`
      );
      setPlusvalenze(data.items || []);
      return true;
    } catch {
      console.warn("loadPlusvalenze failed");
      return false;
    }
  };

  const loadAllPlusvalenze = async () => {
    try {
      const data = await fetchJsonWithRetry(
        `${API_BASE}/data/stats/plusvalenze?limit=200&include_negatives=true&period=${encodeURIComponent(
          plusvalenzePeriod
        )}`
      );
      setAllPlusvalenze(data.items || []);
      return true;
    } catch {
      console.warn("loadAllPlusvalenze failed");
      return false;
    }
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
      const data = await fetchJsonWithRetry(
        `${API_BASE}/data/stats/${encodeURIComponent(tab)}?limit=300`
      );
      setStatsItems(data.items || []);
      return true;
    } catch {
      console.warn("loadStatList failed");
      return false;
    }
  };

  const loadInitialData = async ({ silent = false } = {}) => {
    if (!silent) {
      setInitialDataLoading(true);
    }
    setInitialDataError("");
    try {
      const tasks = [
        { key: "summary", label: "Summary", run: loadSummary },
        { key: "status", label: "Data status", run: loadDataStatus },
        { key: "teams", label: "Teams", run: loadTeams },
        { key: "standings", label: "Standings", run: loadMarketStandings },
        { key: "formazioni", label: "Formazioni", run: loadFormazioni },
        { key: "plus_top", label: "Plusvalenze top", run: loadPlusvalenze },
        { key: "plus_all", label: "Plusvalenze all", run: loadAllPlusvalenze },
        { key: "listone", label: "Listone", run: loadListone },
        { key: "quotes_all", label: "Top quotazioni", run: loadTopQuotesAllRoles },
      ];

      let pending = tasks.slice();
      let failedLabels = [];

      for (let pass = 1; pass <= 2; pass += 1) {
        const checks = await Promise.allSettled(pending.map((task) => task.run()));
        failedLabels = [];
        const nextPending = [];
        checks.forEach((result, index) => {
          if (result.status === "rejected" || result.value === false) {
            const task = pending[index];
            failedLabels.push(task.label);
            nextPending.push(task);
          }
        });
        if (!failedLabels.length) {
          return true;
        }
        if (pass < 2) {
          await waitMs(2500);
          pending = nextPending;
        }
      }

      const preview =
        failedLabels.length <= 4
          ? failedLabels.join(", ")
          : `${failedLabels.slice(0, 4).join(", ")}, ...`;
      setInitialDataError(
        `Alcuni dati non sono stati caricati (${failedLabels.length}/9): ${preview}. Premi "Riprova".`
      );
      return false;
    } finally {
      if (!silent) {
        setInitialDataLoading(false);
      }
    }
  };

  const retryInitialDataLoad = async () => {
    await loadInitialData();
    await loadStatList(statsTab);
    if (loggedIn && INSIGHTS_MENU_KEYS.has(String(activeMenu || "").trim())) {
      await loadPremiumInsights(true);
    }
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
      const responses = [];
      const chunkSize = 8;
      for (let idx = 0; idx < teams.length; idx += chunkSize) {
        const chunk = teams.slice(idx, idx + chunkSize);
        const chunkResponses = await Promise.all(
          chunk.map(async (team) => {
            try {
              const data = await fetchJsonWithRetry(
                `${API_BASE}/data/team/${encodeURIComponent(team)}`,
                {},
                { attempts: 2, baseDelayMs: 250 }
              );
              return { team, items: data.items || [] };
            } catch {
              return { team, items: [] };
            }
          })
        );
        responses.push(...chunkResponses);
        if (idx + chunkSize < teams.length) {
          await waitMs(120);
        }
      }

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
    openMenuFeature("player", null, false, false);

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
    } catch {
      console.warn("openPlayer failed");
    }
  };

  /* ===========================
     NAV HELPERS (menu jumps)
  =========================== */
  const jumpToId = (id, menu, after = () => {}) => {
    if (menu) openMenuFeature(menu, null, false, false);
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
    openMenuFeature("rose", null, false, false);
    setSelectedTeam(team);
  };

  const topAcquistiLoaded = useMemo(
    () =>
      ["P", "D", "C", "A"].some((role) =>
        Array.isArray(topPlayersByRole?.[role]) && topPlayersByRole[role].length > 0
      ),
    [topPlayersByRole]
  );

  const goToSquadra = (squadra, role) => {
    if (!squadra) return;
    openMenuFeature("listone", null, false, false);
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
    const posMap = new Map();
    for (const row of marketStandings || []) {
      const team = String(row.team || row.Team || row.Squadra || "").trim();
      const posNum = Number(row.pos ?? row.Pos ?? row.posizione ?? row.Posizione);
      if (!team || !Number.isFinite(posNum)) continue;
      posMap.set(normalizeName(team), Math.trunc(posNum));
    }

    const maxPos = Math.max(84, ...(Array.from(posMap.values()).length ? Array.from(posMap.values()) : [84]));
    const fromRaw = Number.parseInt(String(topPosFrom || "").trim(), 10);
    const toRaw = Number.parseInt(String(topPosTo || "").trim(), 10);
    const hasFrom = Number.isFinite(fromRaw);
    const hasTo = Number.isFinite(toRaw);
    let from = hasFrom ? Math.max(1, fromRaw) : 1;
    let to = hasTo ? Math.max(1, toRaw) : maxPos;
    if (from > to) [from, to] = [to, from];
    const rangeActive = hasFrom || hasTo;

    const list = (topPlayersByRole[activeTopRole] || [])
      .map((item) => {
        const teams = Array.isArray(item.teams) ? item.teams : [];
        const teamsInRange = teams.filter((teamName) => {
          const pos = posMap.get(normalizeName(teamName));
          if (!Number.isFinite(pos)) return false;
          return pos >= from && pos <= to;
        });
        const rangeCount = teamsInRange.length;
        if (rangeActive && rangeCount === 0) return null;
        return {
          ...item,
          countTotal: Number(item.count || 0),
          count: rangeActive ? rangeCount : Number(item.count || 0),
          teams: rangeActive ? teamsInRange : teams,
        };
      })
      .filter(Boolean)
      .filter((p) => {
        if (!q) return true;
        return String(p.name || "")
          .toLowerCase()
          .includes(q);
      })
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        if ((b.countTotal || 0) !== (a.countTotal || 0)) {
          return (b.countTotal || 0) - (a.countTotal || 0);
        }
        return String(a.name || "").localeCompare(String(b.name || ""), "it", {
          sensitivity: "base",
        });
      })
      .map((item, index) => ({
        ...item,
        rank: index + 1,
      }));
    return list;
  }, [
    topPlayersByRole,
    activeTopRole,
    topAcquistiQuery,
    marketStandings,
    topPosFrom,
    topPosTo,
    isAdmin,
  ]);

  const topAcquistiRangeLabel = useMemo(() => {
    const fromRaw = Number.parseInt(String(topPosFrom || "").trim(), 10);
    const toRaw = Number.parseInt(String(topPosTo || "").trim(), 10);
    const hasFrom = Number.isFinite(fromRaw);
    const hasTo = Number.isFinite(toRaw);
    if (!hasFrom && !hasTo) return "Tutte le posizioni";
    const from = hasFrom ? Math.max(1, fromRaw) : 1;
    const to = hasTo ? Math.max(1, toRaw) : 84;
    const low = Math.min(from, to);
    const high = Math.max(from, to);
    return `Range attivo: #${low} - #${high}`;
  }, [topPosFrom, topPosTo]);

  const resetTopAcquistiFilters = () => {
    setTopAcquistiQuery("");
    setTopPosFrom("");
    setTopPosTo("");
  };

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
      const res = await fetchWithAuth(`${API_BASE}/data/market/payload`, {
        headers: buildAuthHeaders({ legacyAccessKey: true }),
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
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/keys`, {
        headers: buildAuthHeaders({ legacyAdminKey: true }),
      });
      if (!res.ok) return;
      const data = await res.json();
      const items = Array.isArray(data) ? data : [];
      setAdminKeys(items);
      setAdminKeyNotesDraft(() => {
        const next = {};
        items.forEach((item) => {
          const keyValue = String(item?.key || "").trim().toLowerCase();
          if (!keyValue) return;
          next[keyValue] = String(item?.note || "");
        });
        return next;
      });
    } catch {
      console.warn("loadAdminKeys failed");
    }
  };

  const createNewKey = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/keys`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
      });
      if (!res.ok) return;
      const data = await res.json();
      setNewKey(data.key || "");
      setAdminNotice("Key creata.");
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore durante la creazione della key.");
    }
  };

  const loadAdminStatus = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/status`, {
        headers: buildAuthHeaders({ legacyAdminKey: true }),
      });
      if (!res.ok) return;
      const data = await res.json();
      setAdminStatus(data || null);
    } catch {
      console.warn("loadAdminStatus failed");
    }
  };

  const loadAdminTeamKeys = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/team-keys`, {
        headers: buildAuthHeaders({ legacyAdminKey: true }),
      });
      if (!res.ok) return;
      const data = await res.json();
      setAdminTeamKeys(data || []);
    } catch {
      console.warn("loadAdminTeamKeys failed");
    }
  };

  const refreshMarketAdmin = async () => {
    if (!isAdmin) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/data/admin/market/refresh`, {
        method: "POST",
        headers: buildAuthHeaders({ legacyAdminKey: true }),
      });
      if (!res.ok) return;
      setAdminNotice("Mercato aggiornato.");
      loadAdminStatus();
      loadMarket();
    } catch {
      setAdminNotice("Errore aggiornamento mercato.");
    }
  };

  const setAdminForKey = async () => {
    if (!isAdmin) return;
    const key = adminSetAdminKey.trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/set-admin`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key, is_admin: true }),
      });
      if (!res.ok) return;
      setAdminNotice(`Key ${key.toUpperCase()} promossa ad admin.`);
      setAdminSetAdminKey("");
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore promozione key ad admin.");
    }
  };

  const assignTeamKey = async () => {
    if (!isAdmin) return;
    const key = adminTeamKey.trim().toLowerCase();
    const team = adminTeamName.trim();
    if (!key || !team) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/team-key`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key, team }),
      });
      if (!res.ok) return;
      setAdminNotice(`Key ${key.toUpperCase()} associata a ${team}.`);
      setAdminTeamKey("");
      setAdminTeamName("");
      loadAdminTeamKeys();
    } catch {
      setAdminNotice("Errore associazione team-key.");
    }
  };

  const loadAdminResetUsage = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) {
      setAdminResetUsage(null);
      return;
    }
    try {
      const res = await fetchWithAuth(
        `${API_BASE}/auth/admin/reset-usage?key=${encodeURIComponent(key)}`,
        {
          headers: buildAuthHeaders({ legacyAdminKey: true }),
        }
      );
      if (!res.ok) {
        setAdminResetUsage(null);
        return;
      }
      const data = await res.json();
      setAdminResetUsage(data || null);
    } catch {
      setAdminResetUsage(null);
    }
  };

  const resetKeyAdmin = async () => {
    if (!isAdmin) return;
    const key = adminResetKey.trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/reset-key`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key, note: adminResetNote.trim() || null }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || "Errore durante il reset key.");
        loadAdminResetUsage(key);
        return;
      }
      setAdminNotice(
        `Key ${key.toUpperCase()} resettata. Reset usati: ${data?.used ?? "-"}${
          data?.limit ? `/${data.limit}` : "/3"
        }`
      );
      setAdminResetNote("");
      loadAdminKeys();
      loadAdminResetUsage(key);
    } catch {
      setAdminNotice("Errore durante il reset key.");
    }
  };

  const importKeysAdmin = async () => {
    if (!isAdmin) return;
    const raw = adminImportKeys
      .split(/[\s,;]+/)
      .map((k) => k.trim())
      .filter(Boolean);
    if (!raw.length) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/import-keys`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ keys: raw, is_admin: adminImportIsAdmin }),
      });
      if (!res.ok) return;
      setAdminNotice(`Importate ${raw.length} key.`);
      setAdminImportKeys("");
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore importazione key.");
    }
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
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/import-team-keys`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ items }),
      });
      if (!res.ok) return;
      setAdminNotice(`Importate ${items.length} associazioni team.`);
      setAdminImportTeamKeys("");
      loadAdminTeamKeys();
    } catch {
      setAdminNotice("Errore importazione associazioni team.");
    }
  };

  const updateAdminKeyNoteDraft = (keyValue, noteValue) => {
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    setAdminKeyNotesDraft((prev) => ({ ...prev, [key]: String(noteValue || "") }));
  };

  const saveAdminKeyNote = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    setAdminSavingNoteKey(key);
    try {
      const note = String(adminKeyNotesDraft[key] ?? "");
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/key-note`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key, note }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || "Errore salvataggio nota.");
        return;
      }
      const savedNote = String(data?.note || "");
      setAdminKeys((prev) =>
        prev.map((item) =>
          String(item?.key || "").trim().toLowerCase() === key
            ? { ...item, note: savedNote }
            : item
        )
      );
      setAdminKeyNotesDraft((prev) => ({ ...prev, [key]: savedNote }));
      setAdminNotice(
        savedNote
          ? `Nota salvata per ${key.toUpperCase()}.`
          : `Nota rimossa per ${key.toUpperCase()}.`
      );
    } catch {
      setAdminNotice("Errore salvataggio nota.");
    } finally {
      setAdminSavingNoteKey("");
    }
  };

  const blockAdminKey = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    if (adminBlockingKey === key) return;
    setAdminBlockingKey(key);
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/key-block`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || "Errore blocco key.");
        return;
      }
      setAdminNotice(`Key ${key.toUpperCase()} bloccata fino a sblocco manuale.`);
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore blocco key.");
    } finally {
      setAdminBlockingKey("");
    }
  };

  const unblockAdminKey = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    if (adminBlockingKey === key) return;
    setAdminBlockingKey(key);
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/key-unblock`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || "Errore sblocco key.");
        return;
      }
      setAdminNotice(`Key ${key.toUpperCase()} sbloccata.`);
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore sblocco key.");
    } finally {
      setAdminBlockingKey("");
    }
  };

  const deleteAdminKey = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    if (adminDeletingKey === key) return;
    setAdminDeletingKey(key);
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/keys`, {
        method: "DELETE",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || "Errore eliminazione key.");
        return;
      }
      setAdminNotice(`Key eliminata: ${key.toUpperCase()}.`);
      setAdminKeyNotesDraft((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      loadAdminStatus();
      loadAdminTeamKeys();
      loadAdminKeys();
    } catch {
      setAdminNotice("Errore eliminazione key.");
    } finally {
      setAdminDeletingKey("");
    }
  };

  const loadPremiumInsights = async (force = false, options = {}) => {
    const silent = Boolean(options?.silent);
    if (!loggedIn) return false;
    if (
      !force &&
      Array.isArray(premiumInsights?.player_tiers) &&
      premiumInsights.player_tiers.length > 0
    ) {
      return true;
    }
    try {
      if (!silent) {
        setPremiumInsightsLoading(true);
        setPremiumInsightsError("");
      }
      const res = await fetchWithAuth(`${API_BASE}/data/insights/premium`, {
        headers: buildAuthHeaders({ legacyAccessKey: true }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.detail || "Errore caricamento insight premium");
      }
      setPremiumInsights({
        player_tiers: Array.isArray(payload?.player_tiers) ? payload.player_tiers : [],
        team_strength_total: Array.isArray(payload?.team_strength_total)
          ? payload.team_strength_total
          : [],
        team_strength_starting: Array.isArray(payload?.team_strength_starting)
          ? payload.team_strength_starting
          : [],
        seriea_current_table: Array.isArray(payload?.seriea_current_table)
          ? payload.seriea_current_table
          : [],
        seriea_round: (() => {
          const raw =
            payload?.seriea_round === null ||
            payload?.seriea_round === undefined ||
            payload?.seriea_round === ""
              ? null
              : Number(payload.seriea_round);
          return Number.isFinite(raw) ? raw : null;
        })(),
        seriea_rounds: Array.isArray(payload?.seriea_rounds) ? payload.seriea_rounds : [],
        seriea_fixtures: Array.isArray(payload?.seriea_fixtures) ? payload.seriea_fixtures : [],
        seriea_live_table: Array.isArray(payload?.seriea_live_table) ? payload.seriea_live_table : [],
        generated_at: String(payload?.generated_at || ""),
      });
      return true;
    } catch (err) {
      if (!silent) {
        setPremiumInsightsError(err?.message || "Errore caricamento insight premium");
      }
      return false;
    } finally {
      if (!silent) {
        setPremiumInsightsLoading(false);
      }
    }
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
      const savedAccessToken = String(localStorage.getItem(ACCESS_TOKEN_STORAGE) || "").trim();
      const savedRefreshToken = String(localStorage.getItem(REFRESH_TOKEN_STORAGE) || "").trim();
      if (savedAccessToken || savedRefreshToken) {
        const rawTs = localStorage.getItem(AUTH_LAST_OK_STORAGE);
        const ts = Number(rawTs || "");
        const hasTs = Number.isFinite(ts) && ts > 0;
        const isFresh = hasTs ? Date.now() - ts <= SESSION_TTL_MS : true;
        if (isFresh) {
          setAccessToken(savedAccessToken);
          setRefreshToken(savedRefreshToken);
          if (!hasTs) {
            touchAuthSessionTs();
          }
        } else {
          clearStoredAuthSession();
        }
      }
    } catch {}
    setAuthBootstrapped(true);
  }, []);

  useEffect(() => {
    if (!authBootstrapped || loggedIn || (!accessToken && !refreshToken)) {
      setAuthRestoring(false);
      return;
    }
    let cancelled = false;
    setAuthRestoring(true);
    loadAuthSession().finally(() => {
      if (!cancelled) setAuthRestoring(false);
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authBootstrapped, loggedIn, accessToken, refreshToken]);

  useEffect(() => {
    if (!loggedIn) {
      menuHistoryReadyRef.current = false;
      return;
    }
    const hashMenu = parseMenuFromHash();
    const initialMenu = hashMenu || "home";
    setActiveMenu(initialMenu);
    updateMenuHistory(initialMenu, true);
    menuHistoryReadyRef.current = true;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn]);

  useEffect(() => {
    if (!loggedIn) return;
    const onPopState = () => {
      const stateMenu = String(window.history.state?.fpMenu || "").trim();
      const hashMenu = parseMenuFromHash();
      const nextMenu = MENU_KEYS.has(stateMenu) ? stateMenu : hashMenu || "home";
      setActiveMenu(nextMenu);
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn]);

  useEffect(() => {
    try {
      const stored = localStorage.getItem("fp_theme");
      const next = stored === "light" ? "light" : "dark";
      setTheme(next);
      document.body.classList.toggle("theme-light", next === "light");
    } catch {}
  }, []);

  useEffect(() => {
    loadInitialData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!loggedIn) return;
    loadInitialData({ silent: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn]);

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
    if (!loggedIn || !isAdmin) return;
    loadAdminResetUsage(adminResetKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [adminResetKey, loggedIn, isAdmin]);

  useEffect(() => {
    if (!loggedIn || !accessKey.trim()) return;
    const ping = async () => {
      try {
        await fetchWithAuth(`${API_BASE}/auth/ping`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ key: accessKey.trim(), device_id: deviceId }),
        });
      } catch {}
      try {
        await loadAuthSession();
      } catch {}
    };
    ping();
    const timer = setInterval(ping, 60000);
    return () => clearInterval(timer);
  }, [loggedIn, accessKey, deviceId, accessToken, refreshToken]);

  useEffect(() => {
    if (!loggedIn || !teams.length) return;
    if (activeMenu !== "top-acquisti") return;
    if (topAcquistiLoaded || aggregatesLoading) return;
    loadLeagueAggregates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, teams, activeMenu, topAcquistiLoaded, aggregatesLoading]);

  useEffect(() => {
    if (!loggedIn) return;
    loadRoster(selectedTeam);
    setRoleFilter("all");
    setSquadraFilter("all");
    setRosterQuery("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, selectedTeam]);

  useEffect(() => {
    setFormationOptimizer(null);
    setFormationOptimizerError("");
  }, [formationTeam, formationRound]);

  useEffect(() => {
    if (!loggedIn) return;
    if (activeMenu !== "formazione-consigliata") return;

    const fallbackTeam =
      String(suggestTeam || "").trim() ||
      String(selectedTeam || "").trim() ||
      String(teams?.[0] || "").trim();
    const activeTeamValue =
      String(formationTeam || "").trim().toLowerCase() === "all"
        ? fallbackTeam
        : String(formationTeam || "").trim();

    if (!activeTeamValue) return;
    if (String(formationTeam || "").trim().toLowerCase() === "all") {
      setFormationTeam(activeTeamValue);
      return;
    }
    runFormationOptimizer(activeTeamValue, formationRound || null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu, formationTeam, formationRound, suggestTeam, selectedTeam, teams]);

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

  useEffect(() => {
    if (!loggedIn || activeMenu !== "live") return;
    loadLivePayload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, isAdmin, activeMenu]);

  useEffect(() => {
    if (!loggedIn) return;
    if (!INSIGHTS_MENU_KEYS.has(String(activeMenu || "").trim())) return;
    const forceRefresh = String(activeMenu || "").trim() === "classifica-fixtures-seriea";
    loadPremiumInsights(forceRefresh);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu, isAdmin]);

  useEffect(() => {
    if (!loggedIn) return;
    if (String(activeMenu || "").trim() !== "classifica-fixtures-seriea") return;
    const timer = setInterval(() => {
      loadPremiumInsights(true, { silent: true });
    }, 60000);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu]);

  /* ===========================
     MENU OPEN (mobile)
  =========================== */
  const setMenuOpen = (open) => {
    if (open) {
      document.body.classList.add("menu-open");
      document.body.classList.remove("admin-menu-open");
      setMenuOpenState(true);
      setAdminMenuOpenState(false);
    } else {
      document.body.classList.remove("menu-open");
      setMenuOpenState(false);
    }
  };

  const setAdminMenuOpen = (open) => {
    if (open) {
      document.body.classList.add("admin-menu-open");
      document.body.classList.remove("menu-open");
      setAdminMenuOpenState(true);
      setMenuOpenState(false);
    } else {
      document.body.classList.remove("admin-menu-open");
      setAdminMenuOpenState(false);
    }
  };

  useEffect(() => {
    if (!loggedIn) {
      setMenuOpen(false);
      setAdminMenuOpen(false);
    }
  }, [loggedIn]);

  /* ===========================
     PLAYER SLUG
  =========================== */
  const playerSlug = slugify(selectedPlayer);

useEffect(() => {
  setSuggestPayload(null);
  setSuggestTeam("");
  setSuggestions([]);
  setSuggestError("");
  setSuggestHasRun(false);
}, [loggedIn]);

  /* ===========================
     RENDER
  =========================== */
  return (
    <div className="login-page">
      {!authBootstrapped || authRestoring ? (
        <div className="login-shell">
          <section className="login-card">
            <h3>Verifica sessione in corso...</h3>
          </section>
        </div>
      ) : !loggedIn ? (
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
              <div className="menu-group">
                <button
                  type="button"
                  className={
                    activeMenu === "home"
                      ? "menu-group-toggle menu-group-toggle-direct active"
                      : "menu-group-toggle menu-group-toggle-direct"
                  }
                  onClick={() => openMenuFeature("home")}
                  aria-current={activeMenu === "home" ? "page" : undefined}
                >
                  <span className="menu-group-title">Home</span>
                </button>
              </div>

              <div className="menu-group">
                <button
                  type="button"
                  className="menu-group-toggle"
                  onClick={() => toggleMenuSection("generali")}
                  aria-expanded={menuSectionsOpen.generali}
                >
                  <span className="menu-group-title">Generali</span>
                  <span
                    className={menuSectionsOpen.generali ? "menu-group-chevron open" : "menu-group-chevron"}
                    aria-hidden="true"
                  >
                    &gt;
                  </span>
                </button>
                {menuSectionsOpen.generali ? (
                  <div className="menu-group-items">
                    <button
                      className={`${menuItemClass("listone")} menu-subitem`}
                      onClick={() => openMenuFeature("listone")}
                    >
                      Listone
                    </button>
                    <button
                      className={`${menuItemClass("stats")} menu-subitem`}
                      onClick={() => openMenuFeature("stats")}
                    >
                      Statistiche giocatori
                    </button>
                    <button
                      className={`${menuItemClass("classifica-fixtures-seriea")} menu-subitem`}
                      onClick={() => openMenuFeature("classifica-fixtures-seriea")}
                    >
                      Serie A
                    </button>
                  </div>
                ) : null}
              </div>

              <div className="menu-group">
                <button
                  type="button"
                  className="menu-group-toggle"
                  onClick={() => toggleMenuSection("lega")}
                  aria-expanded={menuSectionsOpen.lega}
                >
                  <span className="menu-group-title">Lega</span>
                  <span
                    className={menuSectionsOpen.lega ? "menu-group-chevron open" : "menu-group-chevron"}
                    aria-hidden="true"
                  >
                    &gt;
                  </span>
                </button>
                {menuSectionsOpen.lega ? (
                  <div className="menu-group-items">
                    <button
                      className={`${menuItemClass("rose")} menu-subitem`}
                      onClick={() => openMenuFeature("rose")}
                    >
                      Rose
                    </button>
                    <button
                      className={`${menuItemClass("formazioni")} menu-subitem`}
                      onClick={() => openMenuFeature("formazioni")}
                    >
                      Formazioni
                    </button>
                    <button
                      className={`${menuItemClass("classifica-lega")} menu-subitem`}
                      onClick={() => openMenuFeature("classifica-lega")}
                    >
                      Classifica
                    </button>
                    <button
                      className={`${menuItemClass("top-acquisti")} menu-subitem`}
                      onClick={() => openMenuFeature("top-acquisti")}
                    >
                      Giocatori piu acquistati
                    </button>
                  </div>
                ) : null}
              </div>

              <div className="menu-group">
                <button
                  type="button"
                  className="menu-group-toggle"
                  onClick={() => toggleMenuSection("extra")}
                  aria-expanded={menuSectionsOpen.extra}
                >
                  <span className="menu-group-title">Extra</span>
                  <span
                    className={menuSectionsOpen.extra ? "menu-group-chevron open" : "menu-group-chevron"}
                    aria-hidden="true"
                  >
                    &gt;
                  </span>
                </button>
                {menuSectionsOpen.extra ? (
                  <div className="menu-group-items">
                    <button
                      className={`${menuItemClass("formazione-consigliata")} menu-subitem`}
                      onClick={() => openMenuFeature("formazione-consigliata")}
                    >
                      Formazioni consigliate
                    </button>
                    <button
                      className={`${menuItemClass("mercato")} menu-subitem`}
                      onClick={() => openMenuFeature("mercato")}
                    >
                      Mercato
                    </button>
                    <button
                      className={`${menuItemClass("plusvalenze")} menu-subitem`}
                      onClick={() => openMenuFeature("plusvalenze")}
                    >
                      Plusvalenze
                    </button>
                  </div>
                ) : null}
              </div>
            </nav>
          </aside>
          {isAdmin && (
            <aside className="admin-sidebar" aria-label="Menu admin">
              <div className="brand">
                <span className="eyebrow">Admin</span>
                <h2>Gestione</h2>
              </div>

              <nav className="menu">
                <button
                  className={menuItemClass("live", "formazioni_live")}
                  onClick={() => openMenuFeature("live", "formazioni_live", true, true)}
                >
                  Live
                </button>
                <button
                  className={menuItemClass("admin", "home")}
                  onClick={() => openMenuFeature("admin", "home", true, true)}
                >
                  Gestione
                </button>
              </nav>
            </aside>
          )}

          <header className="mobile-topbar">
            <button
              className={menuOpen ? "burger active" : "burger"}
              onClick={() => setMenuOpen(!menuOpen)}
              aria-label="Apri menu"
              aria-expanded={menuOpen}
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
                  : activeMenu === "classifica-lega"
                  ? "Classifica"
                  : activeMenu === "formazioni"
                  ? "Formazioni"
                  : activeMenu === "formazione-consigliata"
                  ? "Formazione consigliata"
                  : activeMenu === "live"
                  ? "Live"
                  : activeMenu === "plusvalenze"
                  ? "Plusvalenze"
                  : activeMenu === "listone"
                  ? "Listone"
                  : activeMenu === "top-acquisti"
                  ? "Top Acquisti"
                  : activeMenu === "mercato"
                  ? "Mercato"
                  : activeMenu === "classifica-fixtures-seriea"
                  ? "Serie A"
                  : activeMenu === "player"
                  ? "Scheda giocatore"
                  : "Gestione"}
              </strong>
            </div>

            <button className="ghost theme-toggle" onClick={toggleTheme}>
              {theme === "dark" ? "Dark" : "Light"}
            </button>
            {isAdmin && (
              <button
                className={
                  adminMenuOpen
                    ? "ghost admin-menu-toggle active"
                    : "ghost admin-menu-toggle"
                }
                onClick={() => setAdminMenuOpen(!adminMenuOpen)}
                aria-label="Apri menu admin"
                aria-expanded={adminMenuOpen}
              >
                Admin
              </button>
            )}
          </header>

          <div className="menu-overlay" onClick={() => setMenuOpen(false)} />
          <div className="admin-menu-overlay" onClick={() => setAdminMenuOpen(false)} />

          <main className="content">
            {error || initialDataError ? (
              <div className="panel app-alert">
                <div className="app-alert-messages">
                  {error ? <p className="error">{error}</p> : null}
                  {initialDataError ? <p className="error">{initialDataError}</p> : null}
                </div>
                <div className="app-alert-actions">
                  {initialDataError ? (
                    <button
                      type="button"
                      className="ghost"
                      onClick={retryInitialDataLoad}
                      disabled={initialDataLoading}
                    >
                      {initialDataLoading ? "Riprovo..." : "Riprova dati"}
                    </button>
                  ) : null}
                  {error ? (
                    <button type="button" className="ghost" onClick={() => setError("")}>
                      Chiudi
                    </button>
                  ) : null}
                </div>
              </div>
            ) : null}
            {/* ===========================
                HOME (placeholder minimale)
            =========================== */}
            {activeMenu === "home" && (
              <HomeSection
                summary={summary}
                dataStatus={dataStatus}
                formatDataStatusDate={formatDataStatusDate}
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
                setActiveMenu={(menuKey) => openMenuFeature(menuKey, null, false, false)}
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

            {activeMenu === "formazioni" && (
              <FormazioniSection
                formations={formations}
                formationTeam={formationTeam}
                setFormationTeam={setFormationTeam}
                formationRound={formationRound}
                onFormationRoundChange={onFormationRoundChange}
                formationOrder={formationOrder}
                onFormationOrderChange={onFormationOrderChange}
                formationMeta={formationMeta}
                reloadFormazioni={() => loadFormazioni(formationRound || null, formationOrder)}
                optimizerData={formationOptimizer}
                optimizerLoading={formationOptimizerLoading}
                optimizerError={formationOptimizerError}
                runOptimizer={runFormationOptimizer}
                openPlayer={openPlayer}
                formatDecimal={formatDecimal}
              />
            )}

            {activeMenu === "formazione-consigliata" && (
              <FormazioniSection
                formations={formations}
                formationTeam={formationTeam}
                setFormationTeam={setFormationTeam}
                formationRound={formationRound}
                onFormationRoundChange={onFormationRoundChange}
                formationOrder={formationOrder}
                onFormationOrderChange={onFormationOrderChange}
                formationMeta={formationMeta}
                reloadFormazioni={() => loadFormazioni(formationRound || null, formationOrder)}
                optimizerData={formationOptimizer}
                optimizerLoading={formationOptimizerLoading}
                optimizerError={formationOptimizerError}
                runOptimizer={runFormationOptimizer}
                openPlayer={openPlayer}
                formatDecimal={formatDecimal}
              />
            )}

            {activeMenu === "live" && isAdmin && (
              <LiveSection
                liveData={livePayload}
                liveLoading={liveLoading}
                liveError={liveError}
                liveSavingKey={liveSavingKey}
                liveImporting={liveImporting}
                liveFullSyncing={liveFullSyncing}
                onReload={() => loadLivePayload(livePayload?.round || null)}
                onImportVotes={() => importLiveVotes(livePayload?.round || null)}
                onFullSync={() => runFullSyncTotal(livePayload?.round || null)}
                onRoundChange={onLiveRoundChange}
                onToggleSixPolitico={saveLiveMatchSix}
                onSavePlayer={saveLivePlayerVote}
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
                topPosFrom={topPosFrom}
                setTopPosFrom={setTopPosFrom}
                topPosTo={topPosTo}
                setTopPosTo={setTopPosTo}
                topAcquistiRangeLabel={topAcquistiRangeLabel}
                onResetTopAcquistiFilters={resetTopAcquistiFilters}
                filteredTopAcquisti={filteredTopAcquisti}
                openPlayer={openPlayer}
                formatInt={formatInt}
              />
            )}

            {activeMenu === "mercato" && (
              <MercatoSection
                marketUpdatedAt={marketUpdatedAt}
                marketCountdown={marketCountdown}
                marketStandings={marketStandings}
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

            {activeMenu === "classifica-lega" && (
              <PremiumInsightsSection
                mode="classifica-lega"
                insights={premiumInsights}
                loading={false}
                error=""
                onReload={null}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "classifica-fixtures-seriea" && (
              <PremiumInsightsSection
                mode="classifica-fixtures-seriea"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
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
                GESTIONE
            =========================== */}
            {activeMenu === "admin" && isAdmin && (
              <section className="dashboard">
                <div className="dashboard-header">
                  <div>
                    <p className="eyebrow">Gestione</p>
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
                      <input
                        className="input"
                        placeholder="Nota reset (opzionale)"
                        value={adminResetNote}
                        onChange={(e) => setAdminResetNote(e.target.value)}
                      />
                      <button className="ghost" onClick={resetKeyAdmin}>
                        Reset key
                      </button>
                    </div>

                    <div className="admin-row admin-row-stacked">
                      <p className="muted">
                        Key selezionata ({(adminResetKey || "-").toUpperCase()}): reset usati{" "}
                        {adminResetUsage?.used ?? 0}/{adminResetUsage?.limit ?? 3}
                        {adminResetUsage?.season
                          ? ` · Stagione ${adminResetUsage.season}`
                          : ""}
                      </p>
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
                      adminKeys.map((item) => {
                        const rowKey = String(item?.key || "").trim().toLowerCase();
                        const noteDraft = String(adminKeyNotesDraft[rowKey] ?? item?.note ?? "");
                        const savingNote = adminSavingNoteKey === rowKey;
                        const deletingKey = adminDeletingKey === rowKey;
                        const blockingKey = adminBlockingKey === rowKey;
                        const isBlocked = Boolean(item?.blocked);
                        return (
                          <div key={item.key} className="list-item player-card">
                            <div>
                              <p>{String(item.key || "").toUpperCase()}</p>
                              <span className="muted">
                                {item.is_admin ? "ADMIN" : "USER"} - {item.used ? "Attivata" : "Non usata"}
                              </span>
                              <span className="muted">Team: {item.team || "-"}</span>
                              <span className="muted">
                                Reset: {item.reset_used ?? 0}/{item.reset_limit ?? 3}
                                {item.reset_season ? ` · Stagione ${item.reset_season}` : ""}
                              </span>
                              <span className="muted">
                                Ultimo accesso: {item.online ? "Online" : formatLastAccess(item.last_seen_at || item.used_at)}
                              </span>
                              <span className={isBlocked ? "muted key-blocked" : "muted"}>
                                Blocco:{" "}
                                {isBlocked
                                  ? item.blocked_until
                                    ? `attivo fino a ${formatLastAccess(item.blocked_until)}`
                                    : "attivo (sblocco manuale)"
                                  : "nessuno"}
                              </span>
                              {isBlocked && item.blocked_reason ? (
                                <span className="muted key-blocked-reason">Motivo: {item.blocked_reason}</span>
                              ) : null}
                              <span className="muted key-note-preview">
                                Nota: {String(item.note || "").trim() || "-"}
                              </span>
                              <div className="admin-row admin-row-key-note">
                                <input
                                  className="input"
                                  placeholder="Nota opzionale per questa key"
                                  value={noteDraft}
                                  maxLength={255}
                                  onChange={(e) => updateAdminKeyNoteDraft(rowKey, e.target.value)}
                                />
                                <button
                                  className={savingNote ? "ghost note-save-btn is-loading" : "ghost note-save-btn"}
                                  onClick={() => saveAdminKeyNote(rowKey)}
                                  disabled={savingNote || deletingKey || blockingKey}
                                >
                                  {savingNote ? "Salvataggio..." : "Salva nota"}
                                </button>
                              </div>
                            </div>
                            <div className="admin-key-actions">
                              <button
                                className={
                                  blockingKey
                                    ? "ghost key-block-btn is-loading"
                                    : isBlocked
                                    ? "ghost key-block-btn is-unblock"
                                    : "ghost key-block-btn"
                                }
                                onClick={() =>
                                  isBlocked ? unblockAdminKey(item.key) : blockAdminKey(item.key)
                                }
                                disabled={blockingKey || deletingKey || savingNote}
                              >
                                {blockingKey
                                  ? "Aggiorno..."
                                  : isBlocked
                                  ? "Sblocca"
                                  : "Blocca"}
                              </button>
                              <button
                                className={deletingKey ? "ghost key-delete-btn is-loading" : "ghost key-delete-btn"}
                                onClick={() => deleteAdminKey(item.key)}
                                disabled={deletingKey || savingNote || blockingKey}
                              >
                                {deletingKey ? "Eliminazione..." : "Elimina"}
                              </button>
                            </div>
                          </div>
                        );
                      })
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


