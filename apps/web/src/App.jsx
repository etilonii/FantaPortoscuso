import { useEffect, useMemo, useState } from "react";
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
const API_BASE =
  import.meta.env.VITE_API_BASE ||
  (import.meta.env.DEV
    ? "http://localhost:8001"
    : "https://fantaportoscuso.up.railway.app");
const TRIAL_TOP_ACQUISTI_LIMIT = 20;
const PLAN_TIER_LABELS = {
  trial: "Trial",
  base: "Base",
  premium: "Premium",
};
const BILLING_LABELS = {
  trial: "Trial",
  monthly: "Mensile",
  season9: "9 mesi",
};
const PLAN_PRICES_FALLBACK = {
  trial: { trial: 0 },
  base: { monthly: 5, season9: 37.99 },
  premium: { monthly: 10, season9: 57.99 },
};
const PLAN_COMPARISON = {
  trial: {
    title: "Trial 3 giorni",
    subtitle: "Accesso guidato con funzioni ridotte",
    features: [
      "Home e classifica lega",
      "Rose, listone, statistiche giocatori",
      "Top acquisti fino alla top 20 per ruolo",
      "Dark/Light mode",
      "Funzioni premium visibili ma bloccate",
    ],
  },
  base: {
    title: "Base",
    subtitle: "Strumenti essenziali per giocare ogni giornata",
    features: [
      "Home, quotazioni, rose, listone",
      "Plusvalenze, top acquisti e mercato (non live)",
      "Statistiche giocatori e formazioni (non live)",
      "Schede giocatori e dark/light mode",
      "Classifica lega",
    ],
  },
  premium: {
    title: "Premium",
    subtitle: "Analisi avanzata e funzioni live complete",
    features: [
      "Tutto il piano Base",
      "Formazioni live e mercato live",
      "Formazione consigliata",
      "Tier list e potenza squadre (XI/totale)",
      "Classifiche avanzate e predictions campionato+fixtures",
    ],
  },
};
const MENU_FEATURES = {
  home: "home",
  abbonamenti: "home",
  quotazioni: "quotazioni",
  stats: "statistiche_giocatori",
  rose: "rose",
  formazioni: "formazioni",
  "formazione-consigliata": "formazione_consigliata",
  plusvalenze: "plusvalenze",
  listone: "listone",
  "top-acquisti": "top_acquisti",
  mercato: "mercato",
  "classifica-lega": "classifica_lega",
  "mercato-live": "mercato_live",
  "tier-list": "tier_list",
  "potenza-titolari": "potenza_squadra_titolari",
  "potenza-totale": "potenza_squadra_totale",
  "classifica-potenza": "classifica_potenza",
  "classifica-reale-lega": "classifica_reale_lega",
  "classifica-fixtures-seriea": "classifica_fixtures_seriea",
  predictions: "predictions_campionato_fixtures",
  player: "schede_giocatori",
  live: "formazioni_live",
  admin: "home",
};

const PREMIUM_INSIGHTS_MENU_KEYS = new Set([
  "tier-list",
  "potenza-titolari",
  "potenza-totale",
  "classifica-potenza",
  "classifica-reale-lega",
  "classifica-fixtures-seriea",
  "predictions",
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
  const [subscription, setSubscription] = useState(null);
  const [status, setStatus] = useState("idle");
  const [error, setError] = useState("");

  /* ===== UI ===== */
  const [theme, setTheme] = useState("dark");
  const [activeMenu, setActiveMenu] = useState("home");
  const [menuOpen, setMenuOpenState] = useState(false);
  const [adminMenuOpen, setAdminMenuOpenState] = useState(false);

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
    seriea_predictions: [],
    seriea_final_table: [],
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
  const [adminSubKey, setAdminSubKey] = useState("");
  const [adminSubPlan, setAdminSubPlan] = useState("base");
  const [adminSubCycle, setAdminSubCycle] = useState("monthly");
  const [adminSubImmediate, setAdminSubImmediate] = useState(false);
  const [adminBlockKey, setAdminBlockKey] = useState("");
  const [adminBlockValue, setAdminBlockValue] = useState(true);
  const [adminBlockReason, setAdminBlockReason] = useState("");
  const [billingPlan, setBillingPlan] = useState("premium");
  const [billingCycle, setBillingCycle] = useState("season9");
  const [billingLoading, setBillingLoading] = useState(false);
  const [billingNotice, setBillingNotice] = useState("");
  const [billingSessionVerifying, setBillingSessionVerifying] = useState(false);

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
    } catch {}
  };

  const clearAuthSession = (message = "") => {
    setLoggedIn(false);
    setIsAdmin(false);
    setSubscription(null);
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

  const applyAuthSessionPayload = (data) => {
    setIsAdmin(Boolean(data?.is_admin));
    setSubscription(normalizeSubscription(data?.subscription));
  };

  const loadAuthSession = async () => {
    const keyValue = accessKey.trim().toLowerCase();
    const headers = buildAuthHeaders({ legacyAccessKey: true });
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/session`, { headers }, true);
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        if (res.status === 402) {
          setLoggedIn(true);
          applyAuthSessionPayload(payload || {});
          const message =
            payload?.detail?.message ||
            payload?.message ||
            "Key bloccata: rinnova per continuare.";
          setError(String(message));
          return false;
        }
        clearAuthSession(payload?.detail || payload?.message || "Sessione non valida.");
        return false;
      }
      setLoggedIn(true);
      applyAuthSessionPayload(payload || {});
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

  const normalizeSubscription = (raw) => {
    const src = raw && typeof raw === "object" ? raw : {};
    const featuresRaw = src.features && typeof src.features === "object" ? src.features : {};
    const features = {};
    Object.keys(featuresRaw).forEach((key) => {
      features[String(key)] = Boolean(featuresRaw[key]);
    });
    const catalogRaw =
      src.price_catalog_eur && typeof src.price_catalog_eur === "object"
        ? src.price_catalog_eur
        : {};
    const priceCatalog = {};
    Object.keys(catalogRaw).forEach((planKey) => {
      const cycleMap = catalogRaw[planKey];
      if (!cycleMap || typeof cycleMap !== "object") return;
      const cleanCycleMap = {};
      Object.keys(cycleMap).forEach((cycleKey) => {
        const amount = Number(cycleMap[cycleKey]);
        if (Number.isFinite(amount)) cleanCycleMap[String(cycleKey)] = amount;
      });
      priceCatalog[String(planKey)] = cleanCycleMap;
    });
    return {
      plan_tier: String(src.plan_tier || "").trim().toLowerCase() || "trial",
      billing_cycle: String(src.billing_cycle || "").trim().toLowerCase() || "trial",
      current_price_eur: Number.isFinite(Number(src.current_price_eur))
        ? Number(src.current_price_eur)
        : null,
      pending_price_eur: Number.isFinite(Number(src.pending_price_eur))
        ? Number(src.pending_price_eur)
        : null,
      price_catalog_eur: priceCatalog,
      status: String(src.status || "").trim().toLowerCase() || "active",
      blocked_reason: String(src.blocked_reason || "").trim().toLowerCase() || "",
      plan_expires_at: src.plan_expires_at ? String(src.plan_expires_at) : "",
      seconds_to_expiry: Number.isFinite(Number(src.seconds_to_expiry))
        ? Number(src.seconds_to_expiry)
        : null,
      pending_plan_tier: src.pending_plan_tier ? String(src.pending_plan_tier) : "",
      pending_billing_cycle: src.pending_billing_cycle
        ? String(src.pending_billing_cycle)
        : "",
      pending_effective_at: src.pending_effective_at ? String(src.pending_effective_at) : "",
      seconds_to_pending: Number.isFinite(Number(src.seconds_to_pending))
        ? Number(src.seconds_to_pending)
        : null,
      features,
    };
  };

  const hasFeature = (featureName) => {
    if (isAdmin) return true;
    if (!subscription || !subscription.features) return false;
    return Boolean(subscription.features[String(featureName || "")]);
  };

  const isSubscriptionBlocked = Boolean(
    !isAdmin && subscription && String(subscription.status || "") === "blocked"
  );

  const resolveMenuFeature = (menuKey, featureName) => {
    const explicit = String(featureName || "").trim();
    if (explicit) return explicit;
    const mapped = MENU_FEATURES[String(menuKey || "").trim()];
    return mapped || "home";
  };

  const planTierLabel = (planTier) =>
    PLAN_TIER_LABELS[String(planTier || "").trim().toLowerCase()] ||
    String(planTier || "-").toUpperCase();

  const billingCycleLabel = (billingCycle) =>
    BILLING_LABELS[String(billingCycle || "").trim().toLowerCase()] ||
    String(billingCycle || "-");

  const blockedReasonLabel = (blockedReason) => {
    const key = String(blockedReason || "").trim().toLowerCase();
    if (key === "trial_expired") return "Trial terminato";
    if (key === "plan_expired") return "Piano scaduto";
    if (key === "manual_suspension") return "Sospensione manuale";
    return key || "bloccata";
  };

  const cleanBillingQueryParams = () => {
    try {
      const nextUrl = new URL(window.location.href);
      nextUrl.searchParams.delete("billing");
      nextUrl.searchParams.delete("session_id");
      window.history.replaceState({}, "", nextUrl.toString());
    } catch {}
  };

  const billingAuthHeaders = () => {
    const headers = { "Content-Type": "application/json" };
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const keyValue = String(accessKey || "").trim().toLowerCase();
    if (keyValue) {
      headers["X-Access-Key"] = keyValue;
    }
    return headers;
  };

  const verifyBillingCheckout = async (sessionId) => {
    const cleanSessionId = String(sessionId || "").trim();
    if (!cleanSessionId) return;
    setBillingSessionVerifying(true);
    try {
      const res = await fetch(
        `${API_BASE}/auth/billing/verify?session_id=${encodeURIComponent(cleanSessionId)}`,
        {
          headers: billingAuthHeaders(),
        }
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const message = data?.detail || data?.message || "Pagamento registrato, aggiornamento in corso.";
        setBillingNotice(String(message));
        return;
      }
      const nextSub = normalizeSubscription(data?.subscription || {});
      if (nextSub && typeof nextSub === "object") {
        setSubscription(nextSub);
      }
      setBillingNotice("Pagamento confermato. Piano aggiornato con successo.");
      setError("");
      try {
        await loadAuthSession();
      } catch {}
    } catch {
      setBillingNotice("Pagamento completato. Ricarica lo stato key tra pochi secondi.");
    } finally {
      setBillingSessionVerifying(false);
      cleanBillingQueryParams();
    }
  };

  const startBillingCheckout = async (planTier = billingPlan, cycle = billingCycle) => {
    const targetPlan = String(planTier || "").trim().toLowerCase();
    const targetCycle = String(cycle || "").trim().toLowerCase();
    if (!["base", "premium"].includes(targetPlan)) {
      setBillingNotice("Seleziona un piano valido.");
      return;
    }
    if (!["monthly", "season9"].includes(targetCycle)) {
      setBillingNotice("Seleziona un ciclo valido.");
      return;
    }
    setBillingLoading(true);
    setBillingNotice("");
    try {
      const res = await fetch(`${API_BASE}/auth/billing/checkout`, {
        method: "POST",
        headers: billingAuthHeaders(),
        body: JSON.stringify({
          plan_tier: targetPlan,
          billing_cycle: targetCycle,
          success_path: "/?billing=success",
          cancel_path: "/?billing=cancel",
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const message = data?.detail || data?.message || "Errore avvio checkout.";
        setBillingNotice(String(message));
        return;
      }
      const checkoutUrl = String(data?.checkout_url || "").trim();
      if (!checkoutUrl) {
        setBillingNotice("Checkout non disponibile.");
        return;
      }
      window.location.assign(checkoutUrl);
    } catch {
      setBillingNotice("Errore connessione checkout.");
    } finally {
      setBillingLoading(false);
    }
  };

  const subscriptionPriceCatalog =
    subscription?.price_catalog_eur &&
    typeof subscription.price_catalog_eur === "object" &&
    Object.keys(subscription.price_catalog_eur).length > 0
      ? subscription.price_catalog_eur
      : PLAN_PRICES_FALLBACK;

  const subscriptionPriceFor = (planTier, billingCycle) => {
    const planKey = String(planTier || "").trim().toLowerCase();
    const cycleKey = String(billingCycle || "").trim().toLowerCase();
    const planMap = subscriptionPriceCatalog[planKey];
    if (!planMap || typeof planMap !== "object") return null;
    const amount = Number(planMap[cycleKey]);
    return Number.isFinite(amount) ? amount : null;
  };

  const currentPlanTier = String(subscription?.plan_tier || "").trim().toLowerCase();
  const currentBillingCycle = String(subscription?.billing_cycle || "").trim().toLowerCase();

  const menuItemClass = (menuKey, featureName) => {
    const active = activeMenu === menuKey;
    const locked = !hasFeature(resolveMenuFeature(menuKey, featureName));
    const classes = ["menu-item"];
    if (active) classes.push("active");
    if (locked) classes.push("locked");
    return classes.join(" ");
  };

  const openMenuFeature = (menuKey, featureName, closeMobile = true, closeAdmin = false) => {
    const requiredFeature = resolveMenuFeature(menuKey, featureName);
    if (!hasFeature(requiredFeature)) {
      const planTier = String(subscription?.plan_tier || "trial").toUpperCase();
      setError(`Funzione non disponibile nel piano ${planTier}.`);
      return;
    }
    setError("");
    setActiveMenu(menuKey);
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
        if (res.status === 402 && detail && typeof detail === "object") {
          setLoggedIn(true);
          setIsAdmin(Boolean(d?.is_admin));
          setSubscription(normalizeSubscription(detail?.subscription || d?.subscription));
          throw new Error(detail?.message || "Key bloccata: rinnova per continuare.");
        }
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
      const res = await fetch(`${API_BASE}/data/summary`);
      if (!res.ok) return;
      const data = await res.json();
      setSummary(data);
    } catch {}
  };

  const loadDataStatus = async () => {
    try {
      const res = await fetch(`${API_BASE}/meta/data-status`);
      if (!res.ok) throw new Error("status request failed");
      const data = await res.json();
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
          ["rose", "stats", "strength"].forEach((key) => {
            const value = String(rawSteps[key] || "").trim().toLowerCase();
            if (allowed.has(value)) {
              normalizedSteps[key] = value;
            }
          });
          return normalizedSteps;
        })(),
      };
      setDataStatus(normalized);
    } catch {
      setDataStatus((prev) => ({
        ...prev,
        result: "error",
        message: "Errore nel recupero stato dati",
        update_id: "",
        steps: {},
      }));
    }
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

  const loadMarketStandings = async () => {
    try {
      const res = await fetch(`${API_BASE}/data/standings`);
      if (!res.ok) return;
      const data = await res.json();
      setMarketStandings(Array.isArray(data.items) ? data.items : []);
    } catch {}
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
      const res = await fetchWithAuth(`${API_BASE}/data/formazioni?${params.toString()}`, {
        headers: buildAuthHeaders({ legacyAccessKey: true }),
      });
      if (!res.ok) return;
      const data = await res.json();
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
    } catch {}
  };

  const onFormationRoundChange = (nextRound) => {
    setFormationRound(nextRound);
    loadFormazioni(nextRound, formationOrder);
  };

  const onFormationOrderChange = (nextOrder) => {
    const normalizedOrder =
      String(nextOrder || "").toLowerCase() === "live_total" ? "live_total" : "classifica";
    if (normalizedOrder === "live_total" && !hasFeature("formazioni_live")) {
      setError("Classifica live giornata disponibile solo con piano Premium.");
      return;
    }
    setFormationOrder(normalizedOrder);
    loadFormazioni(formationRound || null, normalizedOrder);
  };

  const runFormationOptimizer = async (teamName, roundValue = null) => {
    if (!hasFeature("formazione_consigliata")) {
      setFormationOptimizer(null);
      setFormationOptimizerError("XI ottimizzata disponibile solo con piano Premium.");
      return;
    }
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
    if (!loggedIn || (!isAdmin && !hasFeature("formazioni_live"))) return;
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
    } catch (err) {
      setLiveError(err?.message || "Errore import voti live");
    } finally {
      setLiveImporting(false);
    }
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
    if (!hasFeature("schede_giocatori")) {
      setError("Scheda giocatore disponibile dal piano Base.");
      return;
    }
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

    const isTrial = String(subscription?.plan_tier || "").toLowerCase() === "trial";
    if (!isAdmin && isTrial) {
      return list.slice(0, TRIAL_TOP_ACQUISTI_LIMIT);
    }
    return list;
  }, [
    topPlayersByRole,
    activeTopRole,
    topAcquistiQuery,
    marketStandings,
    topPosFrom,
    topPosTo,
    subscription,
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
      setAdminKeys(data || []);
    } catch {}
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
    } catch {}
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
    } catch {}
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
    } catch {}
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
    } catch {}
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
    } catch {}
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
    } catch {}
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
    } catch {}
  };

  const deleteTeamKeyAdmin = async (keyValue) => {
    if (!isAdmin) return;
    const key = String(keyValue || "").trim().toLowerCase();
    if (!key) return;
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/team-key`, {
        method: "DELETE",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({ key }),
      });
      if (!res.ok) return;
      setAdminNotice(`Associazione rimossa: ${key.toUpperCase()}.`);
      loadAdminTeamKeys();
    } catch {}
  };

  const loadPremiumInsights = async (force = false) => {
    if (!loggedIn) return false;
    if (
      !force &&
      Array.isArray(premiumInsights?.player_tiers) &&
      premiumInsights.player_tiers.length > 0
    ) {
      return true;
    }
    try {
      setPremiumInsightsLoading(true);
      setPremiumInsightsError("");
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
        seriea_predictions: Array.isArray(payload?.seriea_predictions)
          ? payload.seriea_predictions
          : [],
        seriea_final_table: Array.isArray(payload?.seriea_final_table)
          ? payload.seriea_final_table
          : [],
        generated_at: String(payload?.generated_at || ""),
      });
      return true;
    } catch (err) {
      setPremiumInsightsError(err?.message || "Errore caricamento insight premium");
      return false;
    } finally {
      setPremiumInsightsLoading(false);
    }
  };

  const setSubscriptionAdmin = async () => {
    if (!isAdmin) return;
    const key = String(adminSubKey || "").trim().toLowerCase();
    if (!key) {
      setAdminNotice("Inserisci una key per impostare il piano.");
      return;
    }
    try {
      const payload = {
        key,
        plan_tier: String(adminSubPlan || "base").trim().toLowerCase(),
        billing_cycle: String(adminSubCycle || "monthly").trim().toLowerCase(),
        force_immediate: Boolean(adminSubImmediate),
      };
      if (payload.plan_tier === "trial") {
        payload.billing_cycle = "trial";
      }
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/subscription`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || data?.message || "Errore impostazione piano.");
        return;
      }
      const schedule = data?.schedule || {};
      const delayHours = Number(schedule?.delay_hours);
      const immediate = !schedule?.scheduled || delayHours <= 0;
      const summary = immediate
        ? `Piano ${String(payload.plan_tier).toUpperCase()} applicato subito.`
        : `Piano ${String(payload.plan_tier).toUpperCase()} pianificato tra ${delayHours}h.`;
      setAdminNotice(`${key.toUpperCase()}: ${summary}`);
      loadAdminKeys();
      if (key === String(accessKey || "").trim().toLowerCase()) {
        loadAuthSession();
      }
    } catch {
      setAdminNotice("Errore impostazione piano.");
    }
  };

  const setSubscriptionBlockAdmin = async () => {
    if (!isAdmin) return;
    const key = String(adminBlockKey || "").trim().toLowerCase();
    if (!key) {
      setAdminNotice("Inserisci una key per sospendere/ripristinare.");
      return;
    }
    try {
      const res = await fetchWithAuth(`${API_BASE}/auth/admin/subscription/block`, {
        method: "POST",
        headers: buildAuthHeaders({
          legacyAdminKey: true,
          extraHeaders: { "Content-Type": "application/json" },
        }),
        body: JSON.stringify({
          key,
          blocked: Boolean(adminBlockValue),
          reason: String(adminBlockReason || "").trim() || null,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setAdminNotice(data?.detail || data?.message || "Errore aggiornamento blocco key.");
        return;
      }
      setAdminNotice(
        `${key.toUpperCase()}: ${adminBlockValue ? "sospesa" : "riattivata"} con successo.`
      );
      loadAdminKeys();
      if (key === String(accessKey || "").trim().toLowerCase()) {
        loadAuthSession();
      }
    } catch {
      setAdminNotice("Errore aggiornamento blocco key.");
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
      const savedAccessToken = localStorage.getItem(ACCESS_TOKEN_STORAGE) || "";
      const savedRefreshToken = localStorage.getItem(REFRESH_TOKEN_STORAGE) || "";
      if (savedAccessToken || savedRefreshToken) {
        setAccessToken(savedAccessToken);
        setRefreshToken(savedRefreshToken);
      }
    } catch {}
  }, []);

  useEffect(() => {
    if (loggedIn) return;
    if (!accessKey.trim()) return;
    if (!accessToken && !refreshToken) return;
    loadAuthSession();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, accessKey, accessToken, refreshToken]);

  useEffect(() => {
    if (!loggedIn) return;
    let params;
    try {
      params = new URLSearchParams(window.location.search);
    } catch {
      return;
    }
    const billingState = String(params.get("billing") || "").trim().toLowerCase();
    if (!billingState) return;
    if (billingState === "cancel") {
      setBillingNotice("Pagamento annullato.");
      cleanBillingQueryParams();
      return;
    }
    if (billingState === "success") {
      const sessionId = String(params.get("session_id") || "").trim();
      if (!sessionId) {
        setBillingNotice("Pagamento completato. Aggiorna lo stato key.");
        cleanBillingQueryParams();
        return;
      }
      verifyBillingCheckout(sessionId);
    }
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
    loadSummary();
    loadDataStatus();
    loadTeams();
    loadMarketStandings();
    loadFormazioni();
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
    if (loggedIn && (activeMenu === "listone" || activeMenu === "quotazioni")) loadListone();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu, quoteRole, quoteOrder]);

  useEffect(() => {
    if (!loggedIn || activeMenu !== "live") return;
    if (!isAdmin && !hasFeature("formazioni_live")) return;
    loadLivePayload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, isAdmin, activeMenu, subscription]);

  useEffect(() => {
    if (!loggedIn) return;
    if (!PREMIUM_INSIGHTS_MENU_KEYS.has(String(activeMenu || "").trim())) return;
    loadPremiumInsights(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loggedIn, activeMenu, subscription, isAdmin]);

  useEffect(() => {
    if (!loggedIn) return;
    if (activeMenu !== "quotazioni") return;
    if (activeTab === "quotazioni") return;
    setActiveTab("quotazioni");
  }, [loggedIn, activeMenu, activeTab]);

  useEffect(() => {
    if (!loggedIn || isAdmin) return;
    const feature = MENU_FEATURES[String(activeMenu || "").trim()] || "home";
    if (!hasFeature(feature)) {
      setActiveMenu("home");
    }
  }, [loggedIn, isAdmin, activeMenu, subscription]);

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
      ) : isSubscriptionBlocked ? (
        <div className="blocked-shell">
          <section className="blocked-card">
            <p className="eyebrow">Accesso Bloccato</p>
            <h2>Rinnova la key per continuare ad utilizzare il sito</h2>
            <p className="muted">
              {error ||
                `Stato: ${blockedReasonLabel(subscription?.blocked_reason)}. Contatta l'admin o rinnova il piano.`}
            </p>
            <div className="blocked-meta">
              <span>Piano attuale: {planTierLabel(subscription?.plan_tier)}</span>
              <span>Ciclo: {billingCycleLabel(subscription?.billing_cycle)}</span>
              {subscription?.plan_expires_at ? (
                <span>Scadenza: {formatDataStatusDate(subscription.plan_expires_at)}</span>
              ) : null}
              {subscription?.pending_plan_tier ? (
                <span>
                  Prossimo piano: {planTierLabel(subscription.pending_plan_tier)} tra{" "}
                  {formatCountdown(Number(subscription.seconds_to_pending || 0))}
                </span>
              ) : null}
            </div>
            <div className="blocked-actions blocked-actions-grid">
              <select
                className="select"
                value={billingPlan}
                onChange={(e) => {
                  const nextPlan = e.target.value;
                  setBillingPlan(nextPlan);
                  if (nextPlan === "trial") setBillingCycle("monthly");
                }}
              >
                <option value="base">Base</option>
                <option value="premium">Premium</option>
              </select>
              <select
                className="select"
                value={billingCycle}
                onChange={(e) => setBillingCycle(e.target.value)}
              >
                <option value="monthly">Mensile</option>
                <option value="season9">9 mesi</option>
              </select>
              <button
                className="primary"
                onClick={() => startBillingCheckout(billingPlan, billingCycle)}
                disabled={billingLoading || billingSessionVerifying}
              >
                {billingLoading
                  ? "Reindirizzamento..."
                  : `Acquista ${planTierLabel(billingPlan)} ${billingCycleLabel(billingCycle)}`}
              </button>
              <button className="ghost" onClick={() => loadAuthSession()}>
                Ricarica stato key
              </button>
            </div>
            {billingNotice ? <p className="muted">{billingNotice}</p> : null}
          </section>
        </div>
      ) : (
        <div className="app-shell">
          <aside className="sidebar" aria-label="Menu principale">
            <div className="brand">
              <span className="eyebrow">FantaPortoscuso</span>
              <h2>MenÃ¹</h2>
            </div>

            <nav className="menu">
              <button
                className={menuItemClass("home")}
                onClick={() => openMenuFeature("home")}
              >
                Home
              </button>
              <button
                className={menuItemClass("abbonamenti")}
                onClick={() => openMenuFeature("abbonamenti")}
              >
                Abbonamenti
              </button>
              <button
                className={menuItemClass("quotazioni")}
                onClick={() => openMenuFeature("quotazioni")}
              >
                Quotazioni
              </button>

              <button
                className={menuItemClass("stats")}
                onClick={() => openMenuFeature("stats")}
              >
                Statistiche Giocatori
              </button>

              <button
                className={menuItemClass("rose")}
                onClick={() => openMenuFeature("rose")}
              >
                Rose
              </button>
              <button
                className={menuItemClass("classifica-lega")}
                onClick={() => openMenuFeature("classifica-lega")}
              >
                Classifica Lega
              </button>

              <button
                className={menuItemClass("formazioni")}
                onClick={() => openMenuFeature("formazioni")}
              >
                Formazioni
              </button>
              <button
                className={menuItemClass("formazione-consigliata")}
                onClick={() => openMenuFeature("formazione-consigliata")}
              >
                Formazione consigliata
              </button>

              <button
                className={menuItemClass("plusvalenze")}
                onClick={() => openMenuFeature("plusvalenze")}
              >
                Plusvalenze
              </button>

              <button
                className={menuItemClass("listone")}
                onClick={() => openMenuFeature("listone")}
              >
                Listone
              </button>

              <button
                className={menuItemClass("top-acquisti")}
                onClick={() => openMenuFeature("top-acquisti")}
              >
                Giocatori piÃ¹ acquistati
              </button>
              <button
                className={menuItemClass("mercato")}
                onClick={() => openMenuFeature("mercato")}
              >
                Mercato
              </button>
              <button
                className={menuItemClass("mercato-live")}
                onClick={() => openMenuFeature("mercato-live")}
              >
                Mercato Live
              </button>
              <button
                className={menuItemClass("tier-list")}
                onClick={() => openMenuFeature("tier-list")}
              >
                Tier List
              </button>
              <button
                className={menuItemClass("potenza-titolari")}
                onClick={() => openMenuFeature("potenza-titolari")}
              >
                Potenza XI
              </button>
              <button
                className={menuItemClass("potenza-totale")}
                onClick={() => openMenuFeature("potenza-totale")}
              >
                Potenza Totale
              </button>
              <button
                className={menuItemClass("classifica-potenza")}
                onClick={() => openMenuFeature("classifica-potenza")}
              >
                Classifica Potenza
              </button>
              <button
                className={menuItemClass("classifica-reale-lega")}
                onClick={() => openMenuFeature("classifica-reale-lega")}
              >
                Classifica Reale Lega
              </button>
              <button
                className={menuItemClass("classifica-fixtures-seriea")}
                onClick={() => openMenuFeature("classifica-fixtures-seriea")}
              >
                Classifica + Fixtures Serie A
              </button>
              <button
                className={menuItemClass("predictions")}
                onClick={() => openMenuFeature("predictions")}
              >
                Predictions
              </button>
            </nav>
            {!isAdmin && subscription ? (
              <div className="subscription-card">
                <p className="eyebrow">Piano</p>
                <h3>{planTierLabel(subscription.plan_tier)}</h3>
                <p className="muted">Ciclo: {billingCycleLabel(subscription.billing_cycle)}</p>
                {Number.isFinite(Number(subscription.seconds_to_expiry)) ? (
                  <p className="muted">
                    Scadenza tra {formatCountdown(Number(subscription.seconds_to_expiry))}
                  </p>
                ) : null}
                {subscription.pending_plan_tier ? (
                  <p className="muted">
                    Cambio a {planTierLabel(subscription.pending_plan_tier)} tra{" "}
                    {formatCountdown(Number(subscription.seconds_to_pending || 0))}
                  </p>
                ) : null}
                <button className="ghost" onClick={() => openMenuFeature("abbonamenti")}>
                  Apri abbonamenti
                </button>
              </div>
            ) : null}
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
                  : activeMenu === "abbonamenti"
                  ? "Abbonamenti"
                  : activeMenu === "quotazioni"
                  ? "Quotazioni"
                  : activeMenu === "stats"
                  ? "Statistiche"
                  : activeMenu === "rose"
                  ? "Rose"
                  : activeMenu === "classifica-lega"
                  ? "Classifica Lega"
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
                  : activeMenu === "mercato-live"
                  ? "Mercato Live"
                  : activeMenu === "tier-list"
                  ? "Tier List"
                  : activeMenu === "potenza-titolari"
                  ? "Potenza XI"
                  : activeMenu === "potenza-totale"
                  ? "Potenza Totale"
                  : activeMenu === "classifica-potenza"
                  ? "Classifica Potenza"
                  : activeMenu === "classifica-reale-lega"
                  ? "Classifica Reale Lega"
                  : activeMenu === "classifica-fixtures-seriea"
                  ? "Classifica + Fixtures Serie A"
                  : activeMenu === "predictions"
                  ? "Predictions"
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
            {error ? (
              <div className="panel app-alert">
                <p className="error">{error}</p>
                <button type="button" className="ghost" onClick={() => setError("")}>
                  Chiudi
                </button>
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

            {activeMenu === "abbonamenti" && (
              <section className="dashboard billing-hub">
                <div className="dashboard-header left">
                  <div>
                    <p className="eyebrow">Abbonamenti</p>
                    <h2>Piani e funzionalita</h2>
                  </div>
                  <p className="muted">
                    Scegli il piano piu adatto: qui trovi confronto feature e checkout diretto.
                  </p>
                </div>

                {subscription ? (
                  <div className="panel billing-current-panel">
                    <div className="billing-current-grid">
                      <div>
                        <p className="eyebrow">Stato corrente</p>
                        <h3>
                          {planTierLabel(subscription.plan_tier)} ·{" "}
                          {billingCycleLabel(subscription.billing_cycle)}
                        </h3>
                        <p className="muted">
                          {subscription.status === "blocked"
                            ? `Bloccata: ${blockedReasonLabel(subscription.blocked_reason)}`
                            : "Attiva"}
                        </p>
                      </div>
                      <div className="billing-current-meta">
                        {Number.isFinite(Number(subscription.seconds_to_expiry)) ? (
                          <span>
                            Scadenza tra {formatCountdown(Number(subscription.seconds_to_expiry))}
                          </span>
                        ) : (
                          <span>Scadenza: non prevista</span>
                        )}
                        {subscription.pending_plan_tier ? (
                          <span>
                            Cambio pianificato: {planTierLabel(subscription.pending_plan_tier)} tra{" "}
                            {formatCountdown(Number(subscription.seconds_to_pending || 0))}
                          </span>
                        ) : (
                          <span>Nessun cambio pianificato</span>
                        )}
                      </div>
                    </div>
                  </div>
                ) : null}

                <div className="billing-plan-grid">
                  {["trial", "base", "premium"].map((plan) => {
                    const details = PLAN_COMPARISON[plan];
                    const isCurrent = currentPlanTier === plan;
                    const monthlyPrice = subscriptionPriceFor(plan, "monthly");
                    const seasonPrice = subscriptionPriceFor(plan, "season9");
                    return (
                      <article
                        key={plan}
                        className={`panel billing-plan-card ${isCurrent ? "active" : ""}`}
                      >
                        <p className="eyebrow">{details?.title || planTierLabel(plan)}</p>
                        <h3>{details?.subtitle || ""}</h3>
                        <ul className="billing-feature-list">
                          {(details?.features || []).map((feature) => (
                            <li key={`${plan}-${feature}`}>{feature}</li>
                          ))}
                        </ul>
                        {plan !== "trial" ? (
                          <div className="billing-plan-actions">
                            <button
                              className="ghost"
                              onClick={() => startBillingCheckout(plan, "monthly")}
                              disabled={
                                billingLoading ||
                                billingSessionVerifying ||
                                (isCurrent && currentBillingCycle === "monthly") ||
                                isAdmin
                              }
                            >
                              Mensile {formatEuro(monthlyPrice)}
                              {isCurrent && currentBillingCycle === "monthly" ? " (attivo)" : ""}
                            </button>
                            <button
                              className="ghost"
                              onClick={() => startBillingCheckout(plan, "season9")}
                              disabled={
                                billingLoading ||
                                billingSessionVerifying ||
                                (isCurrent && currentBillingCycle === "season9") ||
                                isAdmin
                              }
                            >
                              9 mesi {formatEuro(seasonPrice)}
                              {isCurrent && currentBillingCycle === "season9" ? " (attivo)" : ""}
                            </button>
                          </div>
                        ) : (
                          <p className="muted billing-trial-note">
                            Trial automatico di 3 giorni alla prima attivazione key.
                          </p>
                        )}
                      </article>
                    );
                  })}
                </div>

                <div className="panel billing-faq-panel">
                  <div className="dashboard-header left">
                    <div>
                      <p className="eyebrow">Note</p>
                      <h3>Come funziona</h3>
                    </div>
                  </div>
                  <div className="list">
                    <div className="list-item">
                      <div>
                        <p>Upgrade e rinnovi</p>
                        <span className="muted">
                          Pagamento via Stripe, rinnovo del piano in automatico dopo conferma checkout.
                        </span>
                      </div>
                    </div>
                    <div className="list-item">
                      <div>
                        <p>Blocco per scadenza</p>
                        <span className="muted">
                          Se il piano scade, la key viene bloccata fino al rinnovo.
                        </span>
                      </div>
                    </div>
                    <div className="list-item">
                      <div>
                        <p>Utente admin</p>
                        <span className="muted">
                          L&apos;admin gestisce piani e sospensioni dalla sezione Gestione.
                        </span>
                      </div>
                    </div>
                  </div>
                  {billingNotice ? <p className="muted">{billingNotice}</p> : null}
                </div>
              </section>
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

            {activeMenu === "quotazioni" && (
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
                onReload={() => loadLivePayload(livePayload?.round || null)}
                onImportVotes={() => importLiveVotes(livePayload?.round || null)}
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

            {activeMenu === "mercato-live" && (
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

            {activeMenu === "tier-list" && (
              <PremiumInsightsSection
                mode="tier-list"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "potenza-titolari" && (
              <PremiumInsightsSection
                mode="potenza-squadra-titolari"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "potenza-totale" && (
              <PremiumInsightsSection
                mode="potenza-squadra-totale"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "classifica-potenza" && (
              <PremiumInsightsSection
                mode="classifica-potenza"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "classifica-lega" && (
              <PremiumInsightsSection
                mode="classifica-reale-lega"
                insights={premiumInsights}
                loading={false}
                error=""
                onReload={null}
                leagueStandings={marketStandings}
                openPlayer={openPlayer}
              />
            )}

            {activeMenu === "classifica-reale-lega" && (
              <PremiumInsightsSection
                mode="classifica-reale-lega"
                insights={premiumInsights}
                loading={premiumInsightsLoading}
                error={premiumInsightsError}
                onReload={() => loadPremiumInsights(true)}
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

            {activeMenu === "predictions" && (
              <PremiumInsightsSection
                mode="predictions-campionato-fixtures"
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
                        <span className="muted">Squadra Â· Ruolo</span>
                      </div>
                      <strong>
                        {playerProfile?.Squadra || "-"} Â· {playerProfile?.Ruolo || "-"}
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
                          ? ` Â· Stagione ${adminResetUsage.season}`
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
                    <h3>Piani e Sospensioni</h3>
                  </div>
                  <div className="admin-actions">
                    <div className="admin-row admin-row-stacked">
                      <p className="muted">
                        Listino: Base mensile {formatEuro(subscriptionPriceFor("base", "monthly"))} · Base 9 mesi{" "}
                        {formatEuro(subscriptionPriceFor("base", "season9"))} · Premium mensile{" "}
                        {formatEuro(subscriptionPriceFor("premium", "monthly"))} · Premium 9 mesi{" "}
                        {formatEuro(subscriptionPriceFor("premium", "season9"))}
                      </p>
                    </div>
                    <div className="admin-row">
                      <input
                        className="input"
                        placeholder="Key da aggiornare piano"
                        value={adminSubKey}
                        onChange={(e) => setAdminSubKey(e.target.value)}
                      />
                      <select
                        className="input"
                        value={adminSubPlan}
                        onChange={(e) => setAdminSubPlan(e.target.value)}
                      >
                        <option value="trial">Trial</option>
                        <option value="base">Base</option>
                        <option value="premium">Premium</option>
                      </select>
                      <select
                        className="input"
                        value={adminSubCycle}
                        onChange={(e) => setAdminSubCycle(e.target.value)}
                        disabled={adminSubPlan === "trial"}
                      >
                        <option value="monthly">Mensile</option>
                        <option value="season9">9 mesi</option>
                        <option value="trial">Trial</option>
                      </select>
                      <label className="admin-checkbox">
                        <input
                          type="checkbox"
                          checked={adminSubImmediate}
                          onChange={(e) => setAdminSubImmediate(e.target.checked)}
                        />
                        <span>Applica subito</span>
                      </label>
                      <button className="ghost" onClick={setSubscriptionAdmin}>
                        Imposta piano
                      </button>
                    </div>

                    <div className="admin-row">
                      <input
                        className="input"
                        placeholder="Key da sospendere/riattivare"
                        value={adminBlockKey}
                        onChange={(e) => setAdminBlockKey(e.target.value)}
                      />
                      <select
                        className="input"
                        value={adminBlockValue ? "blocked" : "active"}
                        onChange={(e) => setAdminBlockValue(e.target.value === "blocked")}
                      >
                        <option value="blocked">Sospendi</option>
                        <option value="active">Riattiva</option>
                      </select>
                      <input
                        className="input"
                        placeholder="Motivo (opzionale)"
                        value={adminBlockReason}
                        onChange={(e) => setAdminBlockReason(e.target.value)}
                      />
                      <button className="ghost" onClick={setSubscriptionBlockAdmin}>
                        Salva stato
                      </button>
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
                        const sub = normalizeSubscription(item.subscription || {});
                        return (
                          <div key={item.key} className="list-item player-card">
                            <div>
                              <p>{String(item.key || "").toUpperCase()}</p>
                              <span className="muted">
                                {item.is_admin ? "ADMIN" : "USER"} - {item.used ? "Attivata" : "Non usata"}
                              </span>
                              <span className="muted">Team: {item.team || "-"}</span>
                              <span className="muted">
                                Piano: {planTierLabel(sub.plan_tier)} ({billingCycleLabel(sub.billing_cycle)}) - {sub.status === "blocked" ? "Bloccata" : "Attiva"}
                              </span>
                              {sub.blocked_reason ? (
                                <span className="muted">Blocco: {blockedReasonLabel(sub.blocked_reason)}</span>
                              ) : null}
                              {sub.plan_expires_at ? (
                                <span className="muted">Scadenza: {formatDataStatusDate(sub.plan_expires_at)}</span>
                              ) : null}
                              {sub.pending_plan_tier ? (
                                <span className="muted">
                                  Cambio pianificato: {planTierLabel(sub.pending_plan_tier)} tra {formatCountdown(Number(sub.seconds_to_pending || 0))}
                                </span>
                              ) : null}
                              <span className="muted">
                                Reset: {item.reset_used ?? 0}/{item.reset_limit ?? 3}
                                {item.reset_season ? ` · Stagione ${item.reset_season}` : ""}
                              </span>
                              <span className="muted">
                                Ultimo accesso: {item.online ? "Online" : formatLastAccess(item.last_seen_at || item.used_at)}
                              </span>
                            </div>
                            <button className="ghost" onClick={() => deleteTeamKeyAdmin(item.key)}>
                              Elimina
                            </button>
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


