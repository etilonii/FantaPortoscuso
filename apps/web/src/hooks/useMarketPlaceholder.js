import { useCallback, useEffect, useState } from "react";

export function useMarketPlaceholder(apiBase, loggedIn) {
  const [marketCountdown, setMarketCountdown] = useState("");
  const [marketItems, setMarketItems] = useState([]);
  const [marketTeams, setMarketTeams] = useState([]);
  const [marketPreview, setMarketPreview] = useState(false);
  const [marketUpdatedAt, setMarketUpdatedAt] = useState("");

  const getMarketCountdown = useCallback(() => {
    const target = new Date("2026-02-04T08:00:00");
    const now = new Date();
    const diff = target.getTime() - now.getTime();
    if (Number.isNaN(diff) || diff <= 0) return "Apertura imminente";
    const totalSeconds = Math.floor(diff / 1000);
    const days = Math.floor(totalSeconds / (24 * 3600));
    const hours = Math.floor((totalSeconds % (24 * 3600)) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${days}g ${hours}h ${minutes}m ${seconds}s`;
  }, []);

  const loadMarket = useCallback(async () => {
    try {
      const res = await fetch(`${apiBase}/data/market`);
      if (!res.ok) return;
      const data = await res.json();
      setMarketItems(data.items || []);
      setMarketTeams(data.teams || []);
      const allDates = [
        ...(data.items || []).map((it) => it.date).filter(Boolean),
        ...(data.teams || []).map((t) => t.last_date).filter(Boolean),
      ];
      const latest = allDates.sort().slice(-1)[0] || "";
      setMarketUpdatedAt(latest);
    } catch {}
  }, [apiBase]);

  useEffect(() => {
    const update = () => setMarketCountdown(getMarketCountdown());
    update();
    const timer = setInterval(update, 1000);
    return () => clearInterval(timer);
  }, [getMarketCountdown]);

  useEffect(() => {
    if (!loggedIn) {
      setMarketItems([]);
      setMarketTeams([]);
      setMarketUpdatedAt("");
      return;
    }
    loadMarket();
  }, [loggedIn, loadMarket]);

  return {
    marketCountdown,
    marketItems,
    marketTeams,
    marketPreview,
    setMarketPreview,
    marketUpdatedAt,
    loadMarket,
  };
}
