export default function ListoneSection({
  quoteRole,
  setQuoteRole,
  quoteTeam,
  setQuoteTeam,
  quoteOrder,
  setQuoteOrder,
  quoteList,
  listoneQuery,
  setListoneQuery,
  formatInt,
  slugify,
  openPlayer,
}) {
  return (
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

        <label className="field listone-search">
          <span>Cerca giocatore</span>
          <input
            placeholder="Es. Maignan, Barella..."
            value={listoneQuery}
            onChange={(e) => setListoneQuery(e.target.value)}
          />
        </label>

        <div className="list">
          {(() => {
            const baseList = quoteList
              .filter((it) => (quoteTeam === "all" ? true : it.Squadra === quoteTeam))
              .map((it, index) => ({ ...it, rank: index + 1 }));
            const filtered = listoneQuery.trim()
              ? baseList.filter((it) =>
                  String(it.Giocatore || "")
                    .toLowerCase()
                    .includes(listoneQuery.trim().toLowerCase())
                )
              : baseList;

            return filtered.map((it, idx) => {
              const itemSlug = slugify(it.Giocatore);
              return (
                <div
                  key={`${it.Giocatore}-${idx}`}
                  id={`listone-${itemSlug}`}
                  className="list-item boxed player-card"
                  onClick={() => openPlayer(it.Giocatore)}
                >
                  <div>
                    <p className="rank-title">
                      <span className="rank-badge">#{it.rank ?? idx + 1}</span>
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
                    <span className="muted">Ruolo: {it.Ruolo || "-"}</span>
                  </div>
                  <strong>{formatInt(it.PrezzoAttuale)}</strong>
                </div>
              );
            });
          })()}
        </div>
      </div>
    </section>
  );
}
