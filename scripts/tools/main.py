import pandas as pd
import os

class FantaPortoscusoAnalyzer:
    EXPECTED_TEAM_COUNT = 84
    ROLE_ORDER = ['P', 'D', 'C', 'A']
    ROLE_COUNTS = {'P': 3, 'D': 7, 'C': 8, 'A': 5}
    MAX_PER_REAL_TEAM = 3
    STATS_REQUIRED_COLUMNS = [
        'Giocatore', 'Squadra', 'Gol', 'Autogol', 'RigoriParati',
        'RigoriSbagliati', 'Assist', 'Ammonizioni', 'Espulsioni', 'Cleansheet'
    ]

    def __init__(self, csv_path: str = "data/rose_fantaportoscuso.csv"):
        self.csv_path = csv_path
        self.df = None
        self.df_quotazioni = None
        self.df_stats = None
        self.load_data()

    def load_data(self):
        """Carica i dati dal CSV con validazione"""
        try:
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"File {self.csv_path} non trovato!")

            self.df = pd.read_csv(self.csv_path, encoding='utf-8')

            # Validazione colonne richieste
            required_columns = ['Team', 'Ruolo', 'Giocatore', 'Squadra']
            if not all(col in self.df.columns for col in required_columns):
                raise ValueError(f"Colonne richieste mancanti. Necessarie: {required_columns}")

            # Aggiungi colonna prezzi acquisto se non presente
            if 'PrezzoAcquisto' not in self.df.columns:
                self.df['PrezzoAcquisto'] = 0

            # Aggiungi colonna prezzi attuali se non presente
            if 'PrezzoAttuale' not in self.df.columns:
                self.df['PrezzoAttuale'] = 0

            # Pulizia dati
            self.df = self.df.dropna(subset=required_columns)
            for col in required_columns:
                self.df[col] = self.df[col].astype(str).str.strip()
            self.df['Ruolo'] = self.df['Ruolo'].str.upper()

            # Rimuovi righe con campi obbligatori vuoti
            empty_mask = self.df[required_columns].eq('').any(axis=1)
            if empty_mask.any():
                removed = int(empty_mask.sum())
                self.df = self.df[~empty_mask]
                print(f"Attenzione: Rimosse {removed} righe con campi obbligatori vuoti")

            # Controllo rose complete (23 giocatori)
            counts = self.df['Team'].value_counts()
            incomplete = counts[counts != 23]
            if not incomplete.empty:
                print("Attenzione: Alcune squadre non hanno 23 giocatori:")
                for team, count in incomplete.items():
                    print(f"  - {team}: {count} giocatori")
            else:
                print("[OK] Tutte le squadre hanno 23 giocatori!")

            print(f"[OK] Dati caricati correttamente: {len(self.df)} giocatori in {self.df['Team'].nunique()} squadre")
            self._validate_rosa_constraints()

            # Carica quotazioni se disponibile
            try:
                self.df_quotazioni = pd.read_csv('data/quotazioni.csv', encoding='utf-8')
                self.df_quotazioni['Giocatore'] = self.df_quotazioni['Giocatore'].str.strip()
                print(f"[OK] Quotazioni caricate: {len(self.df_quotazioni)} giocatori")
            except FileNotFoundError:
                print("Info: File quotazioni.csv non trovato - prezzi non disponibili")
                self.df_quotazioni = None
            except Exception as e:
                print(f"Attenzione: Errore caricamento quotazioni: {e}")
                self.df_quotazioni = None

            # Carica statistiche giocatori se disponibile
            try:
                self.df_stats = pd.read_csv('data/statistiche_giocatori.csv', encoding='utf-8')
                self.df_stats['Giocatore'] = self.df_stats['Giocatore'].str.strip()
                self.df_stats['Squadra'] = self.df_stats['Squadra'].str.strip()
                print(f"[OK] Statistiche giocatori caricate: {len(self.df_stats)} giocatori")
            except FileNotFoundError:
                print("Info: File statistiche_giocatori.csv non trovato - statistiche non disponibili")
                self.df_stats = None
            except Exception as e:
                print(f"Attenzione: Errore caricamento statistiche: {e}")
                self.df_stats = None

        except Exception as e:
            print(f"[ERRORE] Errore nel caricamento dei dati: {e}")
            self.df = None

    def valida_team(self, team: str) -> bool:
        """Valida se il team esiste"""
        if self.df is None:
            return False
        if not team or not team.strip():
            return False
        return team.strip() in self.df['Team'].values

    def valida_ruolo(self, ruolo: str) -> bool:
        """Valida se il ruolo e valido"""
        return ruolo.upper() in self.ROLE_ORDER

    def mostra_squadra(self, nome_team: str) -> None:
        """Mostra i giocatori di una squadra"""
        if not self.valida_team(nome_team):
            print(f"[ERRORE] Squadra '{nome_team}' non trovata!")
            return

        team_df = self.df[self.df['Team'] == nome_team]
        print(f"\nSquadra: {nome_team}")
        print(f"Giocatori totali: {len(team_df)}")

        # Raggruppa per ruolo
        ruoli_ordine = ['P', 'D', 'C', 'A']
        for ruolo in ruoli_ordine:
            giocatori_ruolo = team_df[team_df['Ruolo'] == ruolo]
            if not giocatori_ruolo.empty:
                print(f"\n{ruolo}:")
                for _, row in giocatori_ruolo.iterrows():
                    print(f"  - {row['Giocatore']} ({row['Squadra']})")

    def cerca_giocatore(self, nome_giocatore: str) -> None:
        """Cerca un giocatore per nome"""
        if not nome_giocatore or not nome_giocatore.strip():
            print("[ERRORE] Inserisci un nome valido!")
            return

        # Ricerca case-insensitive
        mask = self.df['Giocatore'].str.lower().str.contains(nome_giocatore.lower(), na=False)
        risultati = self.df[mask]

        if risultati.empty:
            print(f"[ERRORE] Nessun giocatore trovato con '{nome_giocatore}'")
            return

        # Raggruppa per giocatore univoco
        gruppi = risultati.groupby(['Giocatore', 'Squadra', 'Ruolo'])

        print(f"\nRisultati per '{nome_giocatore}':")
        for (giocatore, squadra, ruolo), gruppo in gruppi:
            articolo = self._get_articolo(squadra)
            print(f"\n{giocatore} {articolo} {squadra} ({ruolo}):")
            for team in sorted(gruppo['Team'].unique()):
                print(f"  - {team}")

    def _get_articolo(self, squadra: str) -> str:
        """Determina l'articolo corretto per la squadra"""
        squadra_lower = squadra.lower().strip()
        if not squadra_lower:
            return "del"

        # Casi speciali
        if squadra_lower in ['cremonese', 'juventus', 'fiorentina', 'roma']:
            return "della"

        # Se inizia con vocale
        vocali = 'aeiou'
        if squadra_lower[0] in vocali:
            return "dell'"

        # Altrimenti consonante
        return "del"

    def statistiche_generali(self) -> None:
        """Mostra statistiche generali della lega"""
        if self.df is None:
            print("[ERRORE] Dati non caricati!")
            return

        print("\nStatistiche Generali Fantacalcio")
        print(f"  Squadre totali: {self.df['Team'].nunique()}")
        print(f"  Giocatori totali: {len(self.df)}")

        # Distribuzione ruoli
        distribuzione_ruoli = self.df['Ruolo'].value_counts()
        print("\nDistribuzione ruoli:")
        for ruolo, count in distribuzione_ruoli.items():
            print(f"  {ruolo}: {count}")

        # Squadre reali più rappresentate
        squadre_reali = self.df['Squadra'].value_counts().head(10)
        print("\nTop 10 squadre reali più rappresentate:")
        for squadra, count in squadre_reali.items():
            print(f"  {squadra}: {count} giocatori")

    def statistiche_squadre_reali(self) -> None:
        """Statistiche sui giocatori unici per squadra reale"""
        if self.df is None:
            print("[ERRORE] Dati non caricati!")
            return

        print("\nGiocatori Unici per Squadra Reale")
        print("Numero di giocatori diversi selezionati nella lega:")

        # Conta giocatori unici per squadra reale
        stats = self.df.groupby('Squadra')['Giocatore'].nunique().sort_values(ascending=False)

        for squadra, count in stats.items():
            articolo = self._get_articolo(squadra)
            print(f"  {squadra}: {count} giocatori unici")

    def statistiche_coverage_serie_a(self, totale_giocatori_serie_a: int = 549) -> None:
        """Statistiche coverage Serie A"""
        if self.df is None:
            print("[ERRORE] Dati non caricati!")
            return

        giocatori_league = len(self.df)
        giocatori_unici_league = self.df['Giocatore'].nunique()

        print("\nCoverage Serie A")
        print(f"Giocatori selezionati nella lega: {giocatori_league}")
        print(f"Giocatori unici selezionati: {giocatori_unici_league}")

        if totale_giocatori_serie_a:
            coverage = (giocatori_unici_league / totale_giocatori_serie_a) * 100
            print(f"Totale giocatori Serie A: {totale_giocatori_serie_a}")
            print(f"Coverage: {coverage:.1f}%")
        else:
            print("Nota: fornisci il totale giocatori Serie A per calcolare il coverage")

    def classifica_plusvalenze(self) -> None:
        """Classifica squadre per plusvalenza economica"""
        if self.df is None:
            print("[ERRORE] Dati non caricati!")
            return

        if 'PrezzoAcquisto' not in self.df.columns or 'PrezzoAttuale' not in self.df.columns:
            print("[ERRORE] Colonne prezzi non disponibili!")
            return

        print("\nClassifica Plusvalenze Economiche")
        print("=" * 60)

        # Calcola plusvalenza per squadra
        plusvalenze = []
        for team in self.df['Team'].unique():
            team_df = self.df[self.df['Team'] == team]
            costo_acquisto = team_df['PrezzoAcquisto'].sum()
            valore_attuale = team_df['PrezzoAttuale'].sum()
            plusvalenza_assoluta = valore_attuale - costo_acquisto
            plusvalenza_percentuale = (plusvalenza_assoluta / costo_acquisto * 100) if costo_acquisto > 0 else 0

            plusvalenze.append({
                'team': team,
                'costo_acquisto': costo_acquisto,
                'valore_attuale': valore_attuale,
                'plusvalenza_assoluta': plusvalenza_assoluta,
                'plusvalenza_percentuale': plusvalenza_percentuale
            })

        # Ordina per plusvalenza assoluta (decrescente)
        plusvalenze.sort(key=lambda x: x['plusvalenza_assoluta'], reverse=True)

        print(f"{'Pos':<3} {'Squadra':<25} {'Plusvalenza':<12} {'Percentuale':<12}")
        print("-" * 60)
        for i, p in enumerate(plusvalenze, 1):
            segno = "+" if p['plusvalenza_assoluta'] >= 0 else ""
            print(f"{i:<3} {p['team']:<25} {segno}{p['plusvalenza_assoluta']:<11.1f} {segno}{p['plusvalenza_percentuale']:<11.1f}%")

        # Classifica per percentuale
        print("\nClassifica per Plusvalenza Percentuale")
        print("=" * 60)
        print(f"{'Pos':<3} {'Squadra':<25} {'Plusvalenza':<12} {'Percentuale':<12}")
        print("-" * 60)

        plusvalenze_percentuale = sorted(plusvalenze, key=lambda x: x['plusvalenza_percentuale'], reverse=True)
        for i, p in enumerate(plusvalenze_percentuale, 1):
            segno = "+" if p['plusvalenza_percentuale'] >= 0 else ""
            print(f"{i:<3} {p['team']:<25} {segno}{p['plusvalenza_assoluta']:<11.1f} {segno}{p['plusvalenza_percentuale']:<11.1f}%")

    def classifica_valore_squadre(self) -> None:
        """Classifica valore squadre basato su statistiche giocatori"""
        if self.df is None or self.df_stats is None:
            print("[ERRORE] Dati o statistiche non caricati!")
            return
        if not self._validate_stats_columns():
            return

        print("\nClassifica Valore Squadre (Statistiche)")
        print("=" * 60)

        stats = self._prepare_stats()

        # Unisci con dati squadre
        df_merged = self.df.merge(stats, on=['Giocatore', 'Squadra'], how='left')
        df_merged['Punteggio'] = df_merged['Punteggio'].fillna(0)

        # Calcola punteggio totale per squadra
        valore_squadre = []
        for team in df_merged['Team'].unique():
            team_df = df_merged[df_merged['Team'] == team]
            punteggio_totale = team_df['Punteggio'].sum()
            num_giocatori = len(team_df)

            valore_squadre.append({
                'team': team,
                'punteggio_totale': punteggio_totale,
                'num_giocatori': num_giocatori,
                'punteggio_medio': punteggio_totale / num_giocatori if num_giocatori > 0 else 0
            })

        # Ordina per punteggio totale
        valore_squadre.sort(key=lambda x: x['punteggio_totale'], reverse=True)

        print(f"{'Pos':<3} {'Squadra':<25} {'Punteggio':<10} {'Media':<8}")
        print("-" * 60)
        for i, v in enumerate(valore_squadre, 1):
            print(f"{i:<3} {v['team']:<25} {v['punteggio_totale']:<10.1f} {v['punteggio_medio']:<8.1f}")

    def classifica_giocatori(self, top_n: int = 20) -> None:
        """Classifica giocatori basata su statistiche"""
        if self.df_stats is None:
            print("[ERRORE] Statistiche giocatori non caricate!")
            return
        if not self._validate_stats_columns():
            return

        print(f"\nClassifica Top {top_n} Giocatori (Statistiche)")
        print("=" * 60)

        stats = self._prepare_stats()

        # Ordina per punteggio
        top_giocatori = stats.sort_values('Punteggio', ascending=False).head(top_n)

        print(f"{'Pos':<3} {'Giocatore':<25} {'Squadra':<15} {'Punteggio':<10}")
        print("-" * 60)
        for i, (_, row) in enumerate(top_giocatori.iterrows(), 1):
            print(f"{i:<3} {row['Giocatore']:<25} {row['Squadra']:<15} {row['Punteggio']:<10.1f}")

        # Mostra anche statistiche dettagliate del top player
        if not top_giocatori.empty:
            top_player = top_giocatori.iloc[0]
            print(f"\nStatistiche dettagliate di {top_player['Giocatore']} ({top_player['Squadra']}):")
            print(f"  Gol: {top_player['Gol']}")
            print(f"  Assist: {top_player['Assist']}")
            print(f"  Autogol: {top_player['Autogol']}")
            print(f"  Rigori Parati: {top_player['RigoriParati']}")
            print(f"  Rigori Sbagliati: {top_player['RigoriSbagliati']}")
            print(f"  Ammonizioni: {top_player['Ammonizioni']}")
            print(f"  Espulsioni: {top_player['Espulsioni']}")
            print(f"  Cleansheet: {top_player['Cleansheet']}")
            print(f"  Punteggio Totale: {top_player['Punteggio']:.1f}")

    def _validate_rosa_constraints(self) -> None:
        if self.df is None:
            return

        team_count = self.df['Team'].nunique()
        if team_count != self.EXPECTED_TEAM_COUNT:
            print(f"Attenzione: Numero squadre atteso {self.EXPECTED_TEAM_COUNT}, trovato {team_count}")

        invalid_roles = self.df[~self.df['Ruolo'].isin(self.ROLE_ORDER)]
        if not invalid_roles.empty:
            invalid_values = sorted(invalid_roles['Ruolo'].unique())
            print(f"Attenzione: Ruoli non validi trovati: {invalid_values}")

        # Controllo composizione ruoli per squadra
        role_counts = (
            self.df[self.df['Ruolo'].isin(self.ROLE_ORDER)]
            .groupby(['Team', 'Ruolo'])
            .size()
            .unstack(fill_value=0)
        )
        ruolo_issues = []
        for team, row in role_counts.iterrows():
            for ruolo, expected in self.ROLE_COUNTS.items():
                if row.get(ruolo, 0) != expected:
                    ruolo_issues.append((team, ruolo, int(row.get(ruolo, 0))))
        if ruolo_issues:
            print("Attenzione: Composizione ruoli non valida per alcune squadre:")
            for team, ruolo, count in ruolo_issues[:20]:
                print(f"  - {team}: {ruolo}={count} (atteso {self.ROLE_COUNTS[ruolo]})")
            if len(ruolo_issues) > 20:
                print(f"  ... e altre {len(ruolo_issues) - 20} anomalie")

        # Max 3 giocatori per squadra reale
        per_real_team = self.df.groupby(['Team', 'Squadra'])['Giocatore'].nunique()
        over_limit = per_real_team[per_real_team > self.MAX_PER_REAL_TEAM]
        if not over_limit.empty:
            print("Attenzione: Limite giocatori per squadra reale superato:")
            for (team, squadra), count in over_limit.items():
                print(f"  - {team}: {squadra} = {count} (max {self.MAX_PER_REAL_TEAM})")

    def _validate_stats_columns(self) -> bool:
        missing = [c for c in self.STATS_REQUIRED_COLUMNS if c not in self.df_stats.columns]
        if missing:
            print(f"[ERRORE] Colonne statistiche mancanti: {missing}")
            return False
        return True

    def _prepare_stats(self) -> pd.DataFrame:
        stats = self.df_stats.copy()
        numeric_cols = [
            'Gol', 'Autogol', 'RigoriParati', 'RigoriSbagliati',
            'Assist', 'Ammonizioni', 'Espulsioni', 'Cleansheet'
        ]
        for col in numeric_cols:
            stats[col] = pd.to_numeric(stats[col], errors='coerce').fillna(0)

        stats['Punteggio'] = (
            stats['Gol'] * 3 +
            stats['Autogol'] * (-2) +
            stats['RigoriParati'] * 3 +
            stats['RigoriSbagliati'] * (-3) +
            stats['Assist'] * 1 +
            stats['Ammonizioni'] * (-0.5) +
            stats['Espulsioni'] * (-1) +
            stats['Cleansheet'] * 1
        )
        return stats


def main():
    analyzer = FantaPortoscusoAnalyzer()

    if analyzer.df is None:
        print("Impossibile continuare senza dati validi.")
        return

    while True:
        print("\n" + "="*50)
        print("FANTAPORTOSCUSO - Analizzatore Fantacalcio")
        print("="*50)
        print("1. Mostra squadra")
        print("2. Cerca giocatore")
        print("3. Statistiche generali")
        print("4. Giocatori unici per squadra reale")
        print("5. Coverage Serie A")
        print("6. Classifica plusvalenze economiche")
        print("7. Classifica valore squadre (statistiche)")
        print("8. Classifica giocatori (statistiche)")
        print("9. Esci")

        try:
            scelta = input("\nScegli un'opzione (1-9): ").strip()

            if scelta == '1':
                team = input("Inserisci il nome della squadra: ").strip()
                analyzer.mostra_squadra(team)
                input("\nPremi INVIO per continuare...")

            elif scelta == '2':
                giocatore = input("Inserisci il nome del giocatore: ").strip()
                analyzer.cerca_giocatore(giocatore)
                input("\nPremi INVIO per continuare...")

            elif scelta == '3':
                analyzer.statistiche_generali()
                input("\nPremi INVIO per continuare...")

            elif scelta == '4':
                analyzer.statistiche_squadre_reali()
                input("\nPremi INVIO per continuare...")

            elif scelta == '5':
                totale = input("Inserisci il totale giocatori Serie A (lascia vuoto se non sai): ").strip()
                totale = int(totale) if totale.isdigit() else None
                analyzer.statistiche_coverage_serie_a(totale)
                input("\nPremi INVIO per continuare...")

            elif scelta == '6':
                analyzer.classifica_plusvalenze()
                input("\nPremi INVIO per continuare...")

            elif scelta == '7':
                analyzer.classifica_valore_squadre()
                input("\nPremi INVIO per continuare...")

            elif scelta == '8':
                top_n = input("Quanti giocatori mostrare? (default 20): ").strip()
                top_n = int(top_n) if top_n.isdigit() else 20
                analyzer.classifica_giocatori(top_n)
                input("\nPremi INVIO per continuare...")

            elif scelta == '9':
                print("Arrivederci!")
                break

            else:
                print("[ERRORE] Scelta non valida! Scegli tra 1-9.")

        except KeyboardInterrupt:
            print("\nInterrotto dall'utente. Arrivederci!")
            break
        except Exception as e:
            print(f"[ERRORE] Errore: {e}")
            input("Premi INVIO per continuare...")


if __name__ == "__main__":
    main()
