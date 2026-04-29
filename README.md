# 🛒 E-Commerce ETL Pipeline

Lekki pipeline ETL napisany w Pythonie, który wyciąga surowe dane transakcyjne z pliku CSV, waliduje je i czyści, a następnie ładuje do bazy PostgreSQL — podzielone na czytelne tabele wymiarowe i faktów.

---

## 📋 Spis treści

- [Opis](#opis)
- [Architektura pipeline'u](#architektura-pipelinu)
- [Struktura projektu](#struktura-projektu)
- [Wymagania](#wymagania)
- [Instalacja](#instalacja)
- [Konfiguracja](#konfiguracja)
- [Uruchomienie](#uruchomienie)
- [Model danych](#model-danych)
- [Obsługa odrzuconych danych](#obsługa-odrzuconych-danych)

---

## Opis

Pipeline przetwarza dane sprzedażowe e-commerce (np. z [UCI Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/online+retail)) i ładuje je do relacyjnej bazy PostgreSQL. Automatycznie obsługuje typowe problemy jakości danych — brakujące wartości i nieprawidłowe ilości.

**Co robi:**
- ✅ Wczytuje surowe dane CSV z obsługą kodowania
- ✅ Usuwa nieprawidłowe wiersze (ujemne/zerowe ilości lub ceny)
- ✅ Uzupełnia brakujące wartości w zależności od typu kolumny
- ✅ Dzieli dane na znormalizowane tabele `customers`, `products` i `orders`
- ✅ Zapisuje odrzucone wiersze do osobnego pliku audytowego

---

## Architektura pipeline'u

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌─────────────┐
│   data.csv  │────▶│     Extract      │────▶│    Transform     │────▶│    Load     │
│ (surowy CSV)│     │  load_raw_data() │     │ clean_quantity() │     │ save_to_db()│
└─────────────┘     └──────────────────┘     │  clean_data()    │     └──────┬──────┘
                                             └──────────────────┘            │
                                                      │                      ▼
                                                      │            ┌─────────────────┐
                                             błędne   │            │   PostgreSQL     │
                                             wiersze  ▼            │                 │
                                             ┌──────────────┐     │  • customers    │
                                             │rejected_data │     │  • products     │
                                             │    .csv      │     │  • orders       │
                                             └──────────────┘     └─────────────────┘
```

---


## Wymagania

- Python 3.8+
- Baza danych PostgreSQL (lokalna lub zdalna)
- pip

---

## Instalacja

**1. Sklonuj repozytorium**
```bash
git clone https://github.com/twoj-username/ecommerce-etl.git
cd ecommerce-etl
```

**2. Zainstaluj zależności**
```bash
pip install -r requirements.txt
```

**3. Utwórz plik `.env`**
```bash
cp .env.example .env
```
Następnie uzupełnij dane do bazy (zobacz [Konfiguracja](#konfiguracja)).

**4. Dodaj plik z danymi**

Umieść plik CSV w głównym katalogu projektu i nazwij go `data.csv`.

---

## Konfiguracja

Cała wrażliwa konfiguracja odbywa się przez zmienne środowiskowe. Utwórz plik `.env` w głównym katalogu projektu:

```env
DB_USER=twoj_uzytkownik
DB_PASSWORD=twoje_haslo
DB_ADRESS=localhost
DB_PORT=5432
DB=ecommerce_db
```

> ⚠️ Nigdy nie commituj pliku `.env`. Jest już dodany do `.gitignore`.

---

## Uruchomienie

Uruchom pełny pipeline komendą:

```bash
python etl.py
```

Przykładowy output:

```
=== ETL Pipeline started ===
Loading data from: data.csv
Successfully loaded 541909 rows.
Validating Quantity and UnitPrice values...
Warning: 10624 invalid rows detected and removed.
Rows remaining after validation: 531285
Checking for missing values...
Found integrity errors in 1 column(s): ['CustomerID']
  Column 'CustomerID': missing values filled with 0.
Connecting to database as: admin
Saving 4373 customer records...
Saving 3958 product records...
Saving 531285 order records...
All data successfully saved to the database!
Database connection closed.
=== ETL Pipeline finished ===
```

---

## Model danych

Pipeline normalizuje płaski CSV do trzech tabel:

### `customers`
| Kolumna | Typ | Opis |
|---|---|---|
| CustomerID | integer | Unikalny identyfikator klienta |
| Country | varchar | Kraj klienta |

### `products`
| Kolumna | Typ | Opis |
|---|---|---|
| StockCode | varchar | Unikalny identyfikator produktu |
| Description | varchar | Nazwa / opis produktu |
| UnitPrice | float | Cena za jednostkę |

### `orders`
| Kolumna | Typ | Opis |
|---|---|---|
| InvoiceNo | varchar | Numer faktury |
| InvoiceDate | varchar | Data i czas transakcji |
| CustomerID | integer | Klucz obcy → customers |
| StockCode | varchar | Klucz obcy → products |
| Quantity | integer | Liczba zakupionych jednostek |

---

## Obsługa odrzuconych danych

Wiersze, które nie przejdą walidacji, **nie są po cichu usuwane**. Trafiają do pliku `rejected_data.csv` do późniejszego przeglądu.

Wiersz jest odrzucany gdy:
- `Quantity` jest zerowe lub ujemne (np. zwroty, anulowania)
- `UnitPrice` jest zerowe lub ujemne

Przy każdym uruchomieniu nowe odrzucone wiersze są **dopisywane** do istniejącego pliku i deduplikowane po `InvoiceNo` + `StockCode` — plik działa jako trwały log audytowy między kolejnymi uruchomieniami.
