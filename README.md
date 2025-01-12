# **ETL proces Chinook**

Cieľom môjho projektu je spracovanie dát z databázy Chinook prostredníctvom ETL procesu, pričom výsledný model bude založený na hviezdicovej schéme.

**Zdroj dát:** 
Chinook databáza je vzorová relačná databáza, ktorá obsahuje údaje o hudobných albumoch, skladbách, umelcoch, zákazníkoch, objednávkach a faktúrach. 
Dáta z Chinook databázy budú transformované a optimalizované na analytické účely pomocou platformy Snowflake.

---
## **1. Úvod a popis zdrojových dát**
Tento semestrálny projekt sa zameriava na spracovanie a analýzu údajov z databázy Chinook s cieľom preskúmať správanie zákazníkov, ich obľúbené skladby a nákupné preferencie. 
Výsledkom bude identifikácia kľúčových trendov v predaji a záujmoch používateľov, pričom sa zameriame na najpredávanejšie skladby, populárnych interpretov a obľúbené hudobné žánre.
Dataset obsahuje tabulky:
- `playlist`: Obsahuje údaje o playlistoch vytvorených používateľmi.
- `playlisttrack`: Spojovacia tabuľka medzi playlistmi a skladbami.
- `track`: Zahŕňa podrobnosti o jednotlivých skladbách.
- `album`: Obsahuje informácie o hudobných albumoch.
- `artist`: Tabuľka s údajmi o hudobných interpretoch.
- `customer`: Informácie o zákazníkoch, ktorí vykonali nákupy.
- `employee`: Záznamy o zamestnancoch predajnej spoločnosti.
- `genre`: Kategorizácia skladieb podľa žánrov.
- `invoice`: Obsahuje faktúry spolu s detailmi o predajoch.
- `invoiceline`: Záznamy o jednotlivých položkách na faktúrach.
- `mediatype`: Obsahuje informácie o formáte, v ktorom sú skladby dostupné.
 
---
### **1.1 Dátová architektúra**

Zdrojové údaje sú organizované v relačnej štruktúre, ktorá je vizualizovaná prostredníctvom **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Entitno-relačná schéma Chinook </em>
</p>

---
## **2 Dimenzionálny model**

Bol navrhnutý **hviezdicový model (star schema)**, ktorý zabezpečuje efektívne spracovanie a analýzu dát. Centrálna časť modelu je faktová tabuľka **`facts_invoice`**, ktorá je spojená s viacerými dimenziami:
- **`dm_tracks`**: Obsahuje údaje o skladbách vrátane názvu, albumu, interpreta, typu médií a žánru.
- **`dm_customer`**: Zaznamenáva informácie o zákazníkoch, ako je ich ID a mesto.
- **`dm_adress`**: Poskytuje detaily o lokalitách, ako sú ulice, mestá a štáty
- **`dm_date`**: Obsahuje časové údaje vrátane dátumu, dňa, mesiaca, týždňa a roku, ktoré umožňujú analýzu predaja podľa rôznych časových období.



<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Hviezdicová schéma pre Chinook </em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces v Snowflake zahŕňal tri kľúčové fázy: extrakcia (Extract), transformácia (Transform) a načítanie (Load). Cieľom tohto procesu bolo spracovať zdrojové údaje zo staging vrstvy a previesť ich do dimenzionálneho modelu, ktorý je optimalizovaný na následné analytické spracovanie a vizualizáciu výsledkov.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojových súborov vo formáte .csv boli nahrané do Snowflake do dočasného úložiska nazvaného my_stage. Pred nahraním dát bola inicializovaná databáza, dátový sklad a schéma. Následné kroky zahŕňali nahratie údajov do staging tabuliek. Proces bol inicializovaný pomocou nasledujúcich príkazov:

```sql
CREATE DATABASE IF NOT EXISTS SLOTH_CHINOOK;
USE DATABASE SLOTH_CHINOOK;

CREATE WAREHOUSE IF NOT EXISTS SLOTH_CHINOOK_WAREHOUSE;
USE WAREHOUSE SLOTH_CHINOOK_WAREHOUSE;

CREATE SCHEMA IF NOT EXISTS SLOTH_CHINOOK.stages;
CREATE OR REPLACE STAGE my_stage;
```

Kroky extrakcie dát:

V prvej fáze ETL procesu boli vytvorené staging tabuľky, ktoré slúžia na dočasné uloženie zdrojových údajov pred ich transformáciou do finálneho dimenzionálneho modelu. Pre každú zdrojovú entitu (napr. zamestnanci, zákazníci, faktúry, skladby, žánre a ďalšie) bola vytvorená samostatná staging tabuľka.

Príklad pre tabuľku customer_staging:

```sql
CREATE OR REPLACE TABLE customer_staging(
    CustomerId INT,
    FirstName VARCHAR(40),
    LastName VARCHAR(20),
    Company VARCHAR(80),
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60),
    SupportRepId INT
);

COPY INTO customer_staging
FROM @stages.my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

---
### **3.2 Transfor (Transformácia dát)**
Transformácia dát zahŕňala proces čistenia, obohacovania a reorganizácie zdrojových údajov do dimenzionálneho modelu, ktorý umožňuje efektívnu viacdimenzionálnu analýzu. 
V tejto fáze boli údaje transformované do dimenzií a faktových tabuliek, pričom sa použili rôzne SQL operácie na odvodenie potrebných atribútov.

Dimenzia dm_tracks: Táto dimenzia obsahuje informácie o skladbách vrátane názvu, interpreta, albumu, typu média a žánru. Tieto údaje boli extrahované zo staging tabuľky tracks_staging a ďalších súvisiacich tabuliek, ako album_staging, artist_staging a genre_staging.

```sql
CREATE OR REPLACE TABLE dm_tracks AS
SELECT
    ts.TrackId AS iddm_tracks,
    ts.Name AS name,
    ts.Composer AS composer,
    ts.Milliseconds AS milliseconds,
    ts.Bytes AS bytes,
    al.Title AS album,
    an.Name AS artist_name,
    mt.Name AS media_type,
    gn.Name AS genre_name,
FROM track_staging ts
 JOIN album_staging al ON ts.AlbumId = al.AlbumId
 JOIN artist_staging an ON al.ArtistId = an.ArtistId
 JOIN mediatype_staging mt ON ts.MediaTypeId = mt.MediaTypeId
 JOIN genre_staging gn ON ts.GenreId = gn.GenreId;
```

Dimenzia dm_address: Dimenzia dm_address obsahuje geografické údaje, ako sú ulica, mesto, štát a krajina. Tieto údaje boli extrahované zo staging tabuľky customer_staging, ktorá obsahuje informácie o zákazníkoch vrátane ich adries.

```sql
CREATE OR REPLACE TABLE dm_address AS
SELECT
    ROW_NUMBER() OVER (ORDER BY BillingAddress, BillingState, BillingCity) AS id_address,
    BillingAddress AS street,
    BillingState AS state,
    BillingCity AS city
FROM (
    SELECT DISTINCT
        BillingAddress,
        BillingState,
        BillingCity
    FROM invoice_staging
);
```

---    
### **3.3 Load (Načítanie dát)**
Po úspešnom vytvorení všetkých dimenzionálnych a faktových tabuliek boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska a znížili sa náklady spojené s uchovávaním dočasných údajov. Tento krok je dôležitý aj z hľadiska zabezpečenia dát, aby sa eliminovala možnosť neúmyselného použitia neaktuálnych údajov zo staging vrstvy.

```sql
DROP TABLE IF EXISTS employee_staging;
DROP TABLE IF EXISTS customer_staging;
DROP TABLE IF EXISTS invoice_staging;
DROP TABLE IF EXISTS invoiceline_staging;
DROP TABLE IF EXISTS playlisttrack_staging;
DROP TABLE IF EXISTS track_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS playlist_staging;
DROP TABLE IF EXISTS album_staging;
DROP TABLE IF EXISTS artist_staging;
DROP TABLE IF EXISTS mediatype_staging;
```

---
## **4 Vizualizácia dát**
<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Dashboard datasetu Chinook </em>
</p>

---  

### **4.1 Celkový predaj podľa mesiaca**
 Táto vizualizácia poskytuje prehľad o celkových príjmoch v každom mesiaci za jednotlivé roky. Na zobrazenie časových období používa kombináciu roku a mesiaca vo formáte „rok-mesiac“ (napr. „2023-01“). Celkové príjmy sú vypočítané ako súčet hodnôt faktúr, ktoré sú priradené k príslušným dátumom cez tabuľku dátumov. Tento pohľad je užitočný na detailnú analýzu mesačných trendov v predaji, napríklad na zistenie, či v určitých mesiacoch dochádza k zvýšenému predaju alebo výkyvom v tržbách.

```sql
SELECT dd.month, dd.year, SUM(fi.total) AS total_sales
FROM facts_invoice fi
JOIN dm_date dd ON fi.dm_date_iddm_date = dd.iddm_date
GROUP BY dd.month, dd.year;
```

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Chinook Hviezdicová Schéma </em>
</p>

---  

### **4.2 Počet objednávok podľa mesta**
  Táto vizualizácia analyzuje celkový predaj podľa mesta. Pre každé mesto vypočíta počet objednávok na základe faktúr priradených k adresám. 
 Umožňuje identifikovať mestá s najväčším počtom objednávok a získať prehľad o predajných trendoch v jednotlivých lokalitách.
 
```sql
SELECT da.city, COUNT(fi.idfacts_invoice) AS order_count
FROM facts_invoice fi
JOIN dm_address da ON fi.dm_address_iddm_address = da.iddm_address
GROUP BY da.city;
```

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Chinook Hviezdicová Schéma </em>
</p>

--- 

### **4.3 Celkový predaj podľa skladieb**
 Táto vizualizácia analyzuje celkové predaje podľa skladieb. Pre každú skladbu vypočíta celkový predaj na základe faktúr priradených ku konkrétnym skladbám. Umožňuje identifikovať najpredávanejšie skladby a ich prínos k celkovým predajom.

 ```sql
SELECT dt.iddm_tracks, dt.name, SUM(fi.total) AS total_sales
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.iddm_tracks, dt.name;
```

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Chinook Hviezdicová Schéma </em>
</p>


---    

### **4.4 Najpredávanejšie žánre**
 Táto vizualizácia analyzuje najpredávanejšie žánre. Pre každý žáner vypočíta celkový predaj na základe predaných kusov skladieb priradených ku konkrétnym žánrom. Umožňuje identifikovať žánre s najvyšším počtom predaných skladieb a získať prehľad o predajných trendoch v rámci rôznych hudobných štýlov.
 ```sql
SELECT dt.genre_name, SUM(fi.quantity) AS total_sold
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.genre_name
ORDER BY total_sold DESC;
```

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Chinook Hviezdicová Schéma </em>
</p>


---    

### **4.5 Najpredávanejšie skladby**
 Táto vizualizácia zobrazuje 10 najpredávanejších skladieb. Pre každú skladbu vypočíta celkový počet predaných kusov na základe faktúr priradených ku konkrétnym skladbám. Umožňuje identifikovať skladby s najvyšším počtom predaných kusov a získať prehľad o najobľúbenejších skladbách medzi zákazníkmi.
 ```sql
SELECT dt.name, SUM(fi.quantity) AS total_sold
FROM facts_invoice fi
JOIN dm_tracks dt ON fi.dm_tracks_iddm_tracks = dt.iddm_tracks
GROUP BY dt.name
ORDER BY total_sold DESC
LIMIT 10;
```

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em> Chinook Hviezdicová Schéma </em>
</p>



Autor: Natália Kűhnová
