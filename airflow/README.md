# HÆ°á»›ng dáº«n Airflow collector HNX / UBCKNN

TÃ i liá»‡u nÃ y mÃ´ táº£ luá»“ng thu tháº­p hiá»‡n táº¡i vÃ  cÃ¡ch tá»± build, khá»Ÿi Ä‘á»™ng, cháº¡y thá»§ cÃ´ng trÃªn Windows báº±ng Docker Desktop. File bÃ¡o cÃ¡o Ä‘Æ°á»£c lÆ°u vÃ o `data/landing` trÃªn á»• D; metadata, tráº¡ng thÃ¡i láº§n cháº¡y vÃ  lá»—i Ä‘Æ°á»£c ghi vÃ o PostgreSQL local Ä‘á»ƒ kiá»ƒm tra báº±ng pgAdmin. MinIO/Spark chÆ°a náº±m trong bÆ°á»›c nÃ y.

## 1. Kiáº¿n trÃºc vÃ  luá»“ng dá»¯ liá»‡u

```text
Airflow DAG (má»—i ngÃ y 07:00, Asia/Ho_Chi_Minh; cÃ³ thá»ƒ trigger thá»§ cÃ´ng)
  â”œâ”€ ensure_schema: táº¡o báº£ng náº¿u chÆ°a cÃ³
  â”œâ”€ collect_hnx: láº¥y danh sÃ¡ch cÃ´ng bá»‘, táº£i file náº¿u tÃ¬m Ä‘Æ°á»£c link táº£i
  â””â”€ collect_ssc: tÆ°Æ¡ng tá»± cho cá»•ng cÃ´ng bá»‘ UBCKNN/SSC
         â”œâ”€ file gá»‘c â†’ ./data/landing/{hnx|ssc}/{ticker}/
         â””â”€ PostgreSQL local â†’ financial_data.collector_runs
                              financial_data.financial_disclosures
```

`airflow/dags/disclosure_sources.py` chá»©a pháº§n crawl Ä‘á»™c láº­p vá»›i Airflow: gá»­i HTTP GET, phÃ¢n tÃ­ch HTML báº±ng BeautifulSoup, phÃ¡t hiá»‡n dÃ²ng cÃ³ cá»¥m tá»« vá» BCTC, láº¥y mÃ£ cá»• phiáº¿u/ngÃ y/link, phÃ¢n trang vá»›i `rel=next`, táº£i attachment vÃ  tÃ­nh SHA-256. Táº£i file theo luá»“ng tá»«ng khá»‘i 1 MiB, giá»›i háº¡n 50 MiB, ghi tá»‡p táº¡m rá»“i Ä‘á»•i tÃªn nguyÃªn tá»­; tÃªn file cÃ³ fingerprint nÃªn láº§n crawl sau khÃ´ng ghi Ä‘Ã¨ báº£n khÃ¡c. Chá»‰ cháº¥p nháº­n host thuá»™c danh sÃ¡ch nguá»“n Ä‘á»ƒ trÃ¡nh táº£i link ngoÃ i nguá»“n.

`airflow/dags/collect_financial_disclosures.py` lÃ  orchestration: Ä‘áº£m báº£o schema, ghi run tráº¡ng thÃ¡i `running`, gá»i hÃ m crawl cho tá»«ng nguá»“n, upsert thÃ´ng bÃ¡o theo `disclosure_id`, rá»“i Ä‘Ã¡nh dáº¥u `success`; ngoáº¡i lá»‡ sáº½ Ä‘Æ°á»£c lÆ°u thÃ nh `failed` vÃ  Airflow retry tá»‘i Ä‘a hai láº§n, cÃ¡ch nhau nÄƒm phÃºt. `airflow/scripts/crawl_to_local.py` gá»i chÃ­nh collector Ä‘Ã³ nhÆ°ng khÃ´ng cáº§n Airflow hay PostgreSQL, tiá»‡n kiá»ƒm tra crawler riÃªng vÃ  xuáº¥t manifest JSON.

## 2. PostgreSQL local vÃ  pgAdmin

PostgreSQL 18 Ä‘ang cháº¡y trÃªn mÃ¡y Windows. Tá»« container Docker Desktop, Ä‘á»‹a chá»‰ `localhost` trá» vÃ o container nÃªn dÃ¹ng `host.docker.internal:5432` Ä‘á»ƒ vá» mÃ¡y Windows.

1. Má»Ÿ pgAdmin, káº¿t ná»‘i server local PostgreSQL 18 báº±ng tÃ i khoáº£n quáº£n trá»‹ Ä‘Ã£ táº¡o khi cÃ i PostgreSQL.
2. In pgAdmin Query Tool connected to database postgres, run the CREATE DATABASE statements in sql/setup_local_postgres.sql one at a time. Reuse the existing postgres role; do not create a new role. Skip a database that already exists.
3. Trong pgAdmin, chá»n `financial_data` â†’ Query Tool â†’ cháº¡y ná»™i dung `sql/init.sql`. DAG cÅ©ng cháº¡y script nÃ y tá»± Ä‘á»™ng khi Ä‘Æ°á»£c trigger; cháº¡y trÆ°á»›c giÃºp xÃ¡c nháº­n role/database Ä‘Ã£ sáºµn sÃ ng.
4. Trong Properties cá»§a server PostgreSQL, kiá»ƒm tra cá»•ng 5432 Ä‘ang láº¯ng nghe. Náº¿u PostgreSQL chá»‰ bind loopback hoáº·c `pg_hba.conf` khÃ´ng cho káº¿t ná»‘i tá»« Docker Desktop, cáº§n cáº¥u hÃ¬nh `listen_addresses` phÃ¹ há»£p vÃ  thÃªm quyá»n truy cáº­p cho máº¡ng Docker Desktop, rá»“i restart service. Giá»¯ xÃ¡c thá»±c `scram-sha-256`; khÃ´ng má»Ÿ PostgreSQL ra Internet.
5. Má»Ÿ `airflow/.env.example`, Ä‘iá»n cÃ¹ng máº­t kháº©u vÃ o hai URL, sao chÃ©p thÃ nh `airflow/.env`. File `.env` chá»©a bÃ­ máº­t nÃªn khÃ´ng commit.

`airflow_meta` chá»‰ chá»©a metadata váº­n hÃ nh cá»§a Airflow; `financial_data` chá»©a báº£ng nghiá»‡p vá»¥ cá»§a collector. Hai DB tÃ¡ch biá»‡t Ä‘á»ƒ dá»… kiá»ƒm tra vÃ  trÃ¡nh trá»™n metadata framework vá»›i dá»¯ liá»‡u dá»± Ã¡n.

### Báº£ng Ä‘á»ƒ theo dÃµi trong pgAdmin

- `collector_runs`: má»™t dÃ²ng cho má»—i nguá»“n trong má»—i láº§n cháº¡y (`run_id`, thá»i gian báº¯t Ä‘áº§u/káº¿t thÃºc, tráº¡ng thÃ¡i, sá»‘ báº£n ghi, lá»—i).
- `financial_disclosures`: má»™t dÃ²ng cho má»—i thÃ´ng bÃ¡o duy nháº¥t; cÃ³ nguá»“n, URL, mÃ£ cá»• phiáº¿u, tiÃªu Ä‘á», ngÃ y cÃ´ng bá»‘, JSONB attachment/local path/raw metadata vÃ  láº§n tháº¥y gáº§n nháº¥t.

VÃ­ dá»¥ kiá»ƒm tra nhanh trong Query Tool:

```sql
SELECT source, status, started_at, records_seen, error_message
FROM collector_runs ORDER BY started_at DESC LIMIT 20;

SELECT source, ticker, title, published_at, attachment_urls, local_files
FROM financial_disclosures ORDER BY published_at DESC NULLS LAST LIMIT 50;
```

## 3. Build vÃ  khá»Ÿi Ä‘á»™ng báº±ng tay

Má»Ÿ PowerShell táº¡i thÆ° má»¥c gá»‘c repo. CÃ¡c lá»‡nh sau chá»‰ build image vÃ  Ä‘á»‹nh nghÄ©a cÃ¡ch cháº¡y; khÃ´ng tá»± khá»Ÿi Ä‘á»™ng dá»‹ch vá»¥ lÃºc mÃ¡y báº­t. TrÆ°á»›c tiÃªn cáº¥u hÃ¬nh PostgreSQL á»Ÿ má»¥c 2 vÃ  táº¡o `airflow/.env`.

```powershell
New-Item -ItemType Directory -Force data\landing, data\airflow-logs | Out-Null
Copy-Item airflow\.env.example airflow\.env  # chá»‰ lÃ m láº§n Ä‘áº§u, rá»“i sá»­a máº­t kháº©u
docker build -t financial-airflow:local .\airflow
```

Cháº¡y Airflow standalone (web UI á»Ÿ `http://localhost:8080`):

```powershell
docker run --name financial-airflow --env-file airflow\.env -p 8080:8080 `
  --mount "type=bind,source=$($PWD.Path)\airflow\dags,target=/opt/airflow/dags,readonly" `
  --mount "type=bind,source=$($PWD.Path)\airflow\sql,target=/opt/airflow/sql,readonly" `
  --mount "type=bind,source=$($PWD.Path)\data\landing,target=/opt/airflow/data/landing" `
  --mount "type=bind,source=$($PWD.Path)\data\airflow-logs,target=/opt/airflow/logs" `
  financial-airflow:local airflow standalone
```

Láº§n Ä‘áº§u, láº¥y máº­t kháº©u UI báº±ng `docker logs financial-airflow`; Airflow táº¡o user `admin`. VÃ o UI, báº­t DAG `collect_financial_disclosures` rá»“i chá»n **Trigger DAG**. DAG cÅ©ng Ä‘Æ°á»£c láº­p lá»‹ch 07:00 háº±ng ngÃ y theo giá» Viá»‡t Nam. Äá»ƒ dá»«ng: `docker stop financial-airflow`; cháº¡y láº¡i: `docker start -ai financial-airflow`. XÃ³a container (khÃ´ng xÃ³a dá»¯ liá»‡u Ä‘Ã£ bind mount): `docker rm -f financial-airflow`.

Náº¿u muá»‘n kiá»ƒm tra crawler mÃ  chÆ°a cháº¡y Airflow/PostgreSQL:

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path)\data\landing,target=/opt/airflow/data/landing" `
  financial-airflow:local python /opt/airflow/scripts/crawl_to_local.py `
  --output-dir /opt/airflow/data/landing --source both --lookback-days 90
```

CÃ³ thá»ƒ thay `both` báº±ng `hnx` hoáº·c `ssc`. Manifest JSON sáº½ ghi sá»‘ thÃ´ng bÃ¡o, metadata, file Ä‘Ã£ táº£i vÃ  lá»—i tá»«ng nguá»“n.

## 4. Cáº¥u hÃ¬nh vÃ  giá»›i háº¡n crawl

- `COLLECTOR_LOOKBACK_DAYS`: cá»­a sá»• lá»c ngÃ y sau khi láº¥y listing (máº·c Ä‘á»‹nh 7 ngÃ y; script local máº·c Ä‘á»‹nh 14).
- `COLLECTOR_HTTP_TIMEOUT_SECONDS`: timeout má»—i request (máº·c Ä‘á»‹nh 30 giÃ¢y).
- `COLLECTOR_LOCAL_DATA_DIR`: nÆ¡i lÆ°u file trong container, máº·c Ä‘á»‹nh `/opt/airflow/data/landing`.
- `HNX_DISCLOSURE_URL`, `SSC_DISCLOSURE_URL`: ghi Ä‘Ã¨ URL listing khi nguá»“n thay Ä‘á»•i.

Crawler dÃ¹ng HTML server-rendered, khÃ´ng giáº£ láº­p Ä‘Äƒng nháº­p vÃ  khÃ´ng vÆ°á»£t CAPTCHA. NÃ³ theo link phÃ¢n trang HTML `rel=next`; paging dá»±a trÃªn JavaScript/form cáº§n adapter riÃªng. Link file thÆ°á»ng Ä‘Æ°á»£c phÃ¡t hiá»‡n tá»« URL Ä‘uÃ´i PDF/Excel/Word/ZIP hoáº·c nhÃ£n táº£i file. Náº¿u cá»•ng chá»‰ phÃ¡t sá»± kiá»‡n JavaScript hoáº·c gá»i API ná»™i bá»™, listing váº«n cÃ³ thá»ƒ tháº¥y nhÆ°ng attachment chÆ°a táº£i Ä‘Æ°á»£c. Theo láº§n quan sÃ¡t trÆ°á»›c Ä‘Ã³, SSC cÃ³ thá»ƒ Ä‘Æ°a ra nÃºt JavaScript thay cho URL file; HNX cÃ³ thá»ƒ lá»—i chá»©ng thÆ° TLS trong mÃ´i trÆ°á»ng container. TLS verification Ä‘ang báº­t cÃ³ chá»§ Ä‘Ã­ch. Khi crawl tháº¥t báº¡i, xem log task trong Airflow vÃ  `error_message`/manifest; khÃ´ng coi trang rá»—ng lÃ  thÃ nh cÃ´ng.

## 5. CÃ¡ch Ã¡p dá»¥ng láº¡i phÆ°Æ¡ng phÃ¡p

1. TÃ¬m URL listing cÃ´ng khai vÃ  xem HTML/Network trong trÃ¬nh duyá»‡t Ä‘á»ƒ biáº¿t ná»™i dung server tráº£ vá», phÃ¢n trang vÃ  URL file.
2. Add recognition rules for titles, dates, and ticker symbols; keep the listing text in raw_metadata for source review.
3. ThÃªm quy táº¯c nháº­n diá»‡n tiÃªu Ä‘á»/ngÃ y/mÃ£ cá»• phiáº¿u; giá»¯ nguyÃªn Ä‘oáº¡n listing trong `raw_metadata` Ä‘á»ƒ Ä‘á»‘i soÃ¡t nguá»“n.
4. Kiá»ƒm tra file táº£i báº±ng status HTTP, content type, kÃ­ch thÆ°á»›c, hash; khÃ´ng lÆ°u pháº£n há»“i HTML giáº£ dáº¡ng file.
5. Upsert báº±ng fingerprint á»•n Ä‘á»‹nh Ä‘á»ƒ rerun/backfill khÃ´ng táº¡o báº£n ghi trÃ¹ng; ghi cáº£ lá»—i nguá»“n vÃ  lá»—i tá»«ng attachment.
6. Khi website chuyá»ƒn sang JavaScript/API, viáº¿t adapter theo nguá»“n vÃ  chá»‰ dÃ¹ng endpoint cÃ´ng khai Ä‘Æ°á»£c phÃ©p; khÃ´ng hardcode cookie/token phiÃªn.

Khi á»•n Ä‘á»‹nh, cÃ³ thá»ƒ thay bind mount landing báº±ng MinIO object storage; giá»¯ metadata/run tracking trong PostgreSQL vÃ  downstream Spark Ä‘á»c tá»« object storage.
