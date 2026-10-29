# Run the taxi project on your own computer, without Docker or S3

This guide assumes you are new to the tools. Follow the commands in order. The **local mode** added to this repository runs on Windows, macOS, or Linux with Python 3.11. It uses files on your computer, not a virtual machine, Docker, an AWS account, or an S3 server.

The first run still needs an internet connection: the taxi trips come from the City of Chicago's public API. The default local run downloads at most **20,000 trips per month** for January, February, and March 2023. This smaller sample makes it easier to check that everything works.

## 1. Why your first clone showed no data

`git clone` copies the **program**, not the taxi trips. The data is downloaded after the program starts. There is no prefilled database in the repository.

In the original Docker setup, these pieces have different jobs:

| Piece | What it does | Where the data lives |
|---|---|---|
| **Local S3 compatible store (SeaweedFS)** | Accepts files through the S3 protocol. It is the project's data lake. It is local to your computer, not AWS S3. | A Docker volume called `s3-data`. |
| **Airflow** | Starts the download and processing tasks in order. | Its task history is in Postgres; task output goes to the data lake. |
| **Spark** | Cleans taxi trips and creates summary tables. | Reads and writes files in the data lake. |
| **DuckDB** | Reads the final summary files so the dashboard can answer questions. | In the Docker setup it reads Parquet from the S3 compatible store; it does not fill the store. |
| **Dashboard/API** | Shows charts and JSON results. | Reads what Spark has already produced. |

The flow is:

```text
Chicago API -> Airflow download -> S3 compatible store (raw CSV)
                              -> Spark (clean Parquet and summary Parquet)
                              -> DuckDB -> dashboard
```

If you only opened the dashboard after cloning, it had nothing to read. If Docker Desktop was stopped, the whole stack could not start. If Airflow had not finished the `silver_to_gold` task, DuckDB had no summary files to open. An empty dashboard **does not by itself prove** that S3 or DuckDB is broken.

On the machine where we investigated this, Docker Desktop was initially stopped and `.env` was missing. After starting Docker and running the pipeline, the S3 compatible store worked. The Chicago API once sent an incomplete response; the downloader now retries that response. The completed run produced all four summary tables. Your earlier attempt may have had a different failure, so use the checks below if you try the Docker version again.

Two addresses that often confuse beginners:

- `http://s3:8333` works **inside** the Docker network. The name `s3` is a Docker service name. A program started directly on your computer cannot normally use that address.
- `http://localhost:8333` is the S3 service's address **from your computer**, when Docker is running. It is still an S3 service, just hosted locally.

## 2. What the no-Docker local mode changes

The local path is deliberately simpler:

```text
Chicago API -> Python script -> data/local/bronze/ (raw CSV)
                           -> DuckDB -> data/local/silver/ (clean Parquet)
                                     -> data/local/gold/ (summary Parquet)
                                     -> dashboard
```

**Bronze** means the raw downloaded files. **Silver** means cleaned trip records and a quality report. **Gold** means small summary tables used by the dashboard. **Parquet** is a compact file format for tables; think of it as an efficient alternative to CSV. DuckDB can read and write these files directly.

This mode uses `scripts/run_local.py` to download and process the data, and the existing dashboard to display it. It does not run Airflow or Spark. The source, main quality checks, table names, and dashboard are the same; some aggregate details, such as the percentile calculation, can differ slightly from Spark. It is a way to run and learn the project on one computer, not a demonstration of the original distributed setup.

## 3. Install the one required program

Install **Python 3.11**. For Windows or macOS, the [Python 3.11.9 release page](https://www.python.org/downloads/release/python-3119/) has installers for both systems. Choose the Windows 64-bit installer or the macOS universal2 installer. On Windows, tick **“Add python.exe to PATH”** in the installer. On Linux, install Python 3.11 and its `venv` package from your distribution's package manager. You do not need Java, Docker Desktop, WSL, or AWS credentials for this mode.

Open a new terminal after installing Python:

| System | Terminal | Check Python |
|---|---|---|
| Windows | PowerShell | `py -3.11 --version` |
| macOS or Linux | Terminal | `python3.11 --version` |

You should see `Python 3.11.x`. If the command is unknown, close and reopen the terminal. On macOS or Linux, your Python installer may expose `python3` instead of `python3.11`; use `python3` in the commands below if `python3 --version` shows 3.11.

## 4. Open the project folder

If you have already cloned the repository, open your terminal **in that folder**. Otherwise, install Git, then run:

```text
git clone https://github.com/omarajraoui/artefact-taxi.git
cd artefact-taxi
```

You need the **updated project folder** that contains `requirements-local.txt` and `scripts/run_local.py`. The local mode was added as part of this work; a fresh clone from GitHub will only include it **after these changes are committed and pushed**. Until then, use this updated working folder or copy it to your other computer. Running `git pull` alone cannot fetch changes that have not been published.

All commands below assume your terminal is in the `artefact-taxi` folder. You can check with `pwd` on macOS/Linux or `Get-Location` in PowerShell.

## 5. Create a private Python environment and install dependencies

A **virtual environment** here means a private folder for this project's Python packages. It is **not a virtual machine**. It does not run another operating system. It keeps this project's packages separate from other Python projects.

### Windows PowerShell

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-local.txt
```

### macOS or Linux

```bash
python3.11 -m venv .venv
./.venv/bin/python -m pip install -r requirements-local.txt
```

This installs DuckDB, FastAPI, and Uvicorn. The install needs internet once. There is no need to activate the environment: the commands call its Python directly.

## 6. Download a sample and build the tables

### Windows PowerShell

```powershell
.\.venv\Scripts\python.exe scripts\run_local.py
```

### macOS or Linux

```bash
./.venv/bin/python scripts/run_local.py
```

Leave this terminal open until it says `Done`. The script downloads up to 20,000 trips for each of three months, checks the rows, and writes four gold tables. You should see messages like `2023-01: 20,000 rows downloaded`, `Quality: ...`, and `Gold: daily_kpis (...)`. The actual number of clean rows can be lower because invalid trips are rejected.

The files appear under `data/local/`:

```text
data/local/
  bronze/taxi_trips/month=2023-01/page=00001.csv
  silver/taxi_trips/data.parquet
  silver/_quality/latest.json
  gold/daily_kpis/data.parquet
  gold/top_pickup_zones/data.parquet
  gold/hourly_demand/data.parquet
  gold/payment_mix/data.parquet
```

The script reuses completed monthly CSV downloads on the next run. You can safely rerun it if the first attempt stops.

## 7. Start the dashboard

Open **a second terminal** in the same project folder. Tell the API to read local files, then start it:

### Windows PowerShell

```powershell
$env:STORAGE_BACKEND = "local"
$env:DATA_DIR = "data/local"
.\.venv\Scripts\python.exe -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

### macOS or Linux

```bash
export STORAGE_BACKEND=local
export DATA_DIR=data/local
./.venv/bin/python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Open these addresses in your browser:

| Address | What to expect |
|---|---|
| [http://127.0.0.1:8000](http://127.0.0.1:8000) | Dashboard with charts. |
| [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health) | `"ready": true` and four gold table names. |
| [http://127.0.0.1:8000/tables](http://127.0.0.1:8000/tables) | Row counts for the four tables. |
| [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) | API page where you can try queries. |

Press **Ctrl+C** in the dashboard terminal to stop the web server. Your data files remain in `data/local/`. Next time, you only need the commands in this section to see the same data again.

## 8. Change the amount of data

The default is a small sample. To request up to **100,000 rows per month**, run:

```text
Windows:        .\.venv\Scripts\python.exe scripts\run_local.py --rows-per-month 100000
macOS/Linux:    ./.venv/bin/python scripts/run_local.py --rows-per-month 100000
```

This downloads more CSV files and rebuilds the local tables. To request the complete default quarter, use `--rows-per-month 2000000`. That can take much longer and needs more free memory and disk space. The local script holds intermediate tables in DuckDB while it runs, so start small and increase only after the sample works.

For a different period, add dates, for example `--start 2023-01-01 --end 2023-02-01`. The end date is **excluded**. Run the script again before refreshing the dashboard. If the dashboard is already open, refresh the browser after the script completes.

To start fresh without permanently deleting the existing files, stop the dashboard and rename `data/local` to `data/local-backup`, then run the script again. You can also pass `--data-dir` to keep separate experiments in separate folders; set the API's `DATA_DIR` to that same folder.

## 9. If something still does not work

| What you see | Likely reason | What to do |
|---|---|---|
| `Python 3.11` command not found | Python is not installed, or the terminal has not picked up the new PATH. | Install Python 3.11, then open a new terminal and repeat section 3. |
| `No module named duckdb` | You used a different Python from the private environment. | Use the exact `.venv` Python command from sections 6 and 7; repeat the install command in section 5. |
| Windows reports a missing DLL when importing DuckDB | The Microsoft Visual C++ runtime may be absent. | Install the [Microsoft Visual C++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist) and repeat the command. [DuckDB documents this Windows requirement](https://duckdb.org/docs/installation?version=stable). |
| `Chicago API did not respond` | Internet, DNS, firewall, or temporary API failure. | Open a [one-row source check](https://data.cityofchicago.org/resource/wrvz-psew.json?%24limit=1) in a browser; restore access and rerun the script. It retries temporary failures automatically. |
| `Quality check failed` | Too many downloaded rows failed the rules. | Read `data/local/silver/_quality/latest.json` for counts and try a different date range. |
| `Address already in use` on port 8000 | Another app, possibly the Docker version of this project, uses the port. | Stop that app, or change `--port 8000` to `--port 8001` and visit port 8001 in the browser. |
| `/health` says `"ready": false` | The local script has not finished, or the API points to another folder. | Finish section 6 and check `STORAGE_BACKEND=local` and `DATA_DIR=data/local` in the **same terminal** that starts the API. |
| Dashboard opens but charts are empty | The API has no gold data to read. | Visit `/health` and `/tables`; run the script again and wait for `Done`. |

### If you choose the original Docker version later

The Docker version is useful when you specifically want to learn Airflow, Spark, and S3. It needs Docker Desktop (or Docker Engine) and therefore does **not** meet the no-VM requirement on Windows. Its checks are:

```text
docker compose ps
docker compose exec airflow-scheduler airflow dags list-runs -d taxi_pipeline
http://localhost:8000/health
```

The Airflow run must say `success`; `/health` must say `"ready": true`. If a task fails, open its log in [Airflow](http://localhost:8080) to see the actual cause. Do not run `docker compose down -v` if you want to keep its downloaded data: `-v` deletes the data volume.
