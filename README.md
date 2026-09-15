# Working Student Data Engineer Challenge

## Overview

Welcome to Terra One's Student Data Engineer coding challenge! It is designed to assess your data engineering skills through debugging a real data pipeline that reflects actual tasks you'll encounter working with energy market data.

 ## The scenario

A teammate needed the intraday energy-bid feed running, so they wrote a quick
Airflow pipeline that reads a raw bid export and produces an hourly market
summary:

`ingest_and_clean → aggregate_hourly`

It runs without errors and writes a summary. Your job is to work out whether the
output can be trusted, and make it so. These numbers would feed a trading
decision, so "it ran" is not good enough.

- Pipeline: `dags/energy_pipeline.py`
- Raw feed: `dags/energy_data.csv`

Use AI tools if you want.  This challenge is not
about writing code; it is about catching where code is quietly
wrong, and knowing what actually matters.

## Deliverables

**1. `FINDINGS.md`** — what's wrong, ranked by impact.
For each issue: what it is and where it is, why it affects the output, and how
you confirmed it. The ranking and your reasoning matter more than a long list.
Note anything you are unsure about.

**2. Your fixes.**
Fix the issues that matter. You do not have to fix everything, and what you
choose to leave (and why) tells us as much as what you change. Note those
decisions in `FINDINGS.md`.

**3. One open question.**
Was there an unusual price event during this period? There is no single right
answer. Define what you mean by "unusual", say what evidence convinced you, and
how confident you are. A paragraph or two is enough.

We read for whether you catch the things that actually change the numbers, how
you prioritise, and how clearly you reason.

## Setup

Requires Docker Desktop (~4GB RAM free).

```bash
docker compose up airflow-init   # one-time: init DB + admin user
docker compose up -d             # start the services
```

Open [http://localhost:8080](http://localhost:8080) (user `airflow`, password `airflow`), then trigger
the `energy_pipeline` DAG. Output lands in `output/`; task logs are in the UI.
Tear down with `docker compose down -v`.

To iterate outside Docker, the task functions in `dags/energy_pipeline.py` are
plain Python you can import and run after `pip install -r requirements.txt`.

## What happens next

Aim for about 90 minutes. Commit your work, including `FINDINGS.md`, and send us the repo. We'll then go through your solution together in the next round.