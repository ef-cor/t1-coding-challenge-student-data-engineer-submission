## Findings

Ranked by how much each one affects the summary output.

### 1. 1,060 rows (35.6% of the dataset) were silently discarded

**Where:** the `Sell_Buy` mapping, then `dropna(subset=["side"])`

```python
df["side"] = df["Sell_Buy"].map(
    {"buy": "buy", "sell": "sell", "BUY": "buy", "SELL": "sell"}
)
```

The column contains **six** spellings, not four: `buy`, `BUY`, `Buy`, `sell`, `SELL`,
`Sell`. `Series.map()` returns `NaN` for anything not a key, and the `dropna`
later removes those rows.

**Effect:** every count, average, volume and spread in the summary was computed on
roughly two-thirds of the market. The discarded rows were not unusable data.

**Confirmed:** `value_counts()` on the raw column gives `Buy` 555 + `Sell` 505 = 1,060,
which is exactly 2,975 − 1,915.

The raw data distribution:

| Sell_Buy | Count |
| --- | --- |
| Buy | 555 |
| Sell | 505 |
| buy | 498 |
| sell | 495 |
| SELL | 467 |
| BUY | 455 |

### 2. `buy_vwap` and `sell_vwap` were not volume-weighted

**Where:** `dags/energy_pipeline.py`, `_vwap()`

```python
def _vwap(group: pd.DataFrame) -> float:
    """Volume-weighted average price for a group of bids."""
    return group["Price"].mean()
```

The docstring says volume-weighted but the actual code was just the group mean.

**Effect:** `buy_vwap` was identical to `buy_avg_price` in every row of the output,
and likewise for sell.

**Confirmed:** `analysis/baseline/run1.csv` — the two columns match exactly, 24 rows out
of 24, and directly from reading the code.

### 3. The UTC offset was ignored, putting ~20% of rows in the wrong hour

**Where:** `df["Timestamp"].astype(str).str.slice(11, 13)`

The feed carries two offsets: 2,367 rows at `+00:00` and 608 at `+01:00`.
Simply slicing the hour out of the string ignores the offset.

**Effect:** the `+01:00` rows landed in the wrong bucket and were then averaged against
`+00:00` rows. 391 of the 1,915 rows remaining after the invalid sides were dropped
were in the wrong hour.

This was really a symptom of `read_csv` leaving `Timestamp` as a string and never
parsing it as a datetime.

**Confirmed:** `Timestamp.str[-6:].value_counts()`, and comparing the sliced hour
against `pd.to_datetime(..., utc=True).dt.hour`. The offsets are real data rather than a
formatting artifact: parsed with `utc=True` the column is `is_monotonic_increasing`,
parsed naively it is not.

### 4. Three trading days were folded into 24 buckets, and the output carried no date

**Where:** `aggregate_hourly`, `df.groupby("hour")`

The hour was derived by slicing characters 11–12 out of the timestamp string, while
discarding the date.

**Effect:** the file spans 2023-12-31 23:02 UTC to 2024-01-03 23:02 UTC. Every row of
the summary blended three separate days, so "hour 14" was an average across three
different afternoons.

**Note:** Unlike the others, this one is ambiguous. "Hourly summary" could
mean an hour-of-day profile. I read it as per-day for the following reasons: the
finer grain is always recoverable (a downstream user wanting a whole-period profile can
aggregate up), and it makes the underlying trends visible — including whether an unusual
price event occurred. **This fix changes the output schema**, and it is something I would
want to confirm with the pipeline's author or verify against downstream requirements.

**Confirmed:** parsing the timestamps gives 73 distinct (date, hour) buckets against
the 24 the pipeline produced.

### 5. The summary was appended to a single file with no separator between runs

**Where:** `summary.to_csv(SUMMARY_PATH, mode="a", header=not os.path.exists(...))`

**Effect:** subsequent runs stack a complete copy underneath the existing records, so the
output grew with the number of triggers instead of being a function of the input.

**Note:** another one of those issues where I wasn't sure of the intended behaviour, so
I followed best practice.

**Confirmed:** triggered the pipeline twice with no code change in between —
`analysis/baseline/run1.csv` is 25 lines, `run2.csv` is 49.

### 6. Missing prices and volumes were imputed with the column mean

**Where:** `df["Price"].fillna(df["Price"].mean())` and the same for `Volume`

59 rows have no price and 51 have no volume — 110 rows in total (3.7% of the dataset),
with no overlap between them.

**Effect:** because the distribution is right-skewed, the mean is pulled up by the
outliers. The imputed values therefore sit on the high side and paint a misleading
picture. The mean was also computed before the invalid-side rows were dropped, so it was
drawn partly from rows that were about to be discarded.

**Confirmed:** `Price` is heavily right-skewed. Its mean is **91.24** while the median
is **61.11** and the 75th percentile is **80.89**. Mean imputation on this distribution
does not insert a typical value — it inserts one above the 75th percentile.
`Volume` behaves the same way: mean 1,439.8 against a median of 1,042.

### 7. `market_spread` uses a mean that the outliers distort

**Where:** `round(buys["Price"].mean() - sells["Price"].mean(), 2)`

**This is not a code defect**, but it is undefended against the distribution it runs on,
which makes the output misleading.

Buy prices average 103.28 against sell 78.85, so the column reports a spread around 24
and swings between **+329.02 and −171.60** across adjacent hours. The medians for the
same data are 61.31 and 60.83 — a gap of about half a unit. Computed robustly, the median
buy-minus-sell spread across all hours has a **mean of 0.98**.

I left the calculation alone and documented it rather than redefining a metric whose
intent I can't confirm.

### 8. No bounds or flags to highlight erroneous input data

The findings about missing price and volume and about the side mismatch share a root
cause: the pipeline discards data without recording it and without any ceiling. A third
of the dataset was thrown away while the DAG executed successfully.

### 9. Lower priority, noted but not fixed

- **Hardcoded absolute paths.** `/opt/airflow/...` is baked into three module constants,
  so the task functions cannot be run or unit-tested outside the container.
- **Stage 2 can read a stale Parquet.** The two tasks communicate through a file rather
  than XCom, so clearing or skipping stage 1 independently leaves stage 2 silently
  summarising a previous run's data.
- **`ingest_and_clean` combines two functions into one.** A bit against the conventional "Do One Thing" approach.



## Fixes

One commit per issue, so each can be read against the output it changed. Listed in the
order they were committed. Reference output from the unmodified pipeline is in
`analysis/baseline/`.

| Commit | Change | Before | After |
| --- | --- | --- | --- |
| `5c5a29f` | Lowercase before mapping; two keys instead of four | 1,915 rows kept | 2,865 rows kept |
| `981da2c` | Parse with `pd.to_datetime(..., utc=True)` at load | ~20% mis-bucketed | correct |
| `dafa457` | `_vwap` weights by volume: `Σ(P×V) / ΣV` | VWAP = mean in all rows | diverges in all rows |
| `daee8b2` | Drop rows missing Price or Volume | 110 values fabricated | 110 rows excluded |
| `3a4f034` | Group by `(date, hour)`, add a `date` column | 24 rows, undated | 73 rows, dated |
| `65da1bb` | Write to `hourly_summary_{ds}.csv`, no append | run 2 doubled the file | stable across runs |
| `9265b00` | Log discarded rows, bound both drop rates, guard zero-volume groups | silent, unbounded, `RuntimeWarning` | logged, capped, clean |
| `a69ef5c` | Update docstrings to match the fixed behaviour | described the original pipeline | accurate |

### How the output changed

`analysis/baseline/run1.csv` against the corrected summary:

| | Before | After |
| --- | --- | --- |
| Rows in the summary | 24 | 73 |
| `date` column | absent | present |
| Buy bids counted | 953 | 1,452 |
| Sell bids counted | 962 | 1,413 |
| Buy volume | 1,270,757 | 2,079,981 |
| Sell volume | 1,432,457 | 2,073,206 |
| Rows where `buy_vwap` = `buy_avg_price` | 24 of 24 | 0 of 73 |

Roughly a third of the market was missing from every count and volume, and both VWAP
columns were duplicates of the averages sitting next to them.

### Decisions inside those fixes

**Why drop the incomplete rows rather than impute.** Missingness appears to be MCAR: no
association with other columns. The skew in the data also makes mean imputation
unreliable. I considered other options — last observation carried forward, a local
window mean, the same hour across days — but rejected them because this is a transaction
log rather than a sampled series, and I was not convinced any of them was the right
approach here.

**Why two thresholds.** Rows missing a price are a *data* problem: the feed lost
values, some background rate is normal, and you bound it. An unrecognised `Sell_Buy`
value is a *code* problem: the row is fine and the mapping has fallen behind the
source's vocabulary. Different remedies justify different tolerances — 10% against 1%,
both chosen arbitrarily.

**Why the output file is keyed on run date.** Plain overwrite would have fixed the
idempotency defect but discards the previous run. Keying on `{ds}` from the task
context gives both: a retry or a cleared task instance rewrites its own file, and a new
run gets its own. Note that the filename records the *execution date*, not the period it
covers.



## What stood out

### Was there an unusual price event?

Honestly, no. I could not identify a single unusual hour whose price level departs materially from the rest of the
period. I noticed the occurrence of outliers, spread sporadically across the dataset, but I was not able to identify
the reason for why they were there or establish a correlation with other features (volume, side, date/time).

Individual bid prices span 20.09 to 4,883.99, and 32 records — 1.08% — lie beyond the
IQR fence at 140.55. Those are what make the naive hourly means look volatile, and they
survive into the VWAP, which reaches 592.85.


Instead of the mean, I looked at the median. My assumption was that if power were
genuinely scarce in some hour, the median bid in that hour would rise, because scarcity
should move every participant, not one record. Across all 73 hours the median buy price
never exceeds 77.96 and the median sell never exceeds 76.69.

The extreme records behave like defects rather than market events:

- **Each is isolated.** If power were worth 4,600 at 01:00 on 2 January, the other
  twenty bids in that hour would also be elevated. They are not.
- **Each is one-sided.** Twenty of the 73 buckets have a VWAP above 100. In every one,
  only the buy side or the sell side is elevated, never both.
- **They arrive at random and don't follow a demand pattern.** Nine fall in the six lowest-demand hours of the day
  and nine in the six highest.
- **They carry ordinary volumes.** Median 958 against 1,042 for the dataset, and none
  exceeds the volume outlier fence.


**There is also no demand pattern for scarcity related price fluctuations.** My starting assumption was that
prices should dip midday when solar is available. Excluding the outliers, the median
price is 60.55 overnight (00–05), 60.48 through the morning ramp, 60.20 midday and
60.27 in the evening peak (16–20). The dataset shows neither a summer solar trough nor
the winter evening peak that early January would produce.

**How confident am I.** It splits in two. On the negative claim, that no hour shows a
market-wide shift in price leve, reasonably. 
Across all 73 hours the median buy never exceeds 77.96 and the median
sell never exceeds 76.69, and genuine scarcity would have to move the median rather than
the tail.

On what the outliers actually are, not confident at all. I could show they do not behave
like market events, but nothing I tried explained them.


### Additional Remarks:

After all eight fixes, `buy_total_volume` and both VWAP columns are correct but not
robust. Thirty-nine records (1.36% of rows) carry **30.2% of all volume** in the
dataset, so those figures remain dominated by a handful of bids.
