# iex-bars

1-minute bars for about 70 US ETFs and large-cap stocks, built from IEX Exchange's free
historical TOPS files (posted T+1, around 7 AM ET).

- `data/iex/bars/YYYY-MM-DD.json.gz` holds one trading day. Columns: minute of day (ET), open, high, low, close, volume. Only IEX-executed trades are included (about 2–3% of US volume).
- `.github/workflows/iex-hist.yml` builds the previous day every morning (Tue–Sat). Run it manually with a `range` to backfill. IEX keeps about 12 months.

Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the
IEX Historical Data Terms of Use (https://www.iex.io/).
