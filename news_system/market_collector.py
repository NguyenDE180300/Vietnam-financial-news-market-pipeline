"""Optional Yahoo Finance adapter. Install yfinance to use it."""
from datetime import datetime, timezone

from .models import MarketBar


def download_daily(ticker: str, period: str = "1mo", start: str | None = None,
                   end: str | None = None) -> list[MarketBar]:
    try:
        import yfinance as yf
    except ImportError as error:
        raise RuntimeError("Install yfinance in the active Conda environment first") from error
    # Event studies must use split/dividend-adjusted OHLC; raw Close creates
    # fake news impacts around ex-dividend dates and corporate actions.
    kwargs = {"interval": "1d", "auto_adjust": True, "progress": False}
    if start or end:
        kwargs.update(start=start, end=end)
    else:
        kwargs["period"] = period
    frame = yf.download(ticker, **kwargs)
    if frame.empty:
        return []
    if hasattr(frame.columns, "levels"):
        frame.columns = frame.columns.get_level_values(0)
    bars = []
    for timestamp, row in frame.iterrows():
        time = timestamp.to_pydatetime()
        if time.tzinfo is None:
            time = time.replace(tzinfo=timezone.utc)
        bars.append(MarketBar(
            ticker=ticker.removesuffix(".VN"), bar_time=time.astimezone(timezone.utc).isoformat(),
            open=float(row["Open"]), high=float(row["High"]), low=float(row["Low"]),
            close=float(row["Close"]), volume=float(row["Volume"]), source="yahoo",
        ))
    return bars


def download_vnstock_daily(symbol: str, start: str, end: str) -> list[MarketBar]:
    """Download a Vietnamese stock or index consistently from VCI/vnstock."""
    try:
        from vnstock.api.quote import Quote
    except ImportError as error:
        raise RuntimeError("Install vnstock in the active Conda environment first") from error
    frame = Quote(symbol=symbol, source="VCI", show_log=False).history(
        start=start, end=end, interval="1D"
    )
    if frame is None or frame.empty:
        return []
    bars = []
    for _, row in frame.iterrows():
        timestamp = row["time"]
        time_value = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else datetime.fromisoformat(str(timestamp))
        if time_value.tzinfo is None:
            time_value = time_value.replace(tzinfo=timezone.utc)
        bars.append(MarketBar(
            ticker=symbol, bar_time=time_value.astimezone(timezone.utc).isoformat(),
            open=float(row["open"]), high=float(row["high"]), low=float(row["low"]),
            close=float(row["close"]), volume=float(row["volume"]), source="vnstock_vci",
        ))
    return bars


def download_vnindex(start: str, end: str) -> list[MarketBar]:
    """Backward-compatible VN-Index adapter."""
    return download_vnstock_daily("VNINDEX", start, end)
