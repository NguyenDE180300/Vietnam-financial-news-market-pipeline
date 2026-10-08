# Yahoo Finance Feature Engineering

Danh sách feature có thể tạo chỉ từ dữ liệu Yahoo Finance để phân tích biến
động giá và kết hợp với sự kiện tin tức.

## 1. Dữ liệu gốc

```text
Date, Open, High, Low, Close, Adj Close, Volume, Dividends, Stock Splits
```

Nên dùng `Adj Close` cho historical return vì đã phản ánh dividend và stock
split. OHLC gốc vẫn cần cho đặc trưng trong phiên và candlestick.

## 2. Price và return

| Feature | Công thức |
|---|---|
| `price_change` | `Close - Close.shift(1)` |
| `return_1d` | `AdjClose / AdjClose.shift(1) - 1` |
| `log_return_1d` | `log(AdjClose / AdjClose.shift(1))` |
| `overnight_return` | `Open / Close.shift(1) - 1` |
| `intraday_return` | `Close / Open - 1` |
| `high_low_range` | `(High - Low) / Close.shift(1)` |
| `gap_pct` | `(Open - Close.shift(1)) / Close.shift(1)` |
| `close_position` | `(Close - Low) / (High - Low)` |

```python
for window in [2, 3, 5, 10, 20, 60]:
    df[f"return_{window}d"] = df["Adj Close"].pct_change(window)
```

## 3. Trend và momentum

```python
for window in [5, 10, 20, 50, 100, 200]:
    df[f"sma_{window}"] = df["Adj Close"].rolling(window).mean()

df["price_to_sma_5"] = df["Adj Close"] / df["sma_5"] - 1
df["price_to_sma_20"] = df["Adj Close"] / df["sma_20"] - 1
df["price_to_sma_50"] = df["Adj Close"] / df["sma_50"] - 1
df["sma_5_to_20"] = df["sma_5"] / df["sma_20"] - 1

ema_12 = df["Adj Close"].ewm(span=12, adjust=False).mean()
ema_26 = df["Adj Close"].ewm(span=26, adjust=False).mean()
df["macd"] = ema_12 - ema_26
df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
df["macd_histogram"] = df["macd"] - df["macd_signal"]
df["macd_pct"] = df["macd"] / df["Adj Close"]
```

Các tỷ lệ như `price_to_sma_20` phù hợp hơn mức SMA tuyệt đối khi mô hình dùng
chung cho nhiều ticker.

## 4. Volatility và ATR

```python
returns = df["Adj Close"].pct_change()
for window in [5, 10, 20, 60]:
    df[f"volatility_{window}d"] = returns.rolling(window).std()

df["volatility_ratio_5_20"] = df["volatility_5d"] / df["volatility_20d"]

previous_close = df["Close"].shift(1)
true_range = pd.concat([
    df["High"] - df["Low"],
    (df["High"] - previous_close).abs(),
    (df["Low"] - previous_close).abs(),
], axis=1).max(axis=1)
df["atr_14"] = true_range.rolling(14).mean()
df["atr_14_pct"] = df["atr_14"] / df["Close"]
```

## 5. RSI và Bollinger Bands

```python
delta = df["Adj Close"].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)
rs = gain.rolling(14).mean() / loss.rolling(14).mean()
df["rsi_14"] = 100 - 100 / (1 + rs)
df["rsi_overbought"] = (df["rsi_14"] > 70).astype(int)
df["rsi_oversold"] = (df["rsi_14"] < 30).astype(int)

middle = df["Adj Close"].rolling(20).mean()
std = df["Adj Close"].rolling(20).std()
upper = middle + 2 * std
lower = middle - 2 * std
df["bollinger_width"] = (upper - lower) / middle
df["bollinger_position"] = (df["Adj Close"] - lower) / (upper - lower)
```

## 6. Volume và thanh khoản

```python
df["volume_change_1d"] = df["Volume"].pct_change()
df["volume_ma_5"] = df["Volume"].rolling(5).mean()
df["volume_ma_20"] = df["Volume"].rolling(20).mean()
df["volume_ratio_5"] = df["Volume"] / df["volume_ma_5"]
df["volume_ratio_20"] = df["Volume"] / df["volume_ma_20"]

volume_mean = df["Volume"].rolling(20).mean()
volume_std = df["Volume"].rolling(20).std()
df["volume_zscore_20"] = (df["Volume"] - volume_mean) / volume_std

df["trading_value"] = df["Close"] * df["Volume"]
df["trading_value_ratio_20"] = (
    df["trading_value"] / df["trading_value"].rolling(20).mean()
)
df["amihud_illiquidity"] = (
    df["return_1d"].abs() / df["trading_value"].replace(0, np.nan)
)
```

`Close × Volume` chỉ là proxy, không phải tổng giá trị khớp lệnh thực tế.

## 7. Candlestick

```python
price_range = (df["High"] - df["Low"]).replace(0, np.nan)
df["body_size_pct"] = (df["Close"] - df["Open"]).abs() / df["Open"]
df["upper_shadow_pct"] = (
    df["High"] - df[["Open", "Close"]].max(axis=1)
) / df["Open"]
df["lower_shadow_pct"] = (
    df[["Open", "Close"]].min(axis=1) - df["Low"]
) / df["Open"]
df["body_to_range"] = (df["Close"] - df["Open"]).abs() / price_range
df["is_bullish"] = (df["Close"] > df["Open"]).astype(int)
df["is_bearish"] = (df["Close"] < df["Open"]).astype(int)
df["is_doji"] = (df["body_to_range"] < 0.1).astype(int)
```

## 8. Rolling high, low và drawdown

```python
for window in [5, 20, 60, 252]:
    rolling_high = df["High"].rolling(window).max()
    rolling_low = df["Low"].rolling(window).min()
    df[f"distance_to_high_{window}d"] = df["Close"] / rolling_high - 1
    df[f"distance_to_low_{window}d"] = df["Close"] / rolling_low - 1

peak_60d = df["Adj Close"].rolling(60).max()
df["drawdown_60d"] = df["Adj Close"] / peak_60d - 1
```

## 9. On-Balance Volume

```python
direction = np.sign(df["Close"].diff()).fillna(0)
df["obv"] = (direction * df["Volume"]).cumsum()
df["obv_change_5d"] = df["obv"].diff(5)
df["obv_slope_20d"] = df["obv"].diff(20) / 20
```

## 10. Corporate actions

```python
df["has_dividend"] = (df["Dividends"] > 0).astype(int)
df["dividend_amount"] = df["Dividends"]
df["dividend_yield_proxy"] = df["Dividends"] / df["Close"].shift(1)
df["has_stock_split"] = (df["Stock Splits"] > 0).astype(int)
df["stock_split_ratio"] = df["Stock Splits"]
```

Nhóm này giúp tránh hiểu nhầm biến động do corporate action thành phản ứng thị
trường thông thường.

## 11. Calendar từ Date

```python
df["day_of_week"] = df.index.dayofweek
df["month"] = df.index.month
df["quarter"] = df.index.quarter
df["is_monday"] = (df.index.dayofweek == 0).astype(int)
df["is_friday"] = (df.index.dayofweek == 4).astype(int)
df["is_month_start"] = df.index.is_month_start.astype(int)
df["is_month_end"] = df.index.is_month_end.astype(int)
```

## 12. Benchmark cũng lấy từ Yahoo Finance

Nếu tải thêm VN-Index hoặc benchmark khác từ Yahoo, có thể tạo:

```text
market_return_1d
market_return_5d
excess_return_1d
excess_return_5d
rolling_beta_20d
rolling_beta_60d
correlation_with_market_20d
abnormal_return
cumulative_abnormal_return
```

```python
df["excess_return_1d"] = df["return_1d"] - df["market_return_1d"]
```

## 13. Bộ baseline đề xuất

```text
return_1d, return_3d, return_5d, return_10d, return_20d
overnight_return, intraday_return, high_low_range, gap_pct, close_position
price_to_sma_5, price_to_sma_20, price_to_sma_50, sma_5_to_20
macd_pct, rsi_14
volatility_5d, volatility_20d, volatility_ratio_5_20, atr_14_pct
bollinger_width, bollinger_position
volume_ratio_5, volume_ratio_20, volume_zscore_20
body_to_range, drawdown_60d
```

Nếu benchmark cũng lấy từ Yahoo, bổ sung:

```text
market_return_1d, market_return_5d, excess_return_1d, rolling_beta_60d
```

## 14. Quy tắc chống data leakage

- Feature tại ngày `T` chỉ được dùng nếu đã biết tại thời điểm dự đoán.
- Tin xuất hiện trước khi phiên `T` đóng cửa không được dùng OHLCV hoàn chỉnh
  của phiên `T`; nên chốt feature tại `T-1`.
- Với tin sau đóng cửa, dữ liệu phiên `T` có thể được xem là đã biết.
- Không `backfill` rolling feature bằng dữ liệu tương lai.
- Chia train/validation/test theo thời gian thay vì random toàn bộ dữ liệu.
- Fit scaler, winsorization và imputation chỉ trên tập train.
- Ưu tiên return, ratio, percentage và z-score thay cho giá tuyệt đối.

## 15. Target tính từ Yahoo Finance

```text
forward_return_1d
forward_return_3d
forward_return_5d
forward_excess_return_1d
forward_excess_return_3d
forward_excess_return_5d
direction_t_plus_1
direction_t_plus_3
direction_t_plus_5
volume_spike_t_plus_1
volatility_spike_t_plus_5
```

Target tương lai chỉ được dùng làm nhãn, tuyệt đối không đưa ngược vào feature
đầu vào của mô hình.
