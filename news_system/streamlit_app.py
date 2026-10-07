from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

if __package__ in (None, ""):
    # Also support launching this file directly with the environment's Python.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from news_system.database import NewsDatabase
    from news_system.pipeline import collect_news
    from news_system.rss_collector import DEFAULT_FEEDS
else:
    from .database import NewsDatabase
    from .pipeline import collect_news
    from .rss_collector import DEFAULT_FEEDS


st.set_page_config(page_title="Vietnam News Monitor", page_icon="📰", layout="wide")
st.title("Vietnam News Monitor")
st.caption("RSS → bài viết → SQLite | Bản UI thử nghiệm")

db_path = st.sidebar.text_input("SQLite database", "news_system.db")
limit = st.sidebar.slider("Số bài hiển thị", 10, 200, 50, 10)
source_filter = st.sidebar.multiselect("Nguồn", list(DEFAULT_FEEDS), default=list(DEFAULT_FEEDS))
ticker_filter = st.sidebar.text_input("Lọc mã cổ phiếu", placeholder="Ví dụ: HPG")
gold_path = st.sidebar.text_input(
    "Gold Parquet local", "data_lake/gold/news_market_impact"
)

database = NewsDatabase(db_path)

with st.sidebar.expander("Thu thập dữ liệu", expanded=True):
    full_text = st.checkbox("Thử crawl toàn văn bài", value=True)
    article_limit = st.number_input("Giới hạn bài crawl mỗi lượt", min_value=1, max_value=30, value=5)
    if st.button("Lấy RSS ngay", type="primary", use_container_width=True):
        progress = st.status("Đang đọc RSS và lưu dữ liệu…", expanded=True)
        try:
            with st.spinner("Đang thu thập; crawl bài có thể mất thêm thời gian"):
                inserted = collect_news(
                    database,
                    full_text=full_text,
                    article_limit=int(article_limit) if full_text else 0,
                )
            progress.update(label=f"Hoàn tất — thêm {inserted} bài mới", state="complete")
            st.rerun()
        except Exception as error:
            progress.update(label=f"Thu thập thất bại: {error}", state="error")

stats = database.stats()
left, middle, right = st.columns(3)
left.metric("Tổng bài đã lưu", stats["total"])
middle.metric("Nguồn đang bật", len(source_filter))
with sqlite3.connect(database.path) as connection:
    enriched = connection.execute("SELECT COUNT(*) FROM news_items WHERE content IS NOT NULL").fetchone()[0]
right.metric("Bài có toàn văn", enriched)

query = """SELECT id,title,summary,source,url,published_at,category,tickers_json,
                  content,author,image_url
           FROM news_items"""
conditions: list[str] = []
params: list[object] = []
if source_filter:
    conditions.append("source IN (" + ",".join("?" for _ in source_filter) + ")")
    params.extend(source_filter)
if ticker_filter.strip():
    conditions.append("EXISTS (SELECT 1 FROM json_each(news_items.tickers_json) WHERE value=?)")
    params.append(ticker_filter.strip().upper())
if conditions:
    query += " WHERE " + " AND ".join(conditions)
query += " ORDER BY COALESCE(published_at, '') DESC, id DESC LIMIT ?"
params.append(limit)
with sqlite3.connect(database.path) as connection:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(query, params).fetchall()

st.subheader("Tin mới")
if not rows:
    st.info("Chưa có bài phù hợp. Bấm 'Lấy RSS ngay' để thu thập.")
else:
    table = pd.DataFrame([
        {
            "Ngày đăng": row["published_at"],
            "Nguồn": row["source"],
            "Danh mục": row["category"],
            "Ticker": ", ".join(json.loads(row["tickers_json"] or "[]")),
            "Tiêu đề": row["title"],
            "Có toàn văn": bool(row["content"]),
        }
        for row in rows
    ])
    st.dataframe(table, use_container_width=True, hide_index=True)

    for row in rows:
        label = f"{row['title']} — {row['source']} — {row['published_at'] or 'chưa rõ ngày'}"
        with st.expander(label):
            if row["image_url"]:
                st.image(row["image_url"], caption="Ảnh từ trang bài viết")
            st.write(f"**Nguồn:** {row['source']} · **Danh mục:** {row['category']}")
            tickers = json.loads(row["tickers_json"] or "[]")
            st.write(f"**Ticker:** {', '.join(tickers) if tickers else 'Chưa nhận diện'}")
            if row["author"]:
                st.write(f"**Tác giả:** {row['author']}")
            st.write(row["content"] or row["summary"] or "RSS không có mô tả; bài chưa được crawl toàn văn.")
            if row["url"]:
                st.link_button("Mở bài gốc", row["url"])

st.divider()
st.subheader("Spark Gold: News → Market event window")
gold_directory = Path(gold_path)
if not gold_directory.exists():
    st.info("Chưa thấy Gold local. Chạy Spark job news_market_gold trước.")
else:
    try:
        gold_frame = pd.read_parquet(gold_directory)
    except Exception as error:
        st.error(f"Không đọc được Gold Parquet: {error}")
        gold_frame = pd.DataFrame()
    if not gold_frame.empty:
        available_gold = gold_frame[gold_frame["has_market_data"] == True].copy()  # noqa: E712
        event_count = gold_frame[["event_id", "ticker"]].drop_duplicates().shape[0]
        g1, g2, g3 = st.columns(3)
        g1.metric("Cặp news–ticker", event_count)
        g2.metric("Event-window rows", len(gold_frame))
        g3.metric("Đã có market data", len(available_gold))

        gold_tickers = sorted(gold_frame["ticker"].dropna().unique().tolist())
        selected_gold_ticker = st.selectbox("Ticker Gold", gold_tickers)
        ticker_gold = gold_frame[gold_frame["ticker"] == selected_gold_ticker].copy()
        event_options = (
            ticker_gold[["event_id", "title"]].drop_duplicates().set_index("event_id")["title"].to_dict()
        )
        selected_event = st.selectbox(
            "Bài news", list(event_options), format_func=lambda event_id: event_options[event_id]
        )
        event_window = ticker_gold[
            (ticker_gold["event_id"] == selected_event) & ticker_gold["has_market_data"]
        ].sort_values("relative_session")
        if event_window.empty:
            st.warning("Các phiên của bài này chưa xảy ra hoặc chưa có trong Market Silver.")
        else:
            chart = event_window.set_index("relative_session")[[
                "stock_cumulative_return", "benchmark_cumulative_return", "abnormal_return"
            ]] * 100
            chart.index = ["T0" if value == 0 else f"T{value:+d}" for value in chart.index]
            st.line_chart(chart, x_label="Phiên tương đối", y_label="Lợi suất (%)")
            display_gold = event_window[[
                "relative_session", "session_date", "close", "daily_return",
                "stock_cumulative_return", "benchmark_cumulative_return", "abnormal_return",
            ]].copy()
            for column in (
                "daily_return", "stock_cumulative_return",
                "benchmark_cumulative_return", "abnormal_return",
            ):
                display_gold[column] = (display_gold[column] * 100).round(2)
            st.dataframe(display_gold, use_container_width=True, hide_index=True)
            st.caption(
                "Abnormal return dùng benchmark equal-weight VN30. Đây là quan hệ trong "
                "event window, không tự động chứng minh quan hệ nhân quả."
            )

st.divider()
st.subheader("News → Market impact")
with sqlite3.connect(database.path) as connection:
    impact_table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='news_market_impacts'"
    ).fetchone()
if not impact_table_exists:
    st.info("Chưa có bảng impact. Chạy module news_system.news_market_impact trước.")
else:
    horizon = st.selectbox("Cửa sổ sau tin", [1, 3, 5], format_func=lambda value: f"T+{value} phiên")
    impact_query = """SELECT i.news_id,i.ticker,i.impact_label,i.raw_return,
                             i.benchmark_return,i.abnormal_return,i.anchor_date,i.target_date,
                             i.join_quality,n.title,n.source,n.url
                      FROM news_market_impacts i
                      JOIN news_items n ON n.id=i.news_id
                      WHERE i.horizon_sessions=?
                      ORDER BY ABS(COALESCE(i.abnormal_return,i.raw_return)) DESC"""
    with sqlite3.connect(database.path) as connection:
        impact_frame = pd.read_sql_query(impact_query, connection, params=(horizon,))
    if impact_frame.empty:
        st.info("Chưa ghép được news với market data ở cửa sổ này.")
    else:
        counts = impact_frame["impact_label"].value_counts()
        positive, neutral, negative = st.columns(3)
        positive.metric("Positive", int(counts.get("positive", 0)))
        neutral.metric("Neutral", int(counts.get("neutral", 0)))
        negative.metric("Negative", int(counts.get("negative", 0)))
        display = impact_frame.head(100).copy()
        for column in ("raw_return", "benchmark_return", "abnormal_return"):
            display[column] = (display[column] * 100).round(2)
        display = display.rename(columns={
            "ticker": "Ticker", "impact_label": "Nhãn", "title": "Tiêu đề",
            "raw_return": "Return CP (%)", "benchmark_return": "VN-Index (%)",
            "abnormal_return": "Abnormal (%)", "anchor_date": "Ngày gốc",
            "target_date": "Ngày đích", "source": "Nguồn",
        })
        st.dataframe(
            display[["Ticker", "Nhãn", "Abnormal (%)", "Return CP (%)", "VN-Index (%)",
                     "Ngày gốc", "Ngày đích", "Tiêu đề", "Nguồn"]],
            use_container_width=True, hide_index=True,
        )
        st.caption(
            "Nhãn dựa trên abnormal return ±2%. Đây là tương quan trong event window, "
            "không tự động chứng minh bài báo gây ra biến động."
        )

    with st.expander("Xem từng phiên quanh T0", expanded=False):
        offsets = [-5, -3, -2, -1, 0, 1, 2, 3, 5]
        selected_offset = st.selectbox(
            "Phiên tương đối",
            offsets,
            index=4,
            format_func=lambda value: "T0" if value == 0 else f"T{value:+d}",
        )
        window_query = """SELECT w.ticker,w.impact_label,w.session_return,
                                  w.benchmark_session_return,w.abnormal_session_return,
                                  w.t0_date,w.session_date,n.title,n.source
                           FROM news_market_event_windows w
                           JOIN news_items n ON n.id=w.news_id
                           WHERE w.relative_session=?
                           ORDER BY ABS(COALESCE(w.abnormal_session_return,w.session_return)) DESC
                           LIMIT 100"""
        try:
            with sqlite3.connect(database.path) as connection:
                window_frame = pd.read_sql_query(
                    window_query, connection, params=(selected_offset,)
                )
        except Exception:
            window_frame = pd.DataFrame()
        if window_frame.empty:
            st.info("Chưa có event-window data. Chạy lại news_system.news_market_impact.")
        else:
            for column in ("session_return", "benchmark_session_return", "abnormal_session_return"):
                window_frame[column] = (window_frame[column] * 100).round(2)
            window_frame = window_frame.rename(columns={
                "ticker": "Ticker", "impact_label": "Nhãn",
                "session_return": "Return CP (%)",
                "benchmark_session_return": "VN-Index (%)",
                "abnormal_session_return": "Abnormal (%)",
                "t0_date": "Ngày T0", "session_date": "Ngày phiên",
                "title": "Tiêu đề", "source": "Nguồn",
            })
            st.dataframe(window_frame, use_container_width=True, hide_index=True)

st.divider()
st.caption("Nội dung/ảnh phụ thuộc RSS và khả năng truy cập trang nguồn; cần tuân thủ điều khoản của từng báo.")
