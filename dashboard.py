import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import altair as alt
import json
from mct_media_collector import collect_media_data  # ✅ our backend helper

# ========================================
# PAGE CONFIGURATION
# ========================================
st.set_page_config(
    page_title="MCT Media Monitoring Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# CUSTOM STYLING (Professional Dark Mode)
# ========================================
st.markdown("""
<style>
.stApp {
    background: radial-gradient(circle at top left, #002933 0%, #001a1f 100%);
    color: #d6f1f0;
    font-family: 'Inter', 'Segoe UI', sans-serif;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #01343f, #001d25);
    padding: 1.5rem 1rem;
    color: #e4f0ef;
    box-shadow: 3px 0 10px rgba(0,0,0,0.4);
}
[data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
    color: #34d399 !important;
    font-weight: 700;
}
[data-testid="stSidebar"] label {
    font-size: 0.9rem;
    color: #a7f3d0 !important;
}
header, .block-container > div:first-child {
    background: linear-gradient(90deg, #001f25, #00494f);
    padding: 0.8rem 1.5rem;
    border-radius: 8px;
    color: #a7f3d0;
    text-align: left;
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
}
[data-testid="stMetric"] {
    background-color: #012b35;
    padding: 15px 20px;
    border-radius: 10px;
    border: 1px solid #0f766e;
    box-shadow: 0 2px 10px rgba(0,0,0,0.4);
}
[data-testid="stMetricLabel"] {
    font-size: 0.9rem;
    color: #99f6e4;
}
[data-testid="stMetricValue"] {
    color: #5eead4 !important;
    font-weight: 800 !important;
    font-size: 1.8rem !important;
}
.stAltairChart {
    border-radius: 8px;
    border: 1px solid #0f766e;
    background-color: #012b35;
    padding: 15px;
}
.stDataFrame {
    border: 1px solid #0f766e;
    border-radius: 8px;
    background-color: #002f36 !important;
}
div.stDownloadButton button {
    background: linear-gradient(90deg, #059669, #10b981);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 8px 20px;
    font-weight: 600;
    transition: 0.3s ease;
}
div.stDownloadButton button:hover {
    background: linear-gradient(90deg, #10b981, #34d399);
    transform: scale(1.03);
}
hr {
    border: 1px solid #0f766e;
    margin: 1.5rem 0;
}
h1, h2, h3 {
    color: #5eead4 !important;
    font-weight: 700;
}
h4, h5 {
    color: #99f6e4 !important;
}
</style>
""", unsafe_allow_html=True)

# ========================================
# HEADER
# ========================================
st.markdown("<h1>🟢 MCT Media Monitoring Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<h4>Outlier reports and analytics for monitored media coverage</h4>", unsafe_allow_html=True)
st.markdown("---")

# ========================================
# GOOGLE SHEET CONNECTION
# ========================================
GSHEET_JSON = st.secrets["GSHEET_JSON"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_info(json.loads(GSHEET_JSON), scopes=SCOPES)
client = gspread.authorize(creds)

SHEET_URL = "https://docs.google.com/spreadsheets/d/1xUnrXB0tSG2EZhGr1WXsytKQu7KbudrjDhz1AHbjKGc"
key = SHEET_URL.split("/d/")[1].split("/")[0]
worksheet = client.open_by_key(key).worksheet("Results")
data = worksheet.get_all_records()
df = pd.DataFrame(data)

if df.empty:
    st.warning("No data found in your sheet yet.")
    st.stop()

# Ensure Date column is proper datetime
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# ========================================
# SIDEBAR FILTERS & COLLECTOR BUTTON
# ========================================
st.sidebar.header("⚙️ Search Settings")

platforms = ["All"] + sorted(df["Platform"].dropna().unique().tolist())
sentiments = ["All"] + sorted(df["Sentiment"].dropna().unique().tolist())
themes = ["All"] + sorted(df["All Themes"].dropna().unique().tolist())
media_sectors = ["All"] + sorted(df["Media Sector Impact"].dropna().unique().tolist())

selected_platform = st.sidebar.selectbox("Platform", platforms)
selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)
selected_theme = st.sidebar.selectbox("Theme", themes)
selected_sector = st.sidebar.selectbox("Media Sector Impact", media_sectors)

# Date range filter — handle missing or invalid dates safely
if df["Date"].notnull().any():
    min_date = df["Date"].min()
    max_date = df["Date"].max()
else:
    min_date = pd.Timestamp.today()
    max_date = pd.Timestamp.today()

selected_dates = st.sidebar.date_input(
    "Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date,
)

# -------------------------------
# Apply dynamic filtering
# -------------------------------
filtered = df.copy()

if selected_platform != "All":
    filtered = filtered[filtered["Platform"] == selected_platform]

if selected_sentiment != "All":
    filtered = filtered[filtered["Sentiment"] == selected_sentiment]

if selected_theme != "All":
    filtered = filtered[filtered["All Themes"] == selected_theme]

if selected_sector != "All":
    filtered = filtered[filtered["Media Sector Impact"] == selected_sector]

if isinstance(selected_dates, list) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
    filtered = filtered[
        (filtered["Date"] >= pd.to_datetime(start_date))
        & (filtered["Date"] <= pd.to_datetime(end_date))
    ]

# Collector Button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Run Collector Now"):
    with st.spinner("Collecting latest media data..."):
        result = collect_media_data()
        st.success(f"✅ Collector finished — {result}")
        st.stop()

# ========================================
# METRICS
# ========================================
col1, col2, col3 = st.columns(3)
col1.metric("📰 Articles", len(filtered))
col2.metric("🌍 Platforms", filtered["Platform"].nunique())
col3.metric("🏷️ Themes", filtered["All Themes"].nunique())
st.markdown("---")

# ========================================
# CHARTS
# ========================================
alt.themes.enable("dark")

st.subheader("📊 Number of Articles by Sentiment")
if not filtered.empty:
    sentiment_chart = (
        alt.Chart(filtered)
        .mark_bar()
        .encode(
            x=alt.X("Sentiment:N", title="Sentiment"),
            y=alt.Y("count()", title="Number of Articles"),
            color=alt.Color(
                "Sentiment:N",
                scale=alt.Scale(
                    domain=["Positive", "Neutral", "Negative"],
                    range=["#22c55e", "#facc15", "#ef4444"]
                )
            )
        )
        .properties(height=300)
    )
    st.altair_chart(sentiment_chart, use_container_width=True)

    st.subheader("🏷️ Themes Distribution")
    theme_counts = (
        filtered["All Themes"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "Theme", "All Themes": "Count"})
    )
    theme_chart = (
        alt.Chart(theme_counts)
        .mark_bar()
        .encode(
            x=alt.X("Theme:N", sort="-y", title="Theme"),
            y=alt.Y("Count:Q", title="Articles"),
            color=alt.Color("Theme:N", legend=None)
        )
        .properties(height=300)
    )
    st.altair_chart(theme_chart, use_container_width=True)
else:
    st.info("No data matches the selected filters.")
st.markdown("---")

# ========================================
# TABLE + DOWNLOAD
# ========================================
st.subheader("📄 Detailed Records")
cols = [c for c in ["Platform", "Date", "All Themes", "Sentiment", "Media Sector Impact", "Link"] if c in filtered.columns]
st.dataframe(filtered[cols], use_container_width=True, height=500)

csv = filtered.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Download CSV", csv, "mct_data.csv", "text/csv")

st.success("✅ Dashboard styled successfully — Professional Analytics Mode Active")
