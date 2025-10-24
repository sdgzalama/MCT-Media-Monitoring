import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import altair as alt
import json

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

/* ===== GLOBAL BACKGROUND ===== */
.stApp {
    background: radial-gradient(circle at top left, #002933 0%, #001a1f 100%);
    color: #d6f1f0;
    font-family: 'Inter', 'Segoe UI', sans-serif;
}

/* ===== SIDEBAR ===== */
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
[data-testid="stSidebar"] .stSelectbox {
    background-color: #022c33 !important;
    border-radius: 8px;
}
[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] {
    background-color: #022c33;
    border: 1px solid #065f46;
    border-radius: 6px;
}

/* ===== HEADER BAR ===== */
header, .block-container > div:first-child {
    background: linear-gradient(90deg, #001f25, #00494f);
    padding: 0.8rem 1.5rem;
    border-radius: 8px;
    color: #a7f3d0;
    text-align: left;
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
}

/* ===== METRIC CARDS ===== */
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

/* ===== CHARTS ===== */
.vega-embed summary {
    background-color: #014b4f !important;
    color: #d1fae5 !important;
}
.stAltairChart {
    border-radius: 8px;
    border: 1px solid #0f766e;
    background-color: #012b35;
    padding: 15px;
}

/* ===== DATAFRAME TABLE ===== */
.stDataFrame {
    border: 1px solid #0f766e;
    border-radius: 8px;
    background-color: #002f36 !important;
}

/* ===== DOWNLOAD BUTTON ===== */
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

/* ===== DIVIDERS ===== */
hr {
    border: 1px solid #0f766e;
    margin: 1.5rem 0;
}

/* ===== TITLES ===== */
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

SHEET_URL = "https://docs.google.com/spreadsheets/d/1xUnrXB0tSG2EZhGr1WXsytKQu7KbudrjDhz1AHbjKGc/edit?gid=857397725#gid=857397725"
key = SHEET_URL.split("/d/")[1].split("/")[0]
worksheet = client.open_by_key(key).worksheet("Results")
data = worksheet.get_all_records()
df = pd.DataFrame(data)

if df.empty:
    st.warning("No data found in your sheet yet.")
    st.stop()

# ========================================
# SIDEBAR FILTERS
# ========================================
st.sidebar.header("⚙️ Search Settings")
platforms = ["All"] + sorted(df["Platform"].dropna().unique().tolist())
sentiments = ["All"] + sorted(df["Sentiment"].dropna().unique().tolist())
themes = ["All"] + sorted(df["All Themes"].dropna().unique().tolist())

selected_platform = st.sidebar.selectbox("Platform", platforms)
selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)
selected_theme = st.sidebar.selectbox("Theme", themes)

filtered = df.copy()
if selected_platform != "All":
    filtered = filtered[filtered["Platform"] == selected_platform]
if selected_sentiment != "All":
    filtered = filtered[filtered["Sentiment"] == selected_sentiment]
if selected_theme != "All":
    filtered = filtered[filtered["All Themes"] == selected_theme]

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
sentiment_chart = (
    alt.Chart(filtered)
    .mark_bar()
    .encode(
        x=alt.X("Sentiment:N", title="Sentiment"),
        y=alt.Y("count()", title="Number of Articles"),
        color=alt.Color("Sentiment:N", scale=alt.Scale(
            domain=["Positive", "Neutral", "Negative"],
            range=["#22c55e", "#facc15", "#ef4444"]
        ))
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
        x=alt.X("Theme:N", sort="-y"),
        y=alt.Y("Count:Q"),
        color=alt.Color("Theme:N", legend=None)
    )
    .properties(height=300)
)
st.altair_chart(theme_chart, use_container_width=True)

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
