# ===============================================
# 🟣 MCT Media Monitoring Analyzer
# (Analyzer tab only — Keyword-Based Tagging)
# ===============================================

import streamlit as st
import pandas as pd
import re
import json
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError as GSpreadAPIError
import tempfile
import os

# ------------------------------
# App Config
# ------------------------------
st.set_page_config(page_title="MCT Media Monitoring Analyzer", layout="wide")
st.title("🟣 MCT Media Monitoring Analyzer")

st.markdown("""
Upload your Excel/CSV file below.  
The app will automatically detect themes such as **Uhuru wa Vyombo vya Habari**,  
**Usalama wa Waandishi wa Habari**, **Uchumi wa Vyombo vya Habari**, and more,  
then append the analyzed results to your **Google Sheet dashboard**.
""")

# ------------------------------
# Load Secrets and Setup GSheets
# ------------------------------
RESULTS_SHEET_URL = st.secrets.get(
    "RESULTS_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1TGS0cfKylyWmxYsbHXw6_FRCGl0o0vdCipM_23EK4cY/edit?gid=0#gid=0"
)

SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_gs_client():
    creds_dict = json.loads(st.secrets["GSHEET_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
    client = gspread.authorize(creds)
    return client

client_gsheets = get_gs_client()

# ------------------------------
# THEME KEYWORDS (Simplified)
# ------------------------------
THEME_KEYWORDS = {
    "📰 Uhuru wa Vyombo vya Habari (Media Freedom)": [
        "uhuru wa vyombo vya habari","uhuru wa habari","uhuru wa kujieleza",
        "sheria ya habari","kanuni za vyombo vya habari","udhibiti wa vyombo vya habari",
        "tume ya vyombo vya habari","leseni ya chombo cha habari","kuzuiwa kwa habari",
        "kufungiwa gazeti","kufutwa leseni","kutopewa habari","sensa ya habari",
        "taarifa kwa umma","uwazi wa serikali"
    ],
    "🧑🏽💻 Usalama wa Waandishi wa Habari (Journalist Safety)": [
        "mwandishi wa habari","kushambuliwa kwa mwandishi","kukamatwa kwa mwandishi",
        "kuhojiwa na polisi","kutekwa","kutoweka","vitisho dhidi ya waandishi",
        "unyanyasaji kwa waandishi","kesi ya mwandishi","kupigwa","kunyimwa ulinzi",
        "kujeruhiwa kazini","kukamatwa bila sababu","waandishi wa habari wanawake",
        "mashambulizi mtandaoni"
    ],
    "💰 Uchumi wa Vyombo vya Habari (Media Economy)": [
        "biashara ya vyombo vya habari","mapato ya matangazo","changamoto za kiuchumi",
        "kupunguza wafanyakazi","mishahara midogo","kudorora kwa matangazo",
        "kampuni za habari","gharama za uzalishaji","usimamizi wa vyombo",
        "kufungwa kwa redio","kufungwa kwa gazeti","kushuka kwa mapato",
        "mmiliki wa chombo cha habari","vyombo binafsi","vyombo vya serikali"
    ],
    "⚖️ Ukiukaji na Malalamiko (Press Violations & Complaints)": [
        "malalamiko","tume ya maadili","makosa ya kimaadili","taarifa za uongo",
        "habari za uzushi","kuchafua jina","uchochezi","upotoshaji","kashfa",
        "kukejeli","kutukana","upendeleo wa vyombo","mahojiano yenye upendeleo",
        "habari zenye ubaguzi","chuki mtandaoni","maoni ya wachambuzi",
        "kampeni za chuki","lugha ya matusi","taarifa zisizo sahihi","propaganda"
    ],
    "🗳️ Upendeleo wa Kisiasa (Media Bias and Political Coverage)": [
        "upendeleo wa kisiasa","vyombo vya ccm","vyombo vya chadema","vyombo vya act wazalendo",
        "vyombo vya upinzani","kampeni za uchaguzi","habari za uchaguzi","wagombea",
        "kura","uchaguzi mkuu","tume ya uchaguzi","kampeni ya chama","mgombea urais",
        "habari za chama","chama tawala","vyama vya siasa","taarifa za kampeni",
        "mahojiano ya kisiasa","makala ya kisiasa","mjadala wa kisiasa"
    ],
    "💬 Hisia za Umma (Public Sentiment & Perception)": [
        "maoni ya wananchi","mitazamo ya jamii","hisia za wananchi","mjadala mtandaoni",
        "hasira za wananchi","pongezi kwa serikali","ukosoaji wa serikali","ukosoaji wa vyombo vya habari",
        "uaminifu wa vyombo","maoni ya wasikilizaji","maoni ya watazamaji",
        "mijadala ya twitter","mijadala ya facebook","mjadala wa x space",
        "mitazamo ya vijana","mitazamo ya wanawake","mitazamo ya wanahabari",
        "mada za mtandaoni","gumzo mtandaoni","kampeni za hashtag"
    ],
    "🌍 Masuala ya Kijamii Yanayogusa Sekta ya Habari (Social & Human Rights Issues)": [
        "haki za binadamu","haki ya kupata habari","uwajibikaji wa serikali",
        "demokrasia","uwazi na uwajibikaji","uhuru wa kujieleza","uongozi bora",
        "ukandamizaji","rushwa","haki za wanawake","haki za watoto",
        "ajira kwa vijana","elimu ya habari","usawa wa kijinsia","unyanyasaji wa kijinsia",
        "vyombo vya kijamii","ushawishi wa mitandao","taarifa za kidijitali",
        "habari za mitandaoni","usalama wa mtandao"
    ],
    "🧠 Maneno ya Kiufundi na Uchanganuzi wa Data (Analytics & AI Monitoring)": [
        "ufuatiliaji wa habari","kuchambua maudhui","uchambuzi wa hisia","ai katika habari",
        "data za kijamii","mfumo wa uchambuzi","dashibodi ya habari","ukusanyaji wa taarifa",
        "takwimu za habari","ripoti ya habari","mwenendo wa vyombo","mitazamo chanya",
        "mitazamo hasi","ufahamu wa umma","kuenea kwa habari","ufuatiliaji wa matukio",
        "mwenendo wa mitandao","ufuatiliaji wa habari za uchaguzi","ripoti ya kila wiki",
        "taarifa za kila mwezi","ulinganifu wa maudhui","ushawishi wa watumiaji",
        "vyombo vya habari mtandaoni","blogu za habari","tovuti za habari"
    ]
}

# ------------------------------
# Tagging Function
# ------------------------------
def detect_themes(text):
    text_lower = str(text).lower()
    detected = []
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", text_lower):
                detected.append(theme)
                break
    return detected

# ------------------------------
# Upload Section
# ------------------------------
uploaded_file = st.file_uploader("📂 Upload Excel/CSV file", type=["csv", "xlsx", "xls"])

if uploaded_file:
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.subheader("📋 Uploaded Data Preview")
    st.dataframe(df.head(10), use_container_width=True)

    # Identify content columns automatically
    possible_cols = ["Content", "Description", "Story", "Post"]
    text_col = None
    for c in possible_cols:
        if c in df.columns:
            text_col = c
            break

    if not text_col:
        st.error("❌ No text column found. Please include one of: Content, Description, Story, or Post.")
        st.stop()

    if "Link" not in df.columns:
        st.warning("⚠️ No 'Link' column found. A 'Link' column is recommended for reference.")

    st.info(f"✅ Using **{text_col}** as the text column for analysis.")

    # Detect themes
    with st.spinner("🔍 Analyzing and tagging themes..."):
        df["Detected Themes"] = df[text_col].apply(detect_themes)
        df["Detected Themes"] = df["Detected Themes"].apply(lambda x: ", ".join(x) if x else "—")

    st.success(f"✅ Analysis complete! {len(df)} rows processed.")

    st.subheader("📊 Results Preview")
    st.dataframe(df, use_container_width=True, height=400)

    # ------------------------------
    # Upload to Google Sheet
    # ------------------------------
    def upload_to_results_sheet(df):
        key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
        try:
            sheet = client_gsheets.open_by_key(key)
            worksheet = sheet.get_worksheet(0)
            rows = df.astype(str).fillna("").values.tolist()
            CHUNK = 500
            for i in range(0, len(rows), CHUNK):
                worksheet.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")
            st.success("✅ Data appended to the Results Sheet (Dashboard).")

            sheet_link = RESULTS_SHEET_URL.replace("/edit", "")
            st.markdown(f"[📊 Open Dashboard Sheet ↗️]({sheet_link})", unsafe_allow_html=True)

        except GSpreadAPIError as e:
            if "403" in str(e):
                st.error("Upload failed (403). Share the RESULTS sheet with your service account email.")
            else:
                st.error(f"❌ Failed to upload to Google Sheets: {e}")
        except Exception as e:
            st.error(f"❌ Failed to upload to Google Sheets: {e}")

    # Upload Button
    if st.button("⬆️ Upload Results to Google Sheet"):
        upload_to_results_sheet(df)

    # Allow CSV download
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("💾 Download Tagged CSV", csv, "MCT_Tagged_Results.csv", "text/csv")
else:
    st.info("Please upload an Excel or CSV file to begin analysis.")
