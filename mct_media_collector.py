# ===============================================
# 🟣 MCT Media Monitoring Hybrid Collector
# (RSS + Keywords + AI + Sentiment + GSheet)
# ===============================================

import feedparser
import pandas as pd
import re
from bs4 import BeautifulSoup
from textblob import TextBlob
import json
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI
from datetime import datetime
import streamlit as st

# ------------------------------
# Load Secrets (Streamlit-safe)
# ------------------------------
GSHEET_JSON = st.secrets["GSHEET_JSON"]
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
RESULTS_SHEET_URL = st.secrets["RESULTS_SHEET_URL"]

# ------------------------------
# Initialize clients
# ------------------------------
client_ai = OpenAI(api_key=OPENAI_API_KEY)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
if not GSHEET_JSON:
    raise RuntimeError("GSHEET_JSON missing in Streamlit secrets")

creds = Credentials.from_service_account_info(json.loads(GSHEET_JSON), scopes=SCOPES)
client_gsheets = gspread.authorize(creds)

# ------------------------------
# Define RSS sources
# ------------------------------
# FEEDS = [
#     "https://www.mwananchi.co.tz/feeds/rss.xml",
#     "https://www.thecitizen.co.tz/feeds/rss.xml",
#     "https://habarileo.co.tz/feed",
#     "https://www.ippmedia.com/en/feed",
#     "https://www.bbc.com/swahili/index.xml",
#     "https://rss.app/feeds/JwaKtg2e0IN8uZmg.xml"
# ]
# ------------------------------
# Define RSS sources
# ------------------------------
FEEDS = [
    # --- Major national outlets ---
    "https://www.mwananchi.co.tz/feeds/rss.xml",
    "https://www.thecitizen.co.tz/feeds/rss.xml",
    "https://habarileo.co.tz/feed",
    "https://www.ippmedia.com/en/feed",
    "https://www.dailynews.co.tz/feed",
    "https://mtanzania.co.tz/feed/",
    "https://www.mwanahalisionline.com/feed/",
    "https://dar24.com/feed/",
    "https://bongo5.com/feed/",
    "https://millardayo.com/feed/",
    "https://www.globalpublishers.co.tz/feed/",
    "https://www.fullshangweblog.co.tz/feed/",
    "https://www.fikrapevu.com/feed/",
    "https://www.tzaffairs.org/feed/",
    "https://taifaleo.nation.africa/feed/",
    "https://www.bbc.com/swahili/index.xml",
    "https://zanzibar24.co.tz/feed/",
    "https://www.swahilitimes.co.tz/feed/",
    "https://sautikubwa.org/feed/",
    "https://www.udakuspecially.com/feeds/posts/default",
    "https://www.tanzaniainvest.com/feed",
    "https://mtembezi.co.tz/feed/",
    "https://kivumbinews.co.tz/feed/",
    "https://mwanaspoti.co.tz/feeds/rss.xml",
    "https://www.tanzania.go.tz/rss",
    "https://jamiiforums.com/forums/-/index.rss",
    "https://www.ajirazetu.co.tz/feeds/posts/default",
    "https://shaffihdauda.co.tz/feed/",
    "https://kahamaonline.co.tz/feed/",
    "https://www.michuzi.co.tz/feeds/posts/default",

    # --- Regional news (Zanzibar, Dodoma, Mbeya, etc.) ---
    "https://zanzinews.blogspot.com/feeds/posts/default",
    "https://www.mtembezi.co.tz/feed/",
    "https://kyelatoday.blogspot.com/feeds/posts/default",
    "https://morogoroonline.blogspot.com/feeds/posts/default",
    "https://dodomanews.wordpress.com/feed/",
    "https://arusha24.co.tz/feed/",
    "https://iringa24.blogspot.com/feeds/posts/default",
    "https://lindinews.blogspot.com/feeds/posts/default",
    "https://rukwanews.blogspot.com/feeds/posts/default",
    "https://songwenews.blogspot.com/feeds/posts/default",
    "https://mbeyacity.blogspot.com/feeds/posts/default",
    "https://tundumacity.blogspot.com/feeds/posts/default",
    "https://mwanza24.blogspot.com/feeds/posts/default",
    "https://geitacity.blogspot.com/feeds/posts/default",
    "https://maraonline.blogspot.com/feeds/posts/default",
    "https://katavi24.blogspot.com/feeds/posts/default",
    "https://njombeonline.blogspot.com/feeds/posts/default",
    "https://ruvuma24.blogspot.com/feeds/posts/default",
    "https://morogorocity.blogspot.com/feeds/posts/default",

    # --- Sector-specific feeds ---
    # Economy, Business & Agriculture
    "https://www.tanzaniainvest.com/feed",
    "https://www.ippmedia.com/en/business/feed",
    "https://www.thecitizen.co.tz/tanzania/news/business/-/feeds/rss.xml",
    "https://agrinews.co.tz/feed/",
    "https://kilimonews.co.tz/feed/",
    "https://agronews.co.tz/feed/",
    "https://www.kilimo.go.tz/rss",
    "https://www.nbs.go.tz/rss",
    "https://zabuni.co.tz/feed/",
    "https://www.azaniapost.com/feed/",
    "https://mipango.go.tz/rss",

    # Health, Education & Human Rights
    "https://www.who.int/rss-feeds/news-english.xml",
    "https://unicef.org/tanzania/rss.xml",
    "https://www.unaids.org/en/rss.xml",
    "https://thechanzo.com/feed/",
    "https://thechanzoletu.substack.com/feed",
    "https://humanrights.or.tz/feed/",
    "https://jamiihealth.blogspot.com/feeds/posts/default",
    "https://tanzaniapresscentre.org/feed/",
    "https://uhurumedia.co.tz/feed/",
    "https://thechanzo.com/category/uchumi/feed/",
    "https://thechanzo.com/category/jamii/feed/",
    "https://thechanzo.com/category/siam/feed/",
    "https://thechanzo.com/category/siasa/feed/",

    # --- NGO, Civic, and Watchdog sources ---
    "https://taweza.or.tz/feed/",
    "https://policyforum-tz.org/rss.xml",
    "https://twaweza.org/feed/",
    "https://hakielimu.or.tz/feed/",
    "https://songas.com/feed/",
    "https://legalservices.co.tz/feed/",
    "https://wanawakewakatoliki.or.tz/feed/",
    "https://barazalawanasiasa.or.tz/feed/",
    "https://mis.org.tz/feed/",
    "https://zlsctz.org/feed/",
    "https://tanlap.or.tz/feed/",
    "https://jamiiwatch.or.tz/feed/",
    "https://watetezi.or.tz/feed/",
    "https://defenddefenders.org/feed/",
    "https://mediazanzibar.or.tz/feed/",
    "https://mwanzapressclub.or.tz/feed/",
    "https://mct.or.tz/feed/",

    # --- International or East African feeds relevant to TZ ---
    "https://www.theeastafrican.co.ke/feeds/rss.xml",
    "https://www.standardmedia.co.ke/rss/headlines.php",
    "https://www.monitor.co.ug/uganda/rss.xml",
    "https://nation.africa/kenya/rss.xml",
    "https://www.voaswahili.com/rss",
    "https://www.dw.com/overlay/rss/tz",
    "https://allafrica.com/tools/headlines/rdf/tanzania/headlines.rdf",
    "https://allafrica.com/tools/headlines/rdf/eastafrica/headlines.rdf",
    "https://rss.app/feeds/JwaKtg2e0IN8uZmg.xml"
]

# ------------------------------
# Keyword dictionary
# ------------------------------
THEME_KEYWORDS = {
    "📰 Uhuru wa Vyombo vya Habari (Media Freedom)": [
        "uhuru wa vyombo vya habari", "uhuru wa habari", "uhuru wa kujieleza",
        "sheria ya habari", "kanuni za vyombo vya habari", "udhibiti wa vyombo vya habari",
        "tume ya vyombo vya habari", "leseni ya chombo cha habari", "kuzuiwa kwa habari",
        "kufungiwa gazeti", "kufutwa leseni", "kutopewa habari", "sensa ya habari",
        "taarifa kwa umma", "uwazi wa serikali"
    ],
    "🧑🏽💻 Usalama wa Waandishi wa Habari (Journalist Safety)": [
        "mwandishi wa habari", "kushambuliwa kwa mwandishi", "kukamatwa kwa mwandishi",
        "kuhojiwa na polisi", "kutekwa", "kutoweka", "vitisho dhidi ya waandishi",
        "unyanyasaji kwa waandishi", "kesi ya mwandishi", "kupigwa", "kunyimwa ulinzi",
        "kujeruhiwa kazini", "kukamatwa bila sababu", "waandishi wa habari wanawake",
        "mashambulizi mtandaoni"
    ],
    "💰 Uchumi wa Vyombo vya Habari (Media Economy)": [
        "biashara ya vyombo vya habari", "mapato ya matangazo", "changamoto za kiuchumi",
        "kupunguza wafanyakazi", "mishahara midogo", "kudorora kwa matangazo",
        "kampuni za habari", "gharama za uzalishaji", "usimamizi wa vyombo",
        "kufungwa kwa redio", "kufungwa kwa gazeti", "kushuka kwa mapato",
        "mmiliki wa chombo cha habari", "vyombo binafsi", "vyombo vya serikali"
    ],
    "⚖️ Ukiukaji na Malalamiko (Press Violations & Complaints)": [
        "malalamiko", "tume ya maadili", "makosa ya kimaadili", "taarifa za uongo",
        "habari za uzushi", "kuchafua jina", "uchochezi", "upotoshaji", "kashfa",
        "kukejeli", "kutukana", "upendeleo wa vyombo", "mahojiano yenye upendeleo",
        "habari zenye ubaguzi", "chuki mtandaoni", "maoni ya wachambuzi",
        "kampeni za chuki", "lugha ya matusi", "taarifa zisizo sahihi", "propaganda"
    ],
    "🗳️ Upendeleo wa Kisiasa (Media Bias and Political Coverage)": [
        "upendeleo wa kisiasa", "vyombo vya ccm", "vyombo vya chadema", "vyombo vya act wazalendo",
        "vyombo vya upinzani", "kampeni za uchaguzi", "habari za uchaguzi", "wagombea",
        "kura", "uchaguzi mkuu", "tume ya uchaguzi", "kampeni ya chama", "mgombea urais",
        "habari za chama", "chama tawala", "vyama vya siasa", "taarifa za kampeni",
        "mahojiano ya kisiasa", "makala ya kisiasa", "mjadala wa kisiasa"
    ],
    "💬 Hisia za Umma (Public Sentiment & Perception)": [
        "maoni ya wananchi", "mitazamo ya jamii", "hisia za wananchi", "mjadala mtandaoni",
        "hasira za wananchi", "pongezi kwa serikali", "ukosoaji wa serikali", "ukosoaji wa vyombo vya habari",
        "uaminifu wa vyombo", "maoni ya wasikilizaji", "maoni ya watazamaji",
        "mijadala ya twitter", "mijadala ya facebook", "mjadala wa x space",
        "mitazamo ya vijana", "mitazamo ya wanawake", "mitazamo ya wanahabari",
        "mada za mtandaoni", "gumzo mtandaoni", "kampeni za hashtag"
    ],
    "🌍 Masuala ya Kijamii Yanayogusa Sekta ya Habari (Social & Human Rights Issues)": [
        "haki za binadamu", "haki ya kupata habari", "uwajibikaji wa serikali",
        "demokrasia", "uwazi na uwajibikaji", "uhuru wa kujieleza", "uongozi bora",
        "ukandamizaji", "rushwa", "haki za wanawake", "haki za watoto",
        "ajira kwa vijana", "elimu ya habari", "usawa wa kijinsia", "unyanyasaji wa kijinsia",
        "vyombo vya kijamii", "ushawishi wa mitandao", "taarifa za kidijitali",
        "habari za mitandaoni", "usalama wa mtandao"
    ],
    "🧠 Maneno ya Kiufundi na Uchanganuzi wa Data (Analytics & AI Monitoring)": [
        "ufuatiliaji wa habari", "kuchambua maudhui", "uchambuzi wa hisia", "ai katika habari",
        "data za kijamii", "mfumo wa uchambuzi", "dashibodi ya habari", "ukusanyaji wa taarifa",
        "takwimu za habari", "ripoti ya habari", "mwenendo wa vyombo", "mitazamo chanya",
        "mitazamo hasi", "ufahamu wa umma", "kuenea kwa habari", "ufuatiliaji wa matukio",
        "mwenendo wa mitandao", "ufuatiliaji wa habari za uchaguzi", "ripoti ya kila wiki",
        "taarifa za kila mwezi", "ulinganifu wa maudhui", "ushawishi wa watumiaji",
        "vyombo vya habari mtandaoni", "blogu za habari", "tovuti za habari"
    ]
}

# ------------------------------
# Helper functions
# ------------------------------
def clean_html(text):
    return BeautifulSoup(str(text), "html.parser").get_text()

def detect_themes(text):
    text_lower = str(text).lower()
    detected = []
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", text_lower):
                detected.append(theme)
                break
    return detected

def detect_sentiment(text):
    score = TextBlob(str(text)).sentiment.polarity
    if score > 0.1: return "Positive"
    elif score < -0.1: return "Negative"
    return "Neutral"

def ai_classify_themes(text):
    prompt = f"""
    You are an assistant for media monitoring in Tanzania.
    Classify the following text into one or more of these themes:
    1. Media Freedom
    2. Journalist Safety
    3. Media Economy
    4. Violations & Complaints
    5. Political Bias
    6. Public Sentiment
    7. Social & Human Rights Issues
    8. Analytics & AI Monitoring
    Return only a comma-separated list of matching theme names.
    Text:
    {text}
    """
    try:
        resp = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        answer = resp.choices[0].message.content.strip()
        return [x.strip() for x in answer.split(",") if x.strip()]
    except Exception as e:
        print("⚠️ AI tagging failed:", e)
        return []

# ------------------------------
# Fetch RSS data
# ------------------------------
def fetch_rss():
    records = []
    for url in FEEDS:
        feed = feedparser.parse(url)
        source = feed.feed.get("title", "Unknown Source")
        for entry in feed.entries[:10]:
            text = clean_html(entry.get("title", "") + " " + entry.get("summary", ""))
            records.append({
                "Platform": source,
                "Content": text,
                "Link": entry.get("link", ""),
                "Date": entry.get("published", datetime.now().strftime("%Y-%m-%d"))
            })
    return pd.DataFrame(records)

# ------------------------------
# Determine Media Sector Impact
# ------------------------------
def determine_media_impact(row):
    themes = row.get("All Themes", "")
    text = row.get("Content", "")

    if any(keyword in themes for keyword in [
        "Media Freedom", "Vyombo vya Habari", "Journalist Safety",
        "Media Economy", "Ukiukaji", "Malalamiko"
    ]):
        return "Direct Impact on Media Sector"
    elif any(keyword in themes for keyword in [
        "Political Coverage", "Public Sentiment", "Social", "Human Rights"
    ]):
        return "Indirect / Contextual Impact"

    try:
        prompt = f"""
        You are a Tanzanian media analyst.
        Does this text have a *direct impact* on the media sector
        (e.g., journalists, media freedom, economy) or is it indirect?
        Reply with one:
        - Direct Impact on Media Sector
        - Indirect / Contextual Impact
        - No Direct Impact
        Text: {text}
        """
        resp = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return "No Direct Impact"

# ------------------------------
# Google Sheet upload
# ------------------------------
# def upload_to_gsheet(df, sheet_title="Results"):
#     if not RESULTS_SHEET_URL or "/d/" not in RESULTS_SHEET_URL:
#         raise RuntimeError("RESULTS_SHEET_URL invalid or missing in secrets.")

#     key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
#     sh = client_gsheets.open_by_key(key)

#     try:
#         ws = sh.worksheet(sheet_title)
#     except gspread.exceptions.WorksheetNotFound:
#         ws = sh.add_worksheet(title=sheet_title, rows=2000, cols=26)

#     # ✅ normalize date and clear
#     df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
#     ws.clear()
#     ws.append_row(list(df.columns), value_input_option="USER_ENTERED")

#     # ✅ upload data
#     rows = df.astype(str).fillna("").values.tolist()
#     CHUNK = 300
#     for i in range(0, len(rows), CHUNK):
#         ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")

#     print(f"✅ Replaced sheet data with {len(df)} rows (worksheet: {sheet_title}).")
# ------------------------------
# Google Sheet upload (fixed alignment)
# ------------------------------
def upload_to_gsheet(df, sheet_title="Results"):
    if not RESULTS_SHEET_URL or "/d/" not in RESULTS_SHEET_URL:
        raise RuntimeError("RESULTS_SHEET_URL invalid or missing in secrets.")

    key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
    sh = client_gsheets.open_by_key(key)

    try:
        ws = sh.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_title, rows=2000, cols=26)

    # ✅ Select and reorder columns exactly as desired
    expected_columns = [
        "Platform",
        "Content",
        "Link",
        "Date",
        "All Themes",
        "Sentiment",
        "Media Sector Impact"
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[expected_columns]

    # ✅ Normalize date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%a, %d %b %Y %H:%M:%S")

    # ✅ Clean and overwrite
    ws.clear()
    ws.append_row(expected_columns, value_input_option="USER_ENTERED")

    # ✅ Upload in chunks
    rows = df.astype(str).fillna("").values.tolist()
    CHUNK = 300
    for i in range(0, len(rows), CHUNK):
        ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")

    print(f"✅ Uploaded {len(df)} rows to Google Sheet (worksheet: {sheet_title}).")


# ------------------------------
# Collector Function (used by dashboard)
# ------------------------------
def collect_media_data():
    df = fetch_rss()
    if df.empty:
        return "No articles fetched from RSS feeds."

    df["Detected Themes"] = df["Content"].apply(detect_themes)
    df["AI Themes"] = df.apply(lambda r: ai_classify_themes(r["Content"]) if not r["Detected Themes"] else [], axis=1)
    df["All Themes"] = df.apply(
        lambda r: ", ".join(set(r["Detected Themes"] + r["AI Themes"])) if (r["Detected Themes"] or r["AI Themes"]) else "—",
        axis=1
    )
    df["Sentiment"] = df["Content"].apply(detect_sentiment)
    df["Media Sector Impact"] = df.apply(determine_media_impact, axis=1)

    df_final = df[["Platform", "Content", "Link", "Date", "All Themes", "Sentiment", "Media Sector Impact"]]
    upload_to_gsheet(df_final, sheet_title="Results")

    return f"✅ Collected and uploaded {len(df_final)} articles successfully."


