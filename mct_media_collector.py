# ===============================================
# 🟣 MCT Media Monitoring Hybrid Collector (v4.0)
# (RSS + Keywords + AI + Sentiment + GSheet Append)
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
import time

# =====================================================
# 1️⃣ LOAD SECRETS AND INITIALIZE CLIENTS
# =====================================================

GSHEET_JSON = st.secrets["GSHEET_JSON"]
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
RESULTS_SHEET_URL = st.secrets["RESULTS_SHEET_URL"]

client_ai = OpenAI(api_key=OPENAI_API_KEY)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

if not GSHEET_JSON:
    raise RuntimeError("❌ GSHEET_JSON missing in Streamlit secrets.")

creds = Credentials.from_service_account_info(json.loads(GSHEET_JSON), scopes=SCOPES)
client_gsheets = gspread.authorize(creds)

# =====================================================
# 2️⃣ RSS FEEDS (Top National & Regional Sources)
# =====================================================

FEEDS = [
    "https://www.mwananchi.co.tz/feeds/rss.xml",
    "https://www.thecitizen.co.tz/feeds/rss.xml",
    "https://habarileo.co.tz/feed",
    "https://www.dailynews.co.tz/feed",
    "https://www.ippmedia.com/en/feed",
    "https://mtanzania.co.tz/feed/",
    "https://www.mwanahalisionline.com/feed/",
    "https://millardayo.com/feed/",
    "https://dar24.com/feed/",
    "https://bongo5.com/feed/",
    "https://www.globalpublishers.co.tz/feed/",
    "https://sautikubwa.org/feed/",
    "https://zanzibar24.co.tz/feed/",
    "https://www.swahilitimes.co.tz/feed/",
    "https://kivumbinews.co.tz/feed/",
    "https://thechanzo.com/feed/",
    "https://www.bbc.com/swahili/index.xml",
    "https://www.voaswahili.com/rss",
    "https://www.dw.com/overlay/rss/tz",
    "https://allafrica.com/tools/headlines/rdf/tanzania/headlines.rdf"
]

# =====================================================
# 3️⃣ THEMATIC KEYWORDS (Full List – No Reduction)
# =====================================================

THEME_KEYWORDS = {
    "📰 Uhuru wa Vyombo vya Habari (Media Freedom)": [
        "uhuru wa vyombo vya habari", "uhuru wa habari", "uhuru wa kujieleza",
        "sheria ya habari", "kanuni za vyombo vya habari", "udhibiti wa vyombo vya habari",
        "tume ya vyombo vya habari", "leseni ya chombo cha habari", "kuzuiwa kwa habari",
        "kufungiwa gazeti", "kufutwa leseni", "kutopewa habari", "sensa ya habari",
        "taarifa kwa umma", "uwazi wa serikali", "uhuru wa waandishi"
    ],
    "🧑🏽💻 Usalama wa Waandishi wa Habari (Journalist Safety)": [
        "mwandishi wa habari", "kushambuliwa kwa mwandishi", "kukamatwa kwa mwandishi",
        "kuhojiwa na polisi", "kutekwa", "kutoweka", "vitisho dhidi ya waandishi",
        "unyanyasaji kwa waandishi", "kesi ya mwandishi", "kupigwa", "kunyimwa ulinzi",
        "kujeruhiwa kazini", "kukamatwa bila sababu", "waandishi wa habari wanawake",
        "mashambulizi mtandaoni", "vifaa vya ulinzi kwa waandishi"
    ],
    "💰 Uchumi wa Vyombo vya Habari (Media Economy)": [
        "biashara ya vyombo vya habari", "mapato ya matangazo", "changamoto za kiuchumi",
        "kupunguza wafanyakazi", "mishahara midogo", "kudorora kwa matangazo",
        "kampuni za habari", "gharama za uzalishaji", "usimamizi wa vyombo",
        "kufungwa kwa redio", "kufungwa kwa gazeti", "kushuka kwa mapato",
        "mmiliki wa chombo cha habari", "vyombo binafsi", "vyombo vya serikali",
        "mabadiliko ya teknolojia", "ukosefu wa fedha"
    ],
    "⚖️ Ukiukaji na Malalamiko (Press Violations & Complaints)": [
        "malalamiko", "tume ya maadili", "makosa ya kimaadili", "taarifa za uongo",
        "habari za uzushi", "kuchafua jina", "uchochezi", "upotoshaji", "kashfa",
        "kukejeli", "kutukana", "upendeleo wa vyombo", "mahojiano yenye upendeleo",
        "habari zenye ubaguzi", "chuki mtandaoni", "maoni ya wachambuzi",
        "kampeni za chuki", "lugha ya matusi", "taarifa zisizo sahihi", "propaganda"
    ],
    "🗳️ Upendeleo wa Kisiasa (Media Bias and Political Coverage)": [
        "upendeleo wa kisiasa", "vyombo vya ccm", "vyombo vya chadema",
        "vyombo vya act wazalendo", "vyombo vya upinzani", "kampeni za uchaguzi",
        "habari za uchaguzi", "wagombea", "kura", "uchaguzi mkuu", "tume ya uchaguzi",
        "kampeni ya chama", "mgombea urais", "habari za chama", "chama tawala",
        "vyama vya siasa", "taarifa za kampeni", "mjadala wa kisiasa"
    ],
    "💬 Hisia za Umma (Public Sentiment & Perception)": [
        "maoni ya wananchi", "mitazamo ya jamii", "hisia za wananchi", "mjadala mtandaoni",
        "hasira za wananchi", "pongezi kwa serikali", "ukosoaji wa serikali",
        "ukosoaji wa vyombo vya habari", "uaminifu wa vyombo", "maoni ya wasikilizaji",
        "mijadala ya twitter", "mijadala ya facebook", "mjadala wa x space",
        "mitazamo ya vijana", "mitazamo ya wanawake", "mada za mtandaoni"
    ],
    "🌍 Masuala ya Kijamii Yanayogusa Sekta ya Habari (Social & Human Rights Issues)": [
        "haki za binadamu", "haki ya kupata habari", "uwajibikaji wa serikali",
        "demokrasia", "uwazi na uwajibikaji", "uhuru wa kujieleza", "uongozi bora",
        "ukandamizaji", "rushwa", "haki za wanawake", "haki za watoto",
        "ajira kwa vijana", "usawa wa kijinsia", "unyanyasaji wa kijinsia",
        "vyombo vya kijamii", "ushawishi wa mitandao", "usalama wa mtandao"
    ],
    "🧠 Maneno ya Kiufundi na Uchanganuzi wa Data (Analytics & AI Monitoring)": [
        "ufuatiliaji wa habari", "kuchambua maudhui", "uchambuzi wa hisia",
        "ai katika habari", "data za kijamii", "mfumo wa uchambuzi", "dashibodi ya habari",
        "ukusanyaji wa taarifa", "takwimu za habari", "mwenendo wa vyombo",
        "ufuatiliaji wa habari za uchaguzi", "ripoti ya kila wiki", "taarifa za kila mwezi"
    ]
}

# =====================================================
# 4️⃣ HELPER FUNCTIONS (Enhanced Cleaning & Retry)
# =====================================================

ai_cache = {}

def clean_html(text):
    clean = BeautifulSoup(str(text), "html.parser").get_text()
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()

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
    if score > 0.1:
        return "Positive"
    elif score < -0.1:
        return "Negative"
    return "Neutral"

def ai_classify_themes(text):
    # Use cache to avoid repeated GPT calls
    if text in ai_cache:
        return ai_cache[text]

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
    for attempt in range(2):  # Retry once if API call fails
        try:
            resp = client_ai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            answer = resp.choices[0].message.content.strip()
            result = [x.strip() for x in answer.split(",") if x.strip()]
            ai_cache[text] = result
            return result
        except Exception as e:
            print(f"⚠️ AI tagging failed (attempt {attempt+1}):", e)
            time.sleep(2)
    ai_cache[text] = []
    return []

# =====================================================
# 5️⃣ FETCH RSS (Full History)
# =====================================================

def fetch_rss():
    records = []
    for url in FEEDS:
        try:
            feed = feedparser.parse(url)
            source = feed.feed.get("title", url)
            total = len(feed.entries)
            print(f"📡 {source} — {total} entries")

            for entry in feed.entries:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                text = clean_html(f"{title} {summary}")

                published = (
                    entry.get("published")
                    or entry.get("updated")
                    or datetime.now().strftime("%Y-%m-%d")
                )

                records.append({
                    "Platform": source,
                    "Content": text,
                    "Link": entry.get("link", ""),
                    "Date": published
                })
        except Exception as e:
            print(f"⚠️ Failed to parse {url}: {e}")

    df = pd.DataFrame(records)
    print(f"✅ Total collected: {len(df)}")
    return df

# =====================================================
# 6️⃣ DETERMINE MEDIA SECTOR IMPACT
# =====================================================

def determine_media_impact(row):
    themes = row.get("All Themes", "")
    if any(keyword in themes for keyword in [
        "Media Freedom", "Vyombo vya Habari", "Journalist Safety",
        "Media Economy", "Ukiukaji", "Malalamiko"
    ]):
        return "Direct Impact on Media Sector"
    elif any(keyword in themes for keyword in [
        "Political Coverage", "Public Sentiment", "Social", "Human Rights"
    ]):
        return "Indirect / Contextual Impact"
    return "No Direct Impact"

# =====================================================
# 7️⃣ UPLOAD TO GOOGLE SHEET (Append + No Duplicates)
# =====================================================

def upload_to_gsheet(df, sheet_title="Results"):
    if not RESULTS_SHEET_URL or "/d/" not in RESULTS_SHEET_URL:
        raise RuntimeError("❌ RESULTS_SHEET_URL invalid or missing in secrets.")

    key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
    sh = client_gsheets.open_by_key(key)

    try:
        ws = sh.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_title, rows=2000, cols=26)

    expected_columns = [
        "Platform", "Content", "Link", "Date",
        "All Themes", "Sentiment", "Media Sector Impact", "Collected At"
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[expected_columns]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    existing_data = ws.get_all_records()
    if existing_data:
        existing_df = pd.DataFrame(existing_data)
        new_df = df[~df["Link"].isin(existing_df["Link"])]
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = df

    ws.clear()
    ws.append_row(list(final_df.columns), value_input_option="USER_ENTERED")

    rows = final_df.astype(str).fillna("").values.tolist()
    CHUNK = 300
    for i in range(0, len(rows), CHUNK):
        ws.append_rows(rows[i:i + CHUNK], value_input_option="USER_ENTERED")

    print(f"✅ Appended {len(df)} new rows (total {len(final_df)} records).")

# =====================================================
# 8️⃣ MAIN COLLECTOR FUNCTION
# =====================================================

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
    df["Collected At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_final = df[
        ["Platform", "Content", "Link", "Date", "All Themes", "Sentiment", "Media Sector Impact", "Collected At"]
    ]
    upload_to_gsheet(df_final, sheet_title="Results")

    return f"✅ Collected and uploaded {len(df_final)} articles successfully."
