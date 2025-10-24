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

# ------------------------------
# Load Secrets
# ------------------------------
with open(".streamlit/secrets.toml", "r", encoding="utf-8") as f:
    content = f.read()

def extract_secret(key):
    match = re.search(rf'{key}\s*=\s*"(.*?)"', content)
    return match.group(1) if match else None

OPENAI_API_KEY = extract_secret("OPENAI_API_KEY")
RESULTS_SHEET_URL = extract_secret("RESULTS_SHEET_URL")

GSHEET_JSON_BLOCK = re.search(r"GSHEET_JSON\s*=\s*'''(.*?)'''", content, re.S)
GSHEET_JSON = json.loads(GSHEET_JSON_BLOCK.group(1)) if GSHEET_JSON_BLOCK else None

# ------------------------------
# Initialize clients
# ------------------------------
client_ai = OpenAI(api_key=OPENAI_API_KEY)

# Google Sheets auth via GSHEET_JSON from secrets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
if not GSHEET_JSON:
    raise RuntimeError("GSHEET_JSON missing in .streamlit/secrets.toml")

creds = Credentials.from_service_account_info(GSHEET_JSON, scopes=SCOPES)
client_gsheets = gspread.authorize(creds)

# Helpful once: confirm which SA needs Sheet access
print("Service account:", creds.service_account_email)


# ------------------------------
# Define RSS sources
# ------------------------------
FEEDS = [
    "https://www.mwananchi.co.tz/feeds/rss.xml",
    "https://www.thecitizen.co.tz/feeds/rss.xml",
    "https://habarileo.co.tz/feed",
    "https://www.ippmedia.com/en/feed",
    "https://www.bbc.com/swahili/index.xml",
]

# ------------------------------
# Keyword dictionary (abbreviated header; full 150 items)
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
# Google Sheet upload
# ------------------------------
# def upload_to_gsheet(df):
#     key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
#     worksheet = client_gsheets.open_by_key(key).get_worksheet(0)
#     rows = df.astype(str).fillna("").values.tolist()
#     worksheet.append_rows(rows, value_input_option="USER_ENTERED")
#     print(f"✅ Uploaded {len(df)} rows to Google Sheet!")

def upload_to_gsheet(df, sheet_title="Results"):
    if not RESULTS_SHEET_URL or "/d/" not in RESULTS_SHEET_URL:
        raise RuntimeError("RESULTS_SHEET_URL is missing/invalid in secrets.")

    key = RESULTS_SHEET_URL.split("/d/")[1].split("/")[0]
    sh = client_gsheets.open_by_key(key)

    # get or create worksheet
    try:
        ws = sh.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_title, rows=1000, cols=26)

    # write header if empty
    if len(ws.get_all_values()) == 0:
        ws.append_row(list(df.columns), value_input_option="USER_ENTERED")

    # append in chunks
    rows = df.astype(str).fillna("").values.tolist()
    CHUNK = 300
    for i in range(0, len(rows), CHUNK):
        ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")
    print(f"✅ Uploaded {len(df)} rows to Google Sheet (worksheet: {sheet_title}).")


import requests

# ------------------------------
# API Upload (optional)
# ------------------------------
API_URL = "https://example.com/api/mct-media"  # fake endpoint
API_KEY = "your_api_key_here"  # ⬅️ Optional if your API requires auth headers

def upload_to_api(df):
    """
    TEST MODE: Simulates sending analyzed data to an API.
    Prints JSON payload sample and total rows to verify correctness.
    """
    try:
        payload = df.to_dict(orient="records")
        print("\n====================== 🧪 TEST API UPLOAD PREVIEW ======================\n")
        print(f"API_URL: {API_URL}")
        print(f"Rows to send: {len(payload)}")
        print("\n📦 Sample payload (first 2 rows):\n")
        print(json.dumps(payload[:2], indent=2, ensure_ascii=False))
        print("\n=======================================================================\n")
        print("✅ TEST SUCCESS — Data prepared correctly for API upload.\n")
        print("👉 (No actual network request was made.)\n")
    except Exception as e:
        print(f"❌ Error preparing data for API upload: {e}")


# ------------------------------
# MAIN EXECUTION
# ------------------------------
if __name__ == "__main__":
    df = fetch_rss()
    print(f"✅ Collected {len(df)} articles from {len(FEEDS)} sources.")

    df["Detected Themes"] = df["Content"].apply(detect_themes)
    df["AI Themes"] = df.apply(lambda r: ai_classify_themes(r["Content"]) if not r["Detected Themes"] else [], axis=1)
    df["All Themes"] = df.apply(
        lambda r: ", ".join(set(r["Detected Themes"] + r["AI Themes"])) if (r["Detected Themes"] or r["AI Themes"]) else "—",
        axis=1
    )

    # ✅ Detect sentiment before using it in df_final
    df["Sentiment"] = df["Content"].apply(detect_sentiment)

    # ------------------------------
    # Determine Media Sector Impact
    # ------------------------------

    # ------------------------------
    def determine_media_impact(row):
        """
        Determines if a news item affects the media sector.
        Logic:
        - Directly affects media if theme involves media freedom, journalist safety,
        media economy, or violations.
        - Indirect/neutral if theme is social, political, or public sentiment only.
        - If unclear, use AI to judge based on context.
        """
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

        # Optional: AI refinement
        try:
            prompt = f"""
            You are a Tanzanian media analyst. 
            Does the following text have a *direct impact* on the media sector
            (like on journalists, media economy, freedom of expression),
            or is it *indirect* or *unrelated*?
            Answer with one of:
            - "Direct Impact on Media Sector"
            - "Indirect / Contextual Impact"
            - "No Direct Impact"
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

    df["Media Sector Impact"] = df.apply(determine_media_impact, axis=1)


    # df_final = df[["Platform", "Content", "Link", "Date", "All Themes", "Sentiment"]]
    df_final = df[["Platform", "Content", "Link", "Date", "All Themes", "Sentiment", "Media Sector Impact"]]

    # Pretty print results
    print("\n====================== MCT MEDIA MONITORING RESULTS ======================\n")
    for i, row in df_final.head(10).iterrows():
        print(f"📰 {i+1}. Platform: {row['Platform']}")
        print(f"   📅 Date: {row['Date']}")
        snippet = (row['Content'][:160] + "...") if len(row['Content']) > 160 else row['Content']
        print(f"   ✍️  Content: {snippet}")
        print(f"   🏷️  Themes: {row['All Themes']}")
        print(f"   💬 Sentiment: {row['Sentiment']}")
        print(f"   🧩 Media Impact: {row['Media Sector Impact']}\n")
    print("==========================================================================\n")

    # Upload results to Google Sheet
    upload_to_gsheet(df_final, sheet_title="Results")
    # upload_to_api(df_final)
    # upload_to_gsheet(df_final)
    # upload_to_api(df_final)


