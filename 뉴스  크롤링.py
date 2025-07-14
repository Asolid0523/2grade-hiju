import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pykiwoom.kiwoom import *
from PyQt5.QtWidgets import QApplication
import sys
import time
from gnews import GNews
from trafilatura import fetch_url, extract
import random

# --- [설정] ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CREDS_FILE = "C:\\Users\\oh200\\anaconda3\\envs\\hiju\\dirlqnswhxk-5f085d7fd949.json"
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1TNXyz5QbxY4rvrw-MxiryqqvwtrvabI2lS_QFp5xEpU/edit?usp=sharing'

STOCK_MAX = 200  # 최대 종목수
NEWS_MAX = 20    # 종목당 뉴스 최대 수

def get_news_sheet(client):
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    try:
        ws = spreadsheet.worksheet("뉴스크롤링")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="뉴스크롤링", rows="1000", cols=str(NEWS_MAX + 2))
    return ws

def fetch_kr_stocks(max_count=200):
    print("키움증권에서 종목코드/명 불러오는 중...")
    if not QApplication.instance():
        app = QApplication(sys.argv)
    kiwoom = Kiwoom()
    kiwoom.CommConnect(block=True)
    codes = kiwoom.GetCodeListByMarket('0') + kiwoom.GetCodeListByMarket('10')
    codes = codes[:max_count]
    code_list = []
    name_list = []
    for code in codes:
        name = kiwoom.GetMasterCodeName(code)
        code_list.append(code)
        name_list.append(name)
    print(f"종목 {len(code_list)}개 추출 완료")
    return code_list, name_list

def crawl_news_list(stock_name, max_news=10):
    news_titles = []
    google_news = GNews(language='ko', country='KR')
    try:
        news_items = google_news.get_news(stock_name)
    except Exception as e:
        print(f"[에러] GNews get_news 실패: {stock_name}, 원인: {e}")
        return ["GNews 수집 오류"] + [""] * (max_news-1)
    for i in range(max_news):
        try:
            item = news_items[i]
        except Exception:
            news_titles.append("")  # 뉴스가 부족할 때 빈칸
            continue
        try:
            downloaded = fetch_url(item['url'])
            if downloaded:
                main_content = extract(downloaded, include_comments=False, include_tables=False)
                if main_content and len(main_content) > 20:
                    text = item['title'] + " / " + main_content.strip().replace('\n', ' ')[:300]
                else:
                    text = item['title']
            else:
                text = item['title']
        except Exception as e:
            print(f"[에러] '{stock_name}' 뉴스 크롤링 실패 (제목: {item.get('title','')[:30]}...): {e}")
            text = f"뉴스 수집 실패: {e}"
        news_titles.append(text)
        time.sleep(random.uniform(0.4, 0.9))
    return news_titles

def main():
    print("프로그램 시작")
    # 1. 구글 시트 연결 (뉴스크롤링 시트)
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPES)
    client = gspread.authorize(creds)
    news_sheet = get_news_sheet(client)

    # 2. 종목코드/명 추출
    code_list, name_list = fetch_kr_stocks(STOCK_MAX)
    if not code_list or not name_list:
        print("❗️ 키움 OpenAPI에서 종목 데이터를 가져오지 못했습니다.")
        sys.exit(1)

    print(f"종목코드/명 {len(code_list)}개 추출 완료. 시트에 저장...")

    # 3. 뉴스크롤링 시트에 (A열: 코드, B열: 이름, C~: 뉴스)
    news_sheet.clear()
    rows = []
    for idx in range(len(code_list)):
        row = [code_list[idx], name_list[idx]]
        row += [""] * NEWS_MAX
        rows.append(row)
    news_sheet.update("A1", [["종목코드", "종목명"] + [f"뉴스{i+1}" for i in range(NEWS_MAX)]])
    news_sheet.update(f"A2", rows)
    print("시트 초기화 및 코드/명 저장 완료, 뉴스 크롤링 시작...")

    # 4. 각 종목별 뉴스 크롤링 & 누적
    cell_updates = []
    for idx, stock_name in enumerate(name_list):
        print(f"[{idx + 1}/{len(name_list)}] '{stock_name}' 뉴스 수집 중...")
        news_list = crawl_news_list(stock_name, NEWS_MAX)
        # 각 종목의 A(idx+2), C~에 뉴스
        cell_updates.append((idx+2, news_list))
        print(f"[{idx + 1}] '{stock_name}' 완료.")

    # 5. 시트에 뉴스 본문 한 번에 업데이트
    for row_idx, news_list in cell_updates:
        news_sheet.update(
            f"C{row_idx}:V{row_idx}",
            [news_list]
        )

    print("✅ 모든 뉴스 크롤링/시트 저장 완료")

if __name__ == '__main__':
    main()
