import platform
import gspread
import pandas as pd
import datetime
import time
from PyQt5.QtWidgets import QApplication
from pykiwoom.kiwoom import *
from oauth2client.service_account import ServiceAccountCredentials

print(platform.architecture())

# ===== [설정] =====
SHEET_URL = "https://docs.google.com/spreadsheets/d/1TNXyz5QbxY4rvrw-MxiryqqvwtrvabI2lS_QFp5xEpU/edit"
KEY_PATH = "C:\\Users\\oh200\\anaconda3\\envs\\hiju\\dirlqnswhxk-5f085d7fd949.json"
LIMIT = 200  # 최대 200개 종목
UPDATE_INTERVAL = 180  # 1분 주기

FIXED_SHEET_NAMES = ["통계분석", "통계데이터", "뉴스크롤링", "명령", "매매내역", "통장정보"]

# ===== [Google Sheets 연결] =====
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_PATH, scope)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL)

def get_or_create_today_ws(today_name, ws_list):
    for ws in ws_list:
        if ws.title == today_name:
            return ws
    try:
        ws = sheet.add_worksheet(title=today_name, rows="500", cols="16")
        print(f"✅ '{today_name}' 시트 새로 생성")
        return ws
    except Exception as e:
        print(f"❌ 시트 생성 오류: {e}")
        return sheet.worksheet(today_name)

# ===== 시트 정렬 함수 =====
def reorder_sheets_with_fixed_last(sheet, fixed_names=None):
    if fixed_names is None:
        fixed_names = FIXED_SHEET_NAMES
    worksheets = sheet.worksheets()
    date_ws = []
    fixed_ws = []
    for ws in worksheets:
        if ws.title in fixed_names:
            fixed_ws.append(ws)
        else:
            if ws.title.count('-') == 2:
                try:
                    dt = datetime.datetime.strptime(ws.title, "%Y-%m-%d")
                    date_ws.append((ws, dt))
                except:
                    date_ws.append((ws, None))
            else:
                date_ws.append((ws, None))
    date_ws.sort(key=lambda x: (x[1] is None, x[1] or ws.title))
    for idx, (ws, _) in enumerate(date_ws):
        ws.update_index(idx)
        time.sleep(0.2)
    for i, ws in enumerate(fixed_ws):
        ws.update_index(len(date_ws) + i)
        time.sleep(0.2)
    print("✅ 날짜/기타 시트는 앞, 고정 시트 6개는 항상 맨 뒤로 정렬 완료.")

# ===== PyQt5 및 키움 연결 =====
app = QApplication([])
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)
print("✅ 키움 API 로그인 완료")

# ===== 기준 종목 선정 =====
codes = kiwoom.GetCodeListByMarket("0")[:LIMIT]
names = [kiwoom.GetMasterCodeName(code) for code in codes]

def get_basic_info(kiwoom, code):
    try:
        df = kiwoom.block_request("opt10001",
                                  종목코드=code,
                                  output="주식기본정보",
                                  next=0)
        row = df.iloc[0] if not df.empty else {}
        return [
            code,
            kiwoom.GetMasterCodeName(code),
            row.get("현재가", ""),
            row.get("시가", ""),
            row.get("고가", ""),
            row.get("저가", ""),
            row.get("전일가", ""),
            row.get("거래량", ""),
            row.get("등락율"),
            row.get("PER", ""),
            row.get("PBR", ""),
            row.get("ROE", ""),
            row.get("EPS", ""),
            row.get("BPS", "")
        ]
    except Exception as e:
        print(f"[{code}] opt10001 조회 오류: {e}")
        return [code, kiwoom.GetMasterCodeName(code)] + [""] * 12

columns = [
    "종목코드", "종목명", "현재가", "시가", "고가", "저가", "전일가", "거래량", "등락률",
    "PER", "PBR", "ROE", "EPS", "BPS"
]

print("📡 opt10001 방식 오늘자 시트에 3분 주기 실시간 업데이트 시작!")

while True:
    today_name = datetime.datetime.now().strftime("%Y-%m-%d")
    ws_list = sheet.worksheets()
    ws_today = get_or_create_today_ws(today_name, ws_list)

    rows = [columns]
    for code in codes:
        info = get_basic_info(kiwoom, code)
        rows.append(info)
        time.sleep(0.3)  # 키움 TR 과도요청 방지

    try:
        ws_today.clear()
        ws_today.update([columns] + rows[1:])
        print(f"✅ {datetime.datetime.now()} [{today_name}] opt10001 시트 실시간 업데이트 완료 ({len(rows)-1} 종목)")
        reorder_sheets_with_fixed_last(sheet)
    except Exception as e:
        print(f"❌ 구글 시트 업데이트 실패: {e}")

    time.sleep(UPDATE_INTERVAL)
