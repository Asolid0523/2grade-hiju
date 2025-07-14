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
MAX_ROWS = 200
DAYS = 30
SLEEP_SEC = 0.5   # 슬립 0.5초로 변경

# ===== [PyQt5, 키움 API 연결] =====
app = QApplication([])
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)
print("✅ 키움 API 로그인 완료")

# ===== [구글 시트 연결] =====
gscope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_PATH, gscope)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL)

# ===== 유틸 함수: 오늘은 빼고 과거 n개 영업일 반환 =====
def get_past_business_days(n, end_date=None):
    days = []
    # today에서 하루 빼고(=어제까지) 시작
    today = datetime.datetime.now() if end_date is None else end_date
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    today -= datetime.timedelta(days=1)
    delta = datetime.timedelta(days=1)
    while len(days) < n:
        if today.weekday() < 5:
            days.append(today)
        today -= delta
    return sorted(days)

# ===== 시트 정렬: 고정 시트 6개는 항상 맨 뒤로 =====
def reorder_sheets_with_fixed_last(sheet, fixed_names=None):
    if fixed_names is None:
        fixed_names = ["통계분석", "통계데이터", "뉴스크롤링", "명령", "매매내역", "통장정보"]
    worksheets = sheet.worksheets()
    date_ws = []
    fixed_ws = []
    for ws in worksheets:
        if ws.title in fixed_names:
            fixed_ws.append(ws)
        else:
            # 날짜시트는 "yyyy-mm-dd" 형식만 날짜순 정렬 (그 외 시트는 생성 순서대로 남김)
            if ws.title.count('-') == 2:
                try:
                    dt = datetime.datetime.strptime(ws.title, "%Y-%m-%d")
                    date_ws.append((ws, dt))
                except:
                    date_ws.append((ws, None))  # 날짜 변환 실패시 그냥 남김
            else:
                date_ws.append((ws, None))  # 날짜형식 아니면 그냥 남김
    # 날짜 시트는 날짜순, 날짜 인식 안된건 앞에 둠
    date_ws.sort(key=lambda x: (x[1] is None, x[1] or ws.title))
    # 날짜/기타 시트 먼저, 고정시트 6개 마지막에 배치
    for idx, (ws, _) in enumerate(date_ws):
        ws.update_index(idx)
        time.sleep(0.2)
    for i, ws in enumerate(fixed_ws):
        ws.update_index(len(date_ws) + i)
        time.sleep(0.2)
    print("✅ 날짜/기타 시트는 앞, 고정 시트 6개는 항상 맨 뒤로 정렬 완료.")

# ===== 중요 컬럼 결측(빈칸) 검사 =====
def is_sheet_data_complete(values, price_col_idx=5, min_row=MAX_ROWS//2):
    if len(values) <= 1:
        return False
    cnt = 0
    for row in values[1:]:
        if len(row) > price_col_idx and row[price_col_idx] not in ["", "0", "0.0"]:
            cnt += 1
    return cnt >= min_row

dates = get_past_business_days(DAYS)
kospi_codes = kiwoom.GetCodeListByMarket("0")[:MAX_ROWS]

columns = [
    "종목코드", "종목명",
    "시가", "고가", "저가", "종가", "거래량", "전일종가", "전일대비", "등락률",
    "PER", "PBR", "ROE", "EPS", "BPS", "부채비율"
]

def safe_float(val):
    try:
        return float(str(val).replace(",", "").replace("%", ""))
    except:
        return 0.0

# 컬럼 안전 추출 함수
def get_col(row, *cols):
    for col in cols:
        if col in row:
            return row[col]
    return ""

all_worksheets = sheet.worksheets()
ws_dict = {ws.title: ws for ws in all_worksheets if ws.title.count('-') == 2}

for dt in dates:
    date_str = dt.strftime("%Y-%m-%d")
    ymd = dt.strftime("%Y%m%d")
    print(f"▶️ [{date_str}] 데이터 수집 시작")

    if date_str in ws_dict:
        ws = ws_dict[date_str]
        for trycnt in range(2):
            try:
                values = ws.get_all_values()
                break
            except Exception as e:
                print(f"❗ get_all_values() 실패({e}), {SLEEP_SEC}초 대기 후 재시도")
                time.sleep(SLEEP_SEC)
        if is_sheet_data_complete(values, price_col_idx=5, min_row=MAX_ROWS):
            print(f"⏭️ 시트 '{date_str}' 이미 존재 & 데이터 충분(실제 데이터 행 {MAX_ROWS} 이상). 다음 날짜로")
            continue
        else:
            print(f"📝 시트 '{date_str}' 데이터 미완성(빈칸 포함, 덮어쓰기)")
            ws.clear()
            time.sleep(SLEEP_SEC)
    else:
        ws = sheet.add_worksheet(title=date_str, rows="500", cols=str(len(columns)))
        ws_dict[date_str] = ws
        time.sleep(SLEEP_SEC)

    rows = [columns]
    for code in kospi_codes:
        try:
            name = kiwoom.GetMasterCodeName(code)
            print(f" - {code} opt10081 요청")
            chart = kiwoom.block_request("opt10081",
                                         종목코드=code,
                                         기준일자=ymd,
                                         수정주가구분=1,
                                         output="주식일봉차트조회",
                                         next=0)
            time.sleep(0.5)
            if chart is None or not isinstance(chart, pd.DataFrame) or chart.empty:
                print(f"   → opt10081 {code} empty, 건너뜀")
                continue
            row = chart.iloc[0]
            시가 = safe_float(row.get("시가", 0))
            고가 = safe_float(row.get("고가", 0))
            저가 = safe_float(row.get("저가", 0))
            종가 = safe_float(row.get("현재가", 0))
            거래량 = safe_float(row.get("거래량", 0))
            전일종가 = safe_float(chart.iloc[1]["현재가"]) if len(chart) >= 2 else 종가
            전일대비 = 종가 - 전일종가
            등락률 = round((전일대비 / 전일종가) * 100, 2) if 전일종가 != 0 else 0

            print(f" - {code} opt10001 요청")
            info = kiwoom.block_request("opt10001", 종목코드=code, output="주식기본정보", next=0)
            time.sleep(0.5)
            if isinstance(info, pd.DataFrame) and not info.empty:
                _row = info.iloc[0]
                PER = safe_float(get_col(_row, 'PER'))
                PBR = safe_float(get_col(_row, 'PBR'))
                ROE = safe_float(get_col(_row, 'ROE'))
                EPS = safe_float(get_col(_row, 'EPS'))
                BPS = safe_float(get_col(_row, 'BPS'))
                부채비율 = safe_float(get_col(_row, '부채비율', '부채 비율', 'DebtRatio', 'Debt Ratio'))
            else:
                PER = PBR = ROE = EPS = BPS = 부채비율 = 0.0

            rows.append([
                code, name, 시가, 고가, 저가, 종가, 거래량, 전일종가, 전일대비, 등락률,
                PER, PBR, ROE, EPS, BPS, 부채비율
            ])
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ {date_str} {code} 오류: {e}")
            continue

    try:
        ws.update(values=rows, range_name="A1")
        print(f"✅ [{date_str}] 시트 저장 완료 (종목 {len(rows)-1}개)")
        time.sleep(SLEEP_SEC)
        reorder_sheets_with_fixed_last(sheet)   # <=== 여기서 시트 정렬
        time.sleep(SLEEP_SEC)
    except Exception as e:
        print(f"❌ 시트 저장/정렬 오류: {e}")
        continue

print("모든 날짜 데이터 수집/업데이트 및 시트 정렬 완료!")
