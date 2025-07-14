import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pykiwoom.kiwoom import *
from PyQt5.QtWidgets import QApplication
import time
import datetime

SHEET_URL = "https://docs.google.com/spreadsheets/d/1TNXyz5QbxY4rvrw-MxiryqqvwtrvabI2lS_QFp5xEpU/edit"
JSON_KEY_PATH = "C:\\Users\\oh200\\anaconda3\\envs\\hiju\\dirlqnswhxk-5f085d7fd949.json"

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_url(SHEET_URL)

def safe_int(val):
    try:
        if val is None or val == '':
            return 0
        return int(str(val).replace(",", ""))
    except:
        return 0

def get_sheet_by_title(title, cols=12):
    for ws in spreadsheet.worksheets():
        if ws.title == title:
            return ws
    return spreadsheet.add_worksheet(title=title, rows="1000", cols=str(cols))

def get_deposit(kiwoom, acc_no, passwd):
    try:
        output = kiwoom.block_request(
            "opw00001",
            계좌번호=acc_no,
            비밀번호=passwd,
            비밀번호입력매체구분="00",
            조회구분=2,
            output="예수금상세현황",
            next=0
        )
        if len(output) > 0 and "예수금" in output.columns:
            return safe_int(output["예수금"].iloc[0])
        else:
            return 0
    except Exception as e:
        print(f"예수금 조회 실패: {e}")
        return 0

def get_holdings(kiwoom, acc_no, passwd):
    try:
        output = kiwoom.block_request(
            "opw00018",
            계좌번호=acc_no,
            비밀번호=passwd,
            비밀번호입력매체구분="00",
            조회구분=2,
            output="계좌평가현황",
            next=0
        )
        holdings = []
        for idx, row in output.iterrows():
            holdings.append({
                '종목코드': row.get('종목번호', ''),
                '종목명': row.get('종목명', ''),
                '보유수량': safe_int(row.get('보유수량', 0)),
                '매입가': safe_int(row.get('매입가', 0)),
                '평가금액': safe_int(row.get('평가금액', 0))
            })
        return holdings
    except Exception as e:
        print(f"보유주식 조회 실패: {e}")
        return []

def update_account_sheet(kiwoom, acc_no, passwd, account_ws):
    account_ws.clear()
    account_ws.append_row(['계좌번호', acc_no])
    account_ws.append_row(['종목코드', '종목명', '보유수량', '매입가', '평가금액'])
    holdings = get_holdings(kiwoom, acc_no, passwd)
    total_eval = 0
    for stock in holdings:
        account_ws.append_row([
            stock['종목코드'], stock['종목명'], str(stock['보유수량']),
            str(stock['매입가']), str(stock['평가금액'])
        ])
        total_eval += stock['평가금액']
    cash = get_deposit(kiwoom, acc_no, passwd)
    account_ws.append_row(['예수금', '', '', '', str(cash)])
    account_ws.append_row(['총평가금액', '', '', '', str(total_eval)])
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 통장 정보 업데이트 완료: 계좌번호={acc_no}, 예수금={cash}, 보유종목={len(holdings)}, 총평가금액={total_eval}")

def main_account_loop():
    app = QApplication([])
    kiwoom = Kiwoom()
    kiwoom.CommConnect(block=True)
    print("✅ 키움 API 로그인 완료")
    acc_nos = kiwoom.GetLoginInfo("ACCNO")
    acc_no = "8106875411"   # 예시: 본인 모의투자 계좌번호로 반드시 교체
    kiwoom_pw = "0000"      # 실제 비밀번호로 변경

    account_ws = get_sheet_by_title("통장정보", cols=6)
    while True:
        update_account_sheet(kiwoom, acc_no, kiwoom_pw, account_ws)
        time.sleep(60)

if __name__ == "__main__":
    main_account_loop()
