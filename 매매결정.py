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

# ---- 예수금 조회 함수 (TR 사용) ----
def get_deposit(kiwoom, acc_no, passwd):
    try:
        df = kiwoom.block_request(
            "opw00001",
            계좌번호=acc_no,
            비밀번호=passwd,
            비밀번호입력매체구분="00",
            조회구분=2,
            output="예수금상세현황",
            next=0
        )
        if len(df) > 0 and "예수금" in df.columns:
            return int(str(df["예수금"].iloc[0]).replace(",", ""))
        else:
            return 0
    except Exception as e:
        print(f"예수금 조회 실패: {e}")
        return 0

def get_sheet_by_title(title, cols=12):
    for ws in spreadsheet.worksheets():
        if ws.title == title:
            return ws
    return spreadsheet.add_worksheet(title=title, rows="1000", cols=str(cols))

# --- 통장정보 시트 업데이트 ---
def update_account_sheet(kiwoom, acc_no, passwd, account_ws):
    account_ws.clear()
    # 1. 계좌/예수금 등 요약 정보
    cash = get_deposit(kiwoom, acc_no, passwd)
    stocks = []
    try:
        stocks = kiwoom.GetHoldings() if hasattr(kiwoom, "GetHoldings") else []
    except Exception as e:
        print(f"보유주식 정보 조회 오류: {e}")

    total_eval = 0
    for stock in stocks:
        try:
            eval_amt = int(str(stock.get('평가금액', 0)).replace(",", ""))
        except Exception:
            eval_amt = 0
        total_eval += eval_amt

    account_ws.append_row(["계좌번호", acc_no, "예수금", cash, "총평가금액", total_eval, "총보유종목", len(stocks)])

    # 2. 헤더
    account_ws.append_row(['종목코드', '종목명', '보유수량', '매입가', '평가금액'])

    # 3. 보유주식 상세
    for stock in stocks:
        # 숫자 변환 안전 처리
        try:
            qty = int(str(stock.get('보유수량', 0)).replace(",", "")) if stock.get('보유수량', 0) else 0
        except Exception:
            qty = 0
        try:
            price = int(str(stock.get('매입가', 0)).replace(",", "")) if stock.get('매입가', 0) else 0
        except Exception:
            price = 0
        try:
            eval_amt = int(str(stock.get('평가금액', 0)).replace(",", "")) if stock.get('평가금액', 0) else 0
        except Exception:
            eval_amt = 0

        account_ws.append_row([
            stock.get('종목코드', ''), stock.get('종목명', ''), qty, price, eval_amt
        ])

    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 통장 정보 업데이트 완료: 계좌번호={acc_no}, 예수금={cash}, 보유종목={len(stocks)}, 총평가금액={total_eval}")

def execute_commands(kiwoom, acc_no, command_ws, trade_ws, account_ws):
    # 1. 보유 종목 및 잔고 파싱
    holdings = {}
    account_values = account_ws.get_all_values()[2:]  # 0:요약, 1:헤더, 2~:상세
    for row in account_values:
        if row[0] and row[0] != "예수금":
            try:
                qty = int(str(row[2]).replace(",", "")) if row[2] else 0
            except Exception:
                qty = 0
            holdings[row[0]] = qty
    # 예수금
    cash_row = account_ws.get_all_values()[0]
    try:
        cash = int(str(cash_row[3]).replace(",", "")) if len(cash_row) > 3 and str(cash_row[3]).isdigit() else 0
    except Exception:
        cash = 0

    # 2. 명령 파싱
    commands = command_ws.get_all_values()[1:]
    for row in commands:
        if len(row) < 6: continue
        code, name, action, qty_str, price_str, status = row
        try:
            qty = int(str(qty_str).replace(",", "")) if qty_str else 0
        except Exception:
            qty = 0
        try:
            price = int(str(price_str).replace(",", "")) if price_str else 0
        except Exception:
            price = 0
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log = ""
        order_no = ""
        result = "실패"

        # -- 매수 --
        if action == "매수":
            if code in holdings and holdings[code] > 0:
                log = f"보유중이므로 매수 스킵 (보유수량: {holdings[code]})"
                order_no = ""
                result = "중복매수"
            else:
                max_buy = int(cash * 0.1)
                buy_qty = max(1, max_buy // (price if price > 0 else 1))
                buy_qty = min(buy_qty, qty)
                if buy_qty <= 0:
                    log = f"자본 부족. 예수금: {cash}, 희망가: {price}"
                    order_no = ""
                    result = "자본부족"
                else:
                    try:
                        ret = kiwoom.SendOrder("매수", "0101", acc_no, 1, code, buy_qty, price, "03", "")
                        order_no = str(ret)
                        result = "성공"
                        log = f"매수 {buy_qty}주, 단가 {price}, 주문번호:{order_no}"
                    except Exception as e:
                        log = f"매수오류:{e}"
                        order_no = ""
                        result = "오류"
            trade_ws.append_row([now, code, name, action, result, order_no, log])

        # -- 매도 --
        elif action == "매도":
            sell_qty = min(qty, holdings.get(code, 0))
            if sell_qty > 0:
                try:
                    ret = kiwoom.SendOrder("매도", "0101", acc_no, 2, code, sell_qty, price, "03", "")
                    order_no = str(ret)
                    result = "성공"
                    log = f"매도 {sell_qty}주, 단가 {price}, 주문번호:{order_no}"
                    trade_ws.append_row([now, code, name, action, result, order_no, log])
                except Exception as e:
                    log = f"매도오류:{e}"
                    order_no = ""
                    result = "오류"
                    trade_ws.append_row([now, code, name, action, result, order_no, log])
            else:
                pass
        # -- 관망/기타 --
        else:
            continue

def main_trade_loop():
    app = QApplication([])
    kiwoom = Kiwoom()
    kiwoom.CommConnect(block=True)
    print("딱좋노이기야")
    print("✅ 키움 API 로그인 완료")
    acc_nos = kiwoom.GetLoginInfo("ACCNO")
    acc_no = acc_nos[0] if isinstance(acc_nos, list) else acc_nos.split(';')[0]
    passwd = "0000"  # 본인 모의투자 비번(반드시 맞게 입력)

    while True:
        command_ws = get_sheet_by_title("명령", cols=6)
        trade_ws = get_sheet_by_title("매매내역", cols=8)
        account_ws = get_sheet_by_title("통장정보", cols=8)

        # 1. 통장정보(잔고) 최신화
        update_account_sheet(kiwoom, acc_no, passwd, account_ws)
        # 2. 명령 실행
        execute_commands(kiwoom, acc_no, command_ws, trade_ws, account_ws)
        # 3. 명령 시트 초기화 (헤더)
        command_ws.clear()
        command_ws.append_row(['종목코드', '종목명', '명령', '수량', '가격', '주문상태'])
        # 4. 60초 대기
        time.sleep(60)

if __name__ == "__main__":
    main_trade_loop()
