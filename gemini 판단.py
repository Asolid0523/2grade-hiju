import gspread
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai
import re

SHEET_URL = "https://docs.google.com/spreadsheets/d/1TNXyz5QbxY4rvrw-MxiryqqvwtrvabI2lS_QFp5xEpU/edit"
JSON_KEY_PATH = "C:\\Users\\oh200\\anaconda3\\envs\\hiju\\dirlqnswhxk-5f085d7fd949.json"
GEMINI_API_KEY = "AIzaSyCtMUR8Ykq3V3MX2BBqPtL88Zf5rSjZYFw"

genai.configure(api_key=GEMINI_API_KEY)

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_url(SHEET_URL)

def get_sheet_by_title(title):
    for ws in spreadsheet.worksheets():
        if ws.title == title:
            return ws
    return spreadsheet.add_worksheet(title=title, rows="1000", cols="50")

def get_stock_info_from_first_sheet():
    ws = spreadsheet.get_worksheet(0)
    rows = ws.get_all_values()
    stock_info = {}
    header = rows[0]
    idx = {col: header.index(col) for col in ['종목코드', '종목명', '시가', '고가', '저가', '종가', '거래량', '전일종가', '전일대비', '등락률']}
    for row in rows[1:]:
        try:
            code = row[idx['종목코드']].strip()
            stock_info[code] = {
                'name': row[idx['종목명']].strip(),
                'open': row[idx['시가']].strip(),
                'high': row[idx['고가']].strip(),
                'low': row[idx['저가']].strip(),
                'close': row[idx['종가']].strip(),
                'volume': row[idx['거래량']].strip(),
                'prev_close': row[idx['전일종가']].strip(),
                'change': row[idx['전일대비']].strip(),
                'rate': row[idx['등락률']].strip()
            }
        except:
            continue
    return stock_info

def get_news_info():
    try:
        ws = get_sheet_by_title("뉴스크롤링")
        rows = ws.get_all_values()
        news_dict = {}
        for row in rows[1:]:
            if len(row) >= 3:
                code = row[0].strip()
                news = [x.strip() for x in row[2:] if x.strip()]
                news_dict[code] = news
        return news_dict
    except Exception:
        return {}

def get_stats_info():
    try:
        ws = get_sheet_by_title("통계데이터")
        rows = ws.get_all_values()
        stats_dict = {}
        header = rows[0]
        for row in rows[1:]:
            code = row[0].strip()
            stats_dict[code] = {header[i]: row[i].strip() if i < len(row) else "" for i in range(1, len(header))}
        return stats_dict
    except Exception:
        return {}

def gemini_judge(stock_info, news_info, stats_info):
    prompt = f"""
아래는 한국 주식 종목별 데이터, 뉴스, 통계데이터입니다.

# 주식 데이터(딕셔너리: 시가, 고가, 저가, 종가, 거래량, 전일종가, 전일대비, 등락률 포함)
{stock_info}

# 뉴스 데이터(구글 시트 '뉴스크롤링' 시트 전체)
{news_info}

# 통계 데이터(구글 시트 '통계데이터' 시트 전체)
{stats_info}

위 모든 자료를 반드시 종합하여, 각 종목별로 '매수', '매도', '관망' 중 딱 하나만 판단하세요.
파이썬 딕셔너리 한 줄만 출력하세요. 예시: {{'005930':'매수','000660':'관망'}}
딕셔너리 외 문장, 설명, 코드블록 등 추가 출력 절대 금지!
"""
    model = genai.GenerativeModel("gemini-1.5-flash-latest")
    response = model.generate_content(prompt)
    print("Gemini 응답:", response.text)
    match = re.search(r"\{.*\}", response.text.replace('\n', '').replace(' ', ''))
    if match:
        try:
            d = eval(match.group())
            return {str(k).strip(): v for k, v in d.items()}
        except Exception as e:
            print("Gemini 응답 파싱 실패:", e)
            return {}
    else:
        print("Gemini 응답 파싱 실패: dict 형태 미검출")
        return {}

def write_command_sheet(decision_dict, stock_info):
    ws = get_sheet_by_title("명령")
    ws.clear()
    ws.append_row(['종목코드', '종목명', '명령', '수량', '가격', '주문상태'])
    rows_to_append = []
    for code, cmd in decision_dict.items():
        if code not in stock_info:
            print(f"stock_info에 '{code}' 없음, keys: {list(stock_info.keys())}")
            continue
        name = stock_info[code]['name']
        price = stock_info[code]['close']
        rows_to_append.append([code, name, cmd, '1', price, '대기'])
    if rows_to_append:
        ws.append_rows(rows_to_append)

if __name__ == "__main__":
    stock_info = get_stock_info_from_first_sheet()
    news_info = get_news_info()
    stats_info = get_stats_info()
    print("DEBUG stock_info:", stock_info)
    print("DEBUG news_info:", news_info)
    print("DEBUG stats_info:", stats_info)
    decision = gemini_judge(stock_info, news_info, stats_info)
    print("Gemini 최종 판단:", decision)
    write_command_sheet(decision, stock_info)
