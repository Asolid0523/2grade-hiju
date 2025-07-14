import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import time

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CREDS_FILE = "C:\\Users\\oh200\\anaconda3\\envs\\hiju\\dirlqnswhxk-5f085d7fd949.json"
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1TNXyz5QbxY4rvrw-MxiryqqvwtrvabI2lS_QFp5xEpU/edit?usp=sharing'

indicator_names = [
    "상대강도지수", "단순상대강도지수", "15일이동평균선", "25일이동평균선",
    "스토캐스틱 %K", "스토캐스틱 %D",
    "볼린저밴드 상단", "볼린저밴드 중심선", "볼린저밴드 하단",
    "평균진폭범위", "누적거래량지표", "15일표준편차", "25일표준편차",
    "15일평균거래량"
]

def read_ohlcv_by_sheets(client):
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    ws_list = spreadsheet.worksheets()
    data_ws_list = ws_list[:-4]  # 뒤에서 4개 시트 제외
    all_records = []
    for idx, ws in enumerate(reversed(data_ws_list)):
        date = ws.title
        try:
            values = ws.get_all_values()
        except Exception as e:
            print(f"[{date}] get_all_values() 실패: {e}")
            time.sleep(3)
            continue
        if not values or len(values) < 2:
            time.sleep(1.5)
            continue
        header = values[0]
        col_idx = {name: i for i, name in enumerate(header)}
        for row in values[1:]:
            if len(row) < 7:
                continue
            try:
                record = {
                    '종목코드': row[col_idx['종목코드']],
                    '종목명': row[col_idx['종목명']],
                    '날짜': date,
                    '시가': row[col_idx['시가']],
                    '고가': row[col_idx['고가']],
                    '저가': row[col_idx['저가']],
                    '종가': row[col_idx['종가']],
                    '거래량': row[col_idx['거래량']]
                }
                all_records.append(record)
            except Exception as e:
                continue
        time.sleep(1.5)  # 쿼터 초과 방지
    return pd.DataFrame(all_records)

def calc_indicators_for_group(df_group):
    df = df_group.sort_values('날짜')
    try:
        for col in ['시가', '고가', '저가', '종가', '거래량']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        result = {}
        result["15일이동평균선"] = df['종가'].rolling(window=15).mean().iloc[-1] if len(df) >= 15 else 0
        result["25일이동평균선"] = df['종가'].rolling(window=25).mean().iloc[-1] if len(df) >= 25 else 0

        # RSI (Wilder)
        delta = df['종가'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        result["상대강도지수"] = rsi.iloc[-1] if not rsi.isna().all() else 0

        # 단순 RSI
        avg_gain_simple = gain.rolling(window=14).mean()
        avg_loss_simple = loss.rolling(window=14).mean()
        rs_simple = avg_gain_simple / avg_loss_simple
        rsi_simple = 100 - (100 / (1 + rs_simple))
        result["단순상대강도지수"] = rsi_simple.iloc[-1] if not rsi_simple.isna().all() else 0

        # Stochastic
        low14 = df['저가'].rolling(window=14).min()
        high14 = df['고가'].rolling(window=14).max()
        percent_k = ((df['종가'] - low14) / (high14 - low14) * 100)
        percent_d = percent_k.rolling(window=3).mean()
        result["스토캐스틱 %K"] = percent_k.iloc[-1] if len(percent_k.dropna()) > 0 else 0
        result["스토캐스틱 %D"] = percent_d.iloc[-1] if len(percent_d.dropna()) > 0 else 0

        # 볼린저밴드
        ma20 = df['종가'].rolling(window=20).mean()
        std20 = df['종가'].rolling(window=20).std()
        result["볼린저밴드 중심선"] = ma20.iloc[-1] if len(ma20.dropna()) > 0 else 0
        result["볼린저밴드 상단"] = (ma20 + 2 * std20).iloc[-1] if len(ma20.dropna()) > 0 else 0
        result["볼린저밴드 하단"] = (ma20 - 2 * std20).iloc[-1] if len(ma20.dropna()) > 0 else 0

        # ATR(14)
        high_low = df['고가'] - df['저가']
        high_close = (df['고가'] - df['종가'].shift()).abs()
        low_close = (df['저가'] - df['종가'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(window=14).mean()
        result["평균진폭범위"] = atr.iloc[-1] if len(atr.dropna()) > 0 else 0

        # OBV
        obv = [0]
        for i in range(1, len(df)):
            if df['종가'].iloc[i] > df['종가'].iloc[i-1]:
                obv.append(obv[-1] + df['거래량'].iloc[i])
            elif df['종가'].iloc[i] < df['종가'].iloc[i-1]:
                obv.append(obv[-1] - df['거래량'].iloc[i])
            else:
                obv.append(obv[-1])
        result["누적거래량지표"] = obv[-1]

        # 표준편차
        result["15일표준편차"] = df['종가'].rolling(window=15).std().iloc[-1] if len(df) >= 15 else 0
        result["25일표준편차"] = df['종가'].rolling(window=25).std().iloc[-1] if len(df) >= 25 else 0

        # 15일 평균거래량
        result["15일평균거래량"] = df['거래량'].rolling(window=15).mean().iloc[-1] if len(df) >= 15 else 0

        for k in result:
            if pd.isna(result[k]) or np.isinf(result[k]):
                result[k] = 0
        return result
    except Exception as e:
        print(f"[지표계산오류] {e}")
        return {k:0 for k in indicator_names}

def main():
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPES)
    client = gspread.authorize(creds)
    df = read_ohlcv_by_sheets(client)
    # 종목별 최신 지표 계산
    results = []
    for code, df_group in df.groupby('종목코드'):
        name = df_group['종목명'].iloc[-1] if '종목명' in df_group else code
        indi = calc_indicators_for_group(df_group)
        results.append([name] + [indi.get(k,0) for k in indicator_names])
    cols = ['종목명'] + indicator_names
    df_result = pd.DataFrame(results, columns=cols)
    # '통계데이터' 시트에 저장
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    stat_sheet = None
    for ws in spreadsheet.worksheets():
        if ws.title == "통계데이터":
            stat_sheet = ws
            break
    if stat_sheet is None:
        stat_sheet = spreadsheet.add_worksheet(title="통계데이터", rows="1000", cols=str(len(cols)))
    stat_sheet.clear()
    stat_sheet.update([cols] + df_result.values.tolist())
    print("✅ 지표 계산 후 '통계데이터' 시트에 저장 완료")

if __name__ == '__main__':
    main()
