import pandas as pd
import os
import glob
import warnings

# 경고 메시지 무시
warnings.filterwarnings("ignore")

# 1. 설정
BASE_DIR = './data'
NEWS_DIR = os.path.join(BASE_DIR, 'news')
STOCK_DIR = os.path.join(BASE_DIR, 'stock')
MACRO_FILE = os.path.join(BASE_DIR, 'clean', 'daily_macro_with_nan.csv')
OUTPUT_DIR = os.path.join(BASE_DIR, 'final')

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 매핑 설정
file_mapping = {
    'Kia.csv': ['기아'],
    'DoosanEnerbility.csv': ['두산에너빌리티'],
    'SamsungBio.csv': ['삼성바이오로직스'],
    'SamsungElectronics.csv': ['삼성전자'],
    'HanwhaAerospace.csv': ['한화에어로스페이스'],
    'HyundaiMotor.csv': ['현대차'],
    'HDHyundaiHeavy.csv': ['HD현대중공업'],
    'KBFinancial.csv': ['KB금융'],
    'LGES.csv': ['LG에너지솔루션'],
    'SKHynix.csv': ['SK하이닉스']
}

event_keywords = {
    0: {'name': 'E1_EARN', 'keywords': ['실적', '영업이익', '컨센서스', '매출영업이익률', '어닝', '원가']},
    1: {'name': 'E2_ORDER', 'keywords': ['계약', '납품', '발주', '공급', '공급계약', '수주잔고', '납기', '발주']},
    2: {'name': 'E3_POLICY', 'keywords': ['정책', '규제', '정부', '법안', '제도', '행정', '감독', '공공', '당국']},
    3: {'name': 'E4_PRODUCT', 'keywords': ['개발', '기술', '공개', '상용화', '연구', '혁신', '특허']},
    4: {'name': 'E5_CAPEX', 'keywords': ['증설', '공장', '설비투자', '캐파', '신설', '이전', '생산라인']},
    5: {'name': 'E6_MA', 'keywords': ['인수', '합병', 'M&A', '지분', '피인수', '흡수', '분할']},
    6: {'name': 'E7_RISK', 'keywords': ['사고', '중단', '리콜', '결함', '안전사고', '생산중단', '회수', '품질이슈', '화재', '폭발', '벌금', '부도', '논란', '소송', '분쟁', '피해']}
}

def get_event_bitmask(text):
    if not isinstance(text, str): return "0000000"
    bit_list = []
    for i in range(7):
        keywords = event_keywords[i]['keywords']
        is_exist = any(keyword in text for keyword in keywords)
        bit_list.append('1' if is_exist else '0')
    return "".join(bit_list)

# 2. 메인 로직

print(">>> 1. 거시지표 로드 및 전처리")
macro_df = pd.read_csv(MACRO_FILE)
macro_df.columns = [c.lower().strip() for c in macro_df.columns]

if 'kor_3y' in macro_df.columns:
    print("  * 'kor_3y' 컬럼을 제외합니다.")
    macro_df.drop(columns=['kor_3y'], inplace=True)

if 'date' in macro_df.columns:
    macro_df['date'] = pd.to_datetime(macro_df['date'])
    macro_df['date'] = macro_df['date'].dt.normalize()
else:
    print("  ! Error: 거시지표에 date 컬럼이 없습니다.")

macro_columns = [c for c in macro_df.columns if c != 'date']

print(">>> 2. 종목별 처리 시작")

for stock_file, news_keywords in file_mapping.items():
    stock_name = stock_file.replace('.csv', '')
    print(f"\n[{stock_name}] 처리 중...")
    
    # --- A. 주가 데이터 로드 ---
    stock_path = os.path.join(STOCK_DIR, stock_file)
    if not os.path.exists(stock_path):
        print(f"  ! Warning: 파일 없음 - {stock_path}")
        continue
        
    df_stock = pd.read_csv(stock_path)
    
    # 날짜 형식 통일
    if 'Date' in df_stock.columns:
        df_stock.rename(columns={'Date': 'date'}, inplace=True)
        df_stock['date'] = pd.to_datetime(df_stock['date'])
        df_stock['date'] = df_stock['date'].dt.normalize()
    else:
        print("  ! Error: 주가 데이터에 Date 컬럼이 없습니다.")
        continue

    # 주가 변화율 계산 (현재값/이전값 - 1) * 100
    if 'Close' in df_stock.columns:
        # 날짜순 정렬 
        df_stock = df_stock.sort_values('date')
        
        # shift(1)로 이전 행(직전 거래일) 값 가져오기
        df_stock['prev_close'] = df_stock['Close'].shift(1)
        
        # 수익률 계산
        df_stock['y(stock)'] = ((df_stock['Close'] / df_stock['prev_close']) - 1) * 100
        
        # 첫 번째 행(이전 데이터 없음) 제거
        df_stock = df_stock.dropna(subset=['y(stock)'])
        
        # 필요한 컬럼만 선택
        df_stock = df_stock[['date', 'y(stock)']].copy()
    else:
        continue

    # --- B. 뉴스 데이터 로드 ---
    news_files = []
    for nk in news_keywords:
        found = glob.glob(os.path.join(NEWS_DIR, f"*{nk}*.xlsx"))
        found = [f for f in found if not os.path.basename(f).startswith('~$')]
        news_files.extend(found)
    
    df_news_daily = pd.DataFrame()
    df_news_all = pd.DataFrame()

    if news_files:
        news_dfs = []
        for nf in news_files:
            try:
                tmp_df = pd.read_excel(nf, engine='openpyxl')
                news_dfs.append(tmp_df)
            except Exception:
                pass
        
        if news_dfs:
            df_news_all = pd.concat(news_dfs, ignore_index=True)
            df_news_all.columns = [str(c).strip() for c in df_news_all.columns]
            
            # 날짜 컬럼 찾기
            date_col = None
            candidates = ['일자', 'Date', 'date', 'TIME', 'time', '입력일', '작성일', '날짜']
            for col in candidates:
                if col in df_news_all.columns:
                    date_col = col
                    break

            if date_col and '본문' in df_news_all.columns:
                # 날짜 변환 및 시간 제거
                df_news_all['date'] = pd.to_datetime(df_news_all[date_col].astype(str), errors='coerce')
                df_news_all['date'] = df_news_all['date'].dt.normalize()
                
                # 유효하지 않은 날짜 제거
                df_news_all = df_news_all.dropna(subset=['date'])
                
                # 중복 데이터 확인 및 제거
                original_count = len(df_news_all)
                
                # drop_duplicates: 날짜와 본문이 모두 같으면 첫 번째만 남기고 제거
                df_news_all = df_news_all.drop_duplicates(subset=['date', '본문'])
                
                final_count = len(df_news_all)
                removed_count = original_count - final_count
                
                print(f"  * [뉴스] 원본: {original_count}건 -> 중복제거: {removed_count}건 -> 최종: {final_count}건")
                
                # x1 변수: 해당 날짜의 뉴스 기사 개수
                daily_counts = df_news_all['date'].value_counts()
                df_news_all['x1'] = df_news_all['date'].map(daily_counts)
                
                # E1~E7 변수 생성
                df_news_all['bitmask'] = df_news_all['본문'].apply(get_event_bitmask)
                
                df_news_daily = df_news_all[['date', 'x1', 'bitmask']].copy()
            else:
                 print("  ! Warning: 뉴스 파일 컬럼 확인 필요")

    # --- C. 병합 ---
    if not df_news_daily.empty:
        # 1:N 병합 (날짜 하루에 뉴스 N개면 행 N개 생성)
        merged_df = pd.merge(df_stock, df_news_daily, on='date', how='left')
    else:
        merged_df = df_stock.copy()
        merged_df['x1'] = 0
        merged_df['bitmask'] = None

    # 결측치 처리 (뉴스가 없는 날)
    merged_df['x1'] = merged_df['x1'].fillna(0).astype(int)
    merged_df['bitmask'] = merged_df['bitmask'].fillna('0000000')

    # 비트마스크 분리 (E1 ~ E7)
    for i in range(7):
        col_name = f'E{i+1}'
        merged_df[col_name] = merged_df['bitmask'].str[i].astype(int)

    # 거시지표 병합
    merged_df = pd.merge(merged_df, macro_df, on='date', how='left')
    
    # --- D. 저장 ---
    event_cols = [f'E{i+1}' for i in range(7)]
    final_cols_ordered = ['date', 'x1'] + event_cols + macro_columns + ['y(stock)']
    
    # 존재하는 컬럼만 선택하여 순서대로 저장
    merged_df = merged_df[[c for c in final_cols_ordered if c in merged_df.columns]]
    merged_df.rename(columns={'date': 'Date'}, inplace=True)
    
    save_path = os.path.join(OUTPUT_DIR, f"{stock_name}_final.csv")
    merged_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"  -> 저장 완료: {save_path} (Rows: {len(merged_df)})")

print("\n완료되었습니다.")