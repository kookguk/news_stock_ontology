import os
import pandas as pd

# 데이터 폴더 경로
DATA_DIR = "data/stock"
OUTPUT_PATH = "data/stock/stock.csv"

dfs = []

# 폴더 내 모든 csv 파일 순회
for file in os.listdir(DATA_DIR):
    if not file.endswith(".csv"):
        continue

    file_path = os.path.join(DATA_DIR, file)

    # 파일명 → 컬럼명 (확장자 제거)
    stock_name = os.path.splitext(file)[0]

    # CSV 로드
    df = pd.read_csv(file_path)

    # Date, Close만 선택
    df = df[["Date", "Close"]].copy()

    # Close 컬럼 이름을 종목명으로 변경
    df.rename(columns={"Close": stock_name}, inplace=True)

    dfs.append(df)

# Date 기준으로 모두 outer join
from functools import reduce
merged_df = reduce(
    lambda left, right: pd.merge(left, right, on="Date", how="outer"),
    dfs
)

# Date 기준 정렬
merged_df.sort_values("Date", inplace=True)

# 저장
merged_df.to_csv(OUTPUT_PATH, index=False)

print(f"✅ 저장 완료: {OUTPUT_PATH}")
