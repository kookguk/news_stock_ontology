from fredapi import Fred
import pandas as pd
from pathlib import Path

# ---------------------------
# 1. FRED 설정
# ---------------------------
fred = Fred(api_key="a766178067d139159672781876da6028")

start_date = "2024-12-09"
end_date   = "2025-12-09"

# ---------------------------
# 2. 데이터 정의
# ---------------------------
FX = {
    "usd_krw": "DEXKOUS",
    "jpy_usd": "DEXJPUS",
    "eur_usd": "DEXUSEU",
    "gbp_usd": "DEXUSUK"
}

COMMODITY = {
    "wti": "DCOILWTICO",
    "gold": "GOLDPMGBD228NLBM"
}

RATE = {
    "kor_3y": "IRLTLT01KRM156N",
    "us_1m": "DGS1MO"
}

MACRO = {
    "vix": "VIXCLS"
}

# ---------------------------
# 3. 저장 경로 생성
# ---------------------------
base_dir = Path("data")
paths = {
    "fx": base_dir / "fx",
    "commodity": base_dir / "commodity",
    "rate": base_dir / "rate",
    "macro": base_dir / "macro"
}

for p in paths.values():
    p.mkdir(parents=True, exist_ok=True)

# ---------------------------
# 4. 데이터 수집 및 저장
# ---------------------------
def fetch_and_save(series_dict, save_path):
    for name, code in series_dict.items():
        try:
            s = fred.get_series(code, start_date, end_date)

            df = s.to_frame(name="Value")
            df.index.name = "Date"

            # Name 컬럼 추가
            df["Name"] = name

            # 컬럼 순서 정리
            df = df.reset_index()[["Date", "Value", "Name"]]

            df.to_csv(save_path / f"{name}.csv", index=False)
            df.to_excel(save_path / f"{name}.xlsx", index=False)

            print(f"✅ Saved: {name}")

        except Exception as e:
            print(f"❌ Failed: {name} ({code})")
            print(e)


# ---------------------------
# 5. 실행
# ---------------------------
fetch_and_save(FX, paths["fx"]) # 환율
fetch_and_save(COMMODITY, paths["commodity"]) # 유가, 금
fetch_and_save(RATE, paths["rate"]) # 금리
fetch_and_save(MACRO, paths["macro"]) # 거시지표