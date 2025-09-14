import pandas as pd
import re
from utils.logger_config import logger
import os
from dotenv import load_dotenv
import numpy as np
from utils.utilities import get_minio_client, upload_parquet, get_csv_from_lake
load_dotenv(override=True)


def fix_datetime_columns(data:pd.DataFrame):
    date_columns = ['begin_effect_date', 'end_effective_date']
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], errors='coerce')
    return data

def preprocess_tariff_data(tariff_2025):
    tariff_2025['hts6'] = tariff_2025['hts8'].astype(str).str[:6]
    columns_of_interest = [
        'hts8', 'hts6', 'brief_description', 'mfn_text_rate', 'mfn_ad_val_rate',
        'mfn_specific_rate', 'mfn_other_rate', 'begin_effect_date', 'end_effective_date', 'additional_duty'
    ]
    tariff_2025_filtered = tariff_2025[columns_of_interest]
    tariff_2025_filtered = fix_datetime_columns(tariff_2025_filtered)
    return tariff_2025_filtered

def extract_extra_rate(text):
    if pd.isna(text):
        return 0
    match = re.search(r"(\d+(\.\d+)?)\s*%", text)
    return float(match.group(1)) / 100 if match else 0

def process_chapter99(tariff_2025_filtered):
    chapter99 = tariff_2025_filtered[tariff_2025_filtered["hts8"].astype(str).str.startswith("99")].copy()
    chapter99["extra_rate_text"] = chapter99.apply(
        lambda row: extract_extra_rate(row["mfn_text_rate"]) if row["mfn_ad_val_rate"] >= 9999 else 0,
        axis=1
    )
    chapter99["surcharge_rate"] = np.where(
        chapter99["mfn_ad_val_rate"] < 9999,
        chapter99["mfn_ad_val_rate"],
        chapter99["extra_rate_text"]
    )
    chapter99['hts8'] = chapter99['hts8'].astype(str)
    chapter99 = fix_datetime_columns(chapter99)
    return chapter99

def preprocess_china_tariff(china_tariff):
    china_tariff.columns = ['hts8', 'mapped_99_code']
    china_tariff['hts8'] = china_tariff['hts8'].str.replace('.', '')
    return china_tariff

def merge_tariff_data(tariff_2025_filtered, china_tariff, chapter99):
    tariff_2025_filtered['hts8'] = tariff_2025_filtered['hts8'].astype(str).str.zfill(8)
    merged = pd.merge(tariff_2025_filtered, china_tariff, on='hts8', how='inner')
    merged['mapped_99_code'] = merged['mapped_99_code'].astype(str).str.replace('.', '')
    merged = merged.merge(
        chapter99[["hts8", "surcharge_rate", "begin_effect_date", "end_effective_date"]],
        left_on="mapped_99_code", right_on="hts8", how="left", suffixes=("", "_99")
    )
    merged["effective_ad_val_rate"] = merged["mfn_ad_val_rate"] + merged["surcharge_rate"].fillna(0)
    return merged

def aggregate_tariff_data(merged):
    merged_grouped = merged.groupby('hts6').agg({
        'mfn_ad_val_rate': 'mean',
        'effective_ad_val_rate': 'mean',
        'mfn_specific_rate': 'mean',
        'mfn_other_rate': 'mean',
        'surcharge_rate': 'mean',
        'effective_ad_val_rate': 'mean',
        'begin_effect_date': 'min',
        'end_effective_date': 'max',
        'begin_effect_date_99': 'min',
        'end_effective_date_99': 'max'
    }).reset_index()
    cutoff_date = pd.Timestamp("2025-12-31")
    
    merged_grouped["end_effective_date"] = merged_grouped["end_effective_date"].clip(upper=cutoff_date)
    merged_grouped["end_effective_date_99"] = merged_grouped["end_effective_date_99"].clip(upper=cutoff_date)
    return merged_grouped

def build_tariff_timeline(merged_copy):
    start_date = merged_copy["begin_effect_date"].min()
    end_date = merged_copy["end_effective_date"].max()
    all_months = pd.date_range(start=start_date, end=end_date, freq='MS')

    timeline_rows = []
    for _, row in merged_copy.iterrows():
        for month in all_months:
            if row["begin_effect_date"] <= month <= row["end_effective_date"]:
                base_mfn_rate = row["mfn_ad_val_rate"]
                if pd.notnull(row["begin_effect_date_99"]) and row["begin_effect_date_99"] <= month <= row["end_effective_date_99"]:
                    surcharge_rate = row["surcharge_rate"] if pd.notnull(row["surcharge_rate"]) else 0
                    effective_rate = base_mfn_rate + surcharge_rate
                else:
                    effective_rate = base_mfn_rate
                timeline_rows.append({
                    "hts6": row["hts6"],
                    "month": month,
                    "base_mfn_rate": base_mfn_rate,
                    "surcharge_rate": surcharge_rate if 'surcharge_rate' in locals() else 0,
                    "effective_ad_val_rate": effective_rate
                })
    return pd.DataFrame(timeline_rows)

def apply_blanket_tariffs(timeline_df, blanket_tariffs):
    sorted_events = sorted(blanket_tariffs, key=lambda x: pd.to_datetime(x['date']))
    month_tariff_map = {}
    for event in sorted_events:
        event_month = pd.to_datetime(event['date']).replace(day=1)
        month_tariff_map[event_month] = event['tariff']
    def get_latest_tariff(month):
        applicable_months = [m for m in month_tariff_map if m <= month]
        if not applicable_months:
            return 0
        latest_month = max(applicable_months)
        return month_tariff_map[latest_month]
    timeline_df['effective_ad_val_rate'] = timeline_df.apply(
        lambda row: row['effective_ad_val_rate'] + get_latest_tariff(row['month']),
        axis=1
    )
    return timeline_df

def main():
    minio_client = get_minio_client(
        os.getenv("MINIO_EXTERNAL_URL"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
    )
    cumulative_blanket_tariffs = [
        {"description": "Phase One IEEPA", "date": "2025-02-04", 'tariff': 0.10},
        {"description": "Phase two IEEPA", "date": "2025-03-04", 'tariff': 0.20},
        {"description": "Phase five: China ", "date": "2025-04-10", 'tariff': 1.25},
        {"description": "Phase six: China ", "date": "2025-06-01", 'tariff': 0.10}
    ]
    tariff_2025 = get_csv_from_lake(minio_client, os.getenv("MINIO_BUCKET_NAME"), 'tariff_database_2025.csv')
    logger.info(f"Loaded tariff_databse from Minio")
    china_tariff = get_csv_from_lake(minio_client, os.getenv("MINIO_BUCKET_NAME"), 'china_tarriffs.csv')
    logger.info(f"Loaded China Tariff from Minio")
    tariff_2025_filtered = preprocess_tariff_data(tariff_2025)
    logger.info(f"processed tariff data")
    chapter99 = process_chapter99(tariff_2025_filtered)
    logger.info(f"split chapter99 data")
    china_tariff = preprocess_china_tariff(china_tariff)
    tariff_2025_filtered_cleaned =tariff_2025_filtered[tariff_2025_filtered['mfn_other_rate']!=9999.999999]
    merged = merge_tariff_data(tariff_2025_filtered_cleaned, china_tariff, chapter99)
    merged_copy = aggregate_tariff_data(merged)
    logger.info(f"Merged tariff with chapter99")
    timeline_df = build_tariff_timeline(merged_copy)
    timeline_df = apply_blanket_tariffs(timeline_df, cumulative_blanket_tariffs)
    timeline_df["month"] = timeline_df["month"].astype("datetime64[us]")
    upload_parquet(minio_client, os.getenv("MINIO_BUCKET_NAME"), timeline_df, 'tariff_timeline.parquet')
    logger.info("Tariff timeline data uploaded to MinIO successfully")
    
if __name__=='__main__':
    main()