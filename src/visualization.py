import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pycountry
import streamlit as st
from dotenv import load_dotenv
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir)) # isort:skip
from src.utils.utilities import get_dremio_connection, query_table



load_dotenv(override=True)
st.set_page_config(layout="wide")

dremio_conn = get_dremio_connection(
    os.getenv("DREMIO_USER"),
    os.getenv("DREMIO_PASSWORD"),
    os.getenv("DREMIO_HOST"),
    int(os.getenv("DREMIO_PORT", "32010")),
)

st.title("Visualizing Trends in US International Trade")
st.subheader("Annual aggregate import/export")


def get_iso3(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return None


def classify_code(code):
    if re.match(r"^\d{2}-\d{2}_", code):
        return "HS"
    if re.match(r"^UNCTAD-SoP\d$", code):
        return "SoP"
    return "Broad"


def rename_codes(code):
    return code.split("_")[1] if re.match(r"^\d{2}-\d{2}_", code) else code


@st.cache_data(ttl=3600)
def get_trade_data(trade, year, classification, threshold):
    indicator = "MPRT-TRD-VL" if trade == "import" else "XPRT-TRD-VL"
    query = f"""
        SELECT PRODUCTCODE, OBS_VALUE
        FROM nessie.staged.staged_wits_trade
        WHERE PARTNER_CTY = 'WLD'
          AND TRADE_INDICATOR = '{indicator}'
          AND TIME_PERIOD = {year}
    """
    df = query_table(dremio_conn, query)
    df["group"] = df["PRODUCTCODE"].apply(classify_code)
    df["PRODUCTCODE"] = df["PRODUCTCODE"].apply(rename_codes)
    df = df[df["group"] == classification]
    df = df[df["PRODUCTCODE"] != "Total"]

    total_value = df["OBS_VALUE"].sum()
    main = df[df["OBS_VALUE"] / total_value * 100 >= threshold]
    others = df[df["OBS_VALUE"] / total_value * 100 < threshold]
    if not others.empty:
        main = pd.concat(
            [
                main,
                pd.DataFrame(
                    {
                        "PRODUCTCODE": ["Others"],
                        "OBS_VALUE": [others["OBS_VALUE"].sum()],
                    }
                ),
            ],
            ignore_index=True,
        )
    return main, total_value

def display_trade_summary(import_total, export_total):
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Import Value (Trillion USD)", f"${import_total/1e9:.2f}T")
    with col2:
        st.metric("Total Export Value (Trillion USD)", f"${export_total/1e9:.2f}T")
        
def get_trade_volume_query(data_type, year, month):
    year_col = "EXPORT_YEAR" if data_type == "export" else "IMPORT_YEAR"
    month_col = "EXPORT_MONTH" if data_type == "export" else "IMPORT_MONTH"
    table = f"staged_{data_type}_hs"
    value_col = "ALL_VAL_MO" if data_type == "export" else "GEN_VAL_MO"
    return f"""
        SELECT
            CTY_NAME,
            SUM({value_col}) AS total_value
        FROM "nessie.staged".{table}
        WHERE DIST_NAME != 'TOTAL FOR ALL DISTRICTS'
          AND SUMMARY_LVL = 'DET'
          AND {year_col} = {year}
          AND {month_col} = {month}
          AND CTY_CODE IS NOT NULL
        GROUP BY CTY_NAME
    """


year = st.selectbox("Pick year", [2017, 2018, 2019, 2020, 2021, 2022])
threshold = st.slider("Minimum share (%) to display separately", 0.5, 5.0, 1.3)

try:
    imports, import_total = get_trade_data("import", year, "HS", threshold)
    exports, export_total = get_trade_data("export", year, "HS", threshold)

    # Merge import and export data for common PRODUCTCODEs
    merged = pd.merge(
        imports[["PRODUCTCODE", "OBS_VALUE"]].rename(
            columns={"OBS_VALUE": "Import Value"}
        ),
        exports[["PRODUCTCODE", "OBS_VALUE"]].rename(
            columns={"OBS_VALUE": "Export Value"}
        ),
        on="PRODUCTCODE",
        how="outer",
    ).fillna(0)

    # Melt for grouped bar chart
    melted = merged.melt(
        id_vars="PRODUCTCODE",
        value_vars=["Import Value", "Export Value"],
        var_name="Trade Type",
        value_name="Value",
    )
    display_trade_summary(import_total, export_total)
    
    fig = px.bar(
        melted,
        x="PRODUCTCODE",
        y="Value",
        color="Trade Type",
        barmode="group",
        title=f"Import vs Export Distribution in {year} (HS Classification)",
        labels={"Value": "Trade Value ($)", "PRODUCTCODE": "Commodity Code"},
        width=900,
        height=600,
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig)
except Exception as e:
    st.error(f"Error reading WITS annual trade data: {e}")

st.subheader("Monthly trade volume by country")

year = st.selectbox(
    "Select Year for Trade by Volume",
    [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
    index=8,
)
month = st.selectbox("Pick month", list(range(1, 13)), index=0)
data_type = st.selectbox("Select Data Type", ["import", "export"], index=0)

trade_volume_df = query_table(
    dremio_conn, get_trade_volume_query(data_type, year, month)
)
trade_volume_df["iso3"] = trade_volume_df["CTY_NAME"].apply(get_iso3)

trade_volume_df["log_total_value"] = trade_volume_df["total_value"].apply(lambda x: None if x <= 0 else np.log10(x))
tick_values = [1e4,1e5,1e6,1e7,1e8,1e9,1e10,1e11]
tickvals = [np.log10(v) for v in tick_values]
ticktext = [
    "$10K", "$100K", "$1M", "$10M", "$100M", "$1B", "$10B", "$100B"
]

fig = px.choropleth(
    trade_volume_df,
    locations="iso3",
    color="log_total_value",
    hover_name="CTY_NAME",
    color_continuous_scale=px.colors.sequential.Viridis[::-1],
    title=f"Global {data_type.title()} Trade Value by Country in {year} and month {month}",
    labels={"total_value": "Trade Value ($)"},
    width=1400,
    height=1200,
    color_continuous_midpoint=None,
)

fig.update_coloraxes(
    colorbar_tickvals=tickvals,
    colorbar_ticktext=ticktext,
    colorbar_title="Trade Value"
)
fig.update_layout(geo=dict(showframe=False, showcoastlines=True))
st.plotly_chart(fig)


import_dict = {
    # "smartphones": "851713",
    # "lithium ion batteries": "850760",
    # "video game consoles": "950450",
    "wheat": "100810",
    "furniture": "940350",
    "metal furniture": "940320",
    "processors": "854231",
    "clothing(cotton)": "620342",
    "television sets": "852872",
    # "Estimated imports of low valued transactions": "999995",
    "refrigerators": "841810",
    "air conditioners": "841510",
    "footwear": "640399",
    "plastic articles": "392690",
    "cars": "870323",
    "Head phones, earphones and combined microphone/speaker sets": "851830",
    "CHRISTMAS FESTIVITIES AND ACCESSORIEs": "950510",
    "motorcycles": "871120",
    "bicycles": "871200",
    "paper products": "481910",
    # "medicaments": "300490",
    # "network equipment": "851762",
    "adp parts & accessories": "847330",
    # "tricycles & pedal toys": "950300",
}


st.subheader("Import vs Tariff trend by Commodity from China")

selected = st.selectbox("Select an import commodity", list(import_dict.keys()), index=1)
code = import_dict[selected]

trade_query = f"""
    SELECT IMPORT_YEAR, IMPORT_MONTH, SUM(GEN_VAL_MO) AS TOTAL_VAL_MO
    FROM nessie.staged.staged_import_hs
    WHERE CTY_NAME='CHINA' AND I_COMMODITY='{code}' AND DIST_NAME != 'TOTAL FOR ALL DISTRICTS'
    GROUP BY IMPORT_YEAR, IMPORT_MONTH
    ORDER BY IMPORT_YEAR, IMPORT_MONTH
"""
trade_df = query_table(dremio_conn, trade_query)
trade_df["date"] = pd.to_datetime(
    trade_df["IMPORT_YEAR"].astype(str)
    + "-"
    + trade_df["IMPORT_MONTH"].astype(str).str.zfill(2)
    + "-01"
)

tariff_query = f"""
    SELECT date_tariff, base_mfn_rate, effective_ad_val_rate
    FROM nessie.staged.staged_tariff_timeline
    WHERE hts6='{code}'
    ORDER BY date_tariff
"""
tariff_df = query_table(dremio_conn, tariff_query)
tariff_df["date_tariff"] = pd.to_datetime(tariff_df["date_tariff"])
tariff_df = tariff_df[tariff_df["date_tariff"] >= trade_df["date"].min()]
tariff_df["percent_ad_val_rate"] = tariff_df["effective_ad_val_rate"] * 100
fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=trade_df["date"],
        y=trade_df["TOTAL_VAL_MO"],
        mode="lines+markers",
        name="Import Value",
        yaxis="y1",
    )
)
fig.add_trace(
    go.Scatter(
        x=tariff_df["date_tariff"],
        y=tariff_df["percent_ad_val_rate"],
        mode="lines+markers",
        name="Tariff Rate",
        yaxis="y2",
        line=dict(color="red"),
    )
)
fig.update_layout(
    title=f"Monthly Import Value and Tariff Rate for {selected.title()}",
    xaxis=dict(title="Date"),
    yaxis=dict(title="Import Value", side="left"),
    yaxis2=dict(
        title="Tariff Rate (%)",
        overlaying="y",
        side="right",
        showgrid=False,
        range=[0, max(tariff_df["percent_ad_val_rate"].max(), 2)],
    ),
    legend=dict(x=0.01, y=0.99),
    width=1000,
    height=500,
)
st.plotly_chart(fig)
