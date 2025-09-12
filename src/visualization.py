import streamlit as st
import plotly.graph_objects as go
import sys
import re
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from src.utils.utilities import create_spark_session
import pandas as pd
import plotly.express as px
spark = create_spark_session()

st.title("Visualizing Trends in US International Trade")
st.header("This is the header")
st.markdown("This is the markdown")
st.subheader("This is the subheader")
st.caption("Distribution of import/export by Commodity group")

def classify_code(code):
    if re.match(r"^\d{2}-\d{2}_", code):
        return "HS"
    elif re.match(r"^UNCTAD-SoP\d$", code):
        return "SoP"
    else:
        return "Broad"  
year = st.selectbox('Pick year', [2017, 2018, 2019, 2020, 2021, 2022])
classification = 'HS'

def rename_codes(code):
    if re.match(r"^\d{2}-\d{2}_", code):
        return code.split('_')[1]
    else:
        return code
    
def get_trade_data(trade, year, classification):
    filters = []
    filters.append(f"PARTNER_CTY = 'WLD'")
    if trade == 'import':
        filters.append(f"TRADE_INDICATOR = 'MPRT-TRD-VL'")
    else:
        filters.append(f"TRADE_INDICATOR = 'XPRT-TRD-VL'")
    if year:
        filters.append(f"TIME_PERIOD = {year}")

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)
    query = f"""
        SELECT PRODUCTCODE, OBS_VALUE
        FROM nessie.silver.bronze_wits_annual_trade
        {where_clause}
    """
    tariff = spark.sql(query)
    tariff_pd = tariff.toPandas()
    tariff_pd['group'] = tariff_pd['PRODUCTCODE'].apply(classify_code)
    tariff_pd['PRODUCTCODE']  = tariff_pd['PRODUCTCODE'].apply(rename_codes)
    tariff_filtered = tariff_pd[tariff_pd['group'] == classification]
    if 'Total' in tariff_filtered['PRODUCTCODE'].unique().tolist():
        tariff_filtered = tariff_filtered[tariff_filtered['PRODUCTCODE'] != 'Total']
    threshold = 1.3
    total_value = tariff_filtered['OBS_VALUE'].sum()
    main = tariff_filtered[(tariff_filtered['OBS_VALUE'] / total_value) * 100 >= threshold]
    others = tariff_filtered[(tariff_filtered['OBS_VALUE'] / total_value) * 100 < threshold]
    if not others.empty:
        others_sum = others['OBS_VALUE'].sum()
        main = pd.concat([
            main,
            pd.DataFrame({'PRODUCTCODE': ['Others'], 'OBS_VALUE': [others_sum]})
        ], ignore_index=True)
    return main, total_value

try:
    import_data, import_total = get_trade_data('import', year, classification)
    export_data, export_total = get_trade_data('export', year, classification)

    fig_import = px.pie(
        import_data,
        names='PRODUCTCODE',
        values='OBS_VALUE',
        title=f'Import Distribution in {year}',
        hole=0.4,
        width=700,
        height=600
    )
    fig_import.update_traces(textinfo='percent+label')
    fig_import.update_layout(
        showlegend=False,
        annotations=[dict(
            text=f"Total<br>{import_total/1e9:.3f} Trillion $",
            x=0.5, y=0.5, font_size=16, showarrow=False
        )]
    )

    fig_export = px.pie(
        export_data,
        names='PRODUCTCODE',
        values='OBS_VALUE',
        title=f'Export Distribution in {year}',
        hole=0.4,
        width=700,
        height=600
    )
    fig_export.update_traces(textinfo='percent+label')
    fig_export.update_layout(
        showlegend=False,
        annotations=[dict(
            text=f"Total<br>{export_total/1e9:.3f} Trillion $",
            x=0.5, y=0.5, font_size=16, showarrow=False
        )]
    )
    st.plotly_chart(fig_import)
    st.plotly_chart(fig_export)
except Exception as e:
    st.error(f"Error reading WITS annual trade data: {e}")
    st.error(f"Error reading WITS annual trade data: {e}")

import_dict = {
    'wheat': '100810',
    'furniture': '940350',
    'processors': '854231',
    'clothing(cotton)': '620342',
    'television sets': '852872',
    'refrigerators': '841810',
    'air conditioners': '841510',
    'footwear': '640399',
    'plastic articles': '392690',
    'cars': '870323',
    'motorcycles': '871120',
    'bicycles': '871200',
    'paper products': '481910'
}

selected_commodity = st.selectbox("Select an import commodity", list(import_dict.keys()))
commodity_code = import_dict[selected_commodity]

trade_query = f"""
    SELECT IMPORT_YEAR, IMPORT_MONTH, SUM(GEN_VAL_MO) AS TOTAL_VAL_MO
    FROM nessie.silver.bronze_hs_import
    WHERE CTY_NAME='CHINA' AND I_COMMODITY='{commodity_code}'
    GROUP BY IMPORT_YEAR, IMPORT_MONTH
    ORDER BY IMPORT_YEAR, IMPORT_MONTH
"""
trade_df = spark.sql(trade_query).toPandas()
trade_df['date'] = pd.to_datetime(trade_df['IMPORT_YEAR'].astype(str) + '-' + trade_df['IMPORT_MONTH'].astype(str).str.zfill(2) + '-01')
tariff_query = f"""
    SELECT date_tariff, base_mfn_rate, effective_ad_val_rate
    FROM nessie.silver.bronze_tariff_timeline
    WHERE hts6='{commodity_code}'
    ORDER BY date_tariff
"""
tariff_df = spark.sql(tariff_query).toPandas()
tariff_df['date_tariff'] = pd.to_datetime(tariff_df['date_tariff'])
tariff_df = tariff_df[tariff_df['date_tariff']>=trade_df['date'].min()]
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=trade_df['date'],
    y=trade_df['TOTAL_VAL_MO'],
    mode='lines+markers',
    name='Import Value',
    yaxis='y1'
))
fig.add_trace(go.Scatter(
    x=tariff_df['date_tariff'],
    y=tariff_df['effective_ad_val_rate'],
    mode='lines+markers',
    name='Tariff Rate',
    yaxis='y2',
    line=dict(color='red')
))
fig.update_layout(
    title=f"Monthly Import Value and Tariff Rate for {selected_commodity.title()}",
    xaxis=dict(title='Date'),
    yaxis=dict(title='Import Value', side='left'),
    yaxis2=dict(
        title='Tariff Rate (%)',
        overlaying='y',
        side='right',
        showgrid=False,
        range=[0, max(tariff_df['effective_ad_val_rate'].max(), 2)] 
    ),
    legend=dict(x=0.01, y=0.99),
    width=1000,
    height=500
)
st.plotly_chart(fig)