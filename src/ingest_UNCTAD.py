import requests
import xmltodict
import pandas as pd
from utils.logger_config import logger
import os
from utils.utilities import get_minio_client, upload_csv, upload_parquet
from dotenv import load_dotenv

load_dotenv(override=True)

endpoint = "https://wits.worldbank.org/API/V1/SDMX/V21/rest" 

def getCodelist(codelist):
    codelist_codes = codelist['Code']
    if type(codelist_codes) == list:
        codelist_code = [{'id': code['@id'],
                        'name': code['Name']['#text'],
                        'language': code['Name']['@xml:lang']} for code in codelist_codes]
    else:
        codelist_code = [{'id': codelist_codes['@id'],
        'name': codelist_codes['Name']['#text'],
        'language': codelist_codes['Name']['@xml:lang']}]
    return  pd.DataFrame(codelist_code)


def get_codelist(endpoint, codelist_name, country_list:list=None):
    # codelist_name are found in dimensionn table
    path = '/'.join([endpoint, 'codelist/WBG_WITS', codelist_name])
    try:
        response = requests.get(path)
        response.raise_for_status()
        response_dict = xmltodict.parse(response.text)['Structure']
        codelists = response_dict['Structures']['Codelists']['Codelist']
        codelist_df = getCodelist(codelists)
        if country_list is not None:
            codelist_df = codelist_df[codelist_df['name'].isin(country_list)]
        return codelist_df
    except Exception as e:
        logger.info(f"Error getting {codelist_name}")
        raise e
    

def get_data_for_a_query(query_dict, dataset_id):
    aggregate_data = pd.DataFrame()
    query_string = '.'.join([value for value in query_dict.values()])
    path = '/'.join([endpoint, 'data', dataset_id, query_string])
    print(path)
    try:
        response = requests.get(path)
        response.raise_for_status()
        response_dict = xmltodict.parse(response.text)
        response_series = response_dict['message:GenericData']['message:DataSet']['generic:Series']
        series_list = [response_series] if type(response_series) == dict else response_series
        for i, series in enumerate(series_list):
            series_key = series['generic:SeriesKey']
            series_key_values_raw = [series_key['generic:Value']] if type(series_key['generic:Value']) == dict else series_key['generic:Value']
            series_key_values = {value['@id']:value['@value'] for value in series_key_values_raw}
            series_obs_raw = [series['generic:Obs']] if type(series['generic:Obs']) == dict else series['generic:Obs']
            for j, obs in enumerate(series_obs_raw):
                obs_dimensions_raw = [obs['generic:ObsDimension']] if type(obs['generic:ObsDimension']) == dict else obs['generic:ObsDimension'] 
                obs_dimensions = {value['@id']: value['@value'] for value in obs_dimensions_raw}
                obs_attributes = obs['generic:Attributes']['generic:Value']
                obs_attributes = [obs_attributes] if type(obs_attributes) == dict else obs_attributes
                obs_attributes = {value['@id']: value['@value'] for value in obs_attributes}

                obs_value = obs['generic:ObsValue']
                obs_value = {'OBS_VALUE': float(obs_value['@value'])}
                
                observation = [{**series_key_values, **obs_dimensions, **obs_attributes, **obs_value}]
                observations = observation if j == 0 else observations + observation
            series_observations = observations if i == 0 else series_observations + observations
        series_observation = pd.DataFrame(series_observations)
        aggregate_data = series_observation if aggregate_data.empty else pd.concat([aggregate_data, series_observation], ignore_index=True)
        logger.info(f"Data for product {query_dict['PRODUCTCODE']} and partner {query_dict['PARTNER']} appended successfully") 
        return aggregate_data
    except Exception as e:
        logger.info(f"Error fetching data for path {path}, error: {e}")

def ingest_all_unctad(minio_client, reporter_name, dataset_id, datatype_id):
    country_list_data = get_codelist(endpoint, 'CL_COUNTRY_WITS')
    country_list = country_list_data['id'].tolist()
    reporter_code = country_list_data[country_list_data['name']==reporter_name]['id'].tolist()[0]
    partner_list = [code for code in country_list if code not in reporter_code]
    freq_id = 'A'
    product_list = get_codelist(endpoint, "CL_PRODUCTCODE_WITS")['id'].tolist()
    dimension_list = ['FREQ', 'REPORTER', 'PARTNER', 'PRODUCTCODE', 'DATATYPE']
    query = {k:None for k in dimension_list}
    query['FREQ'] = freq_id
    query['REPORTER'] = reporter_code
    query['DATATYPE'] = datatype_id
    for partner_code in partner_list:
        query['PARTNER'] = partner_code
        total_data = pd.DataFrame()
        for product_id in product_list:
            query['PRODUCTCODE'] = product_id
            data = get_data_for_a_query(query, dataset_id)
            if data is not None:
                total_data = data if total_data.empty else pd.concat([total_data, data], ignore_index=True)
        upload_parquet(minio_client, os.getenv("MINIO_BUCKET_NAME"), total_data, f"unctad/{reporter_name.replace(' ','_')}_to_{partner_code}_unctad_data.parquet")
    logger.info(f"Data uploaded to Minio")
    return None          
  
def main():
    reporter_name = 'United States'
    dataset_id = "DF_WITS_Tariff_TRAINS"
    datatype_id = 'Reported'
    minio_client = get_minio_client(
        os.getenv("MINIO_EXTERNAL_URL"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
    )
    ingest_all_unctad(minio_client, reporter_name, dataset_id, datatype_id)

    
if __name__=='__main__':
    main()
