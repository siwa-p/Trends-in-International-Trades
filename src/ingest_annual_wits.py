import requests
import xmltodict
import pandas as pd
from utils.logger_config import logger

base_endpoint = "https://wits.worldbank.org/API/V1/SDMX/V21/rest" 
datastructure_ids = ['DF_WITS_Tariff_TRAINS', 'DF_WITS_TradeStats_Development', 'DF_WITS_TradeStats_Tariff', 'DF_WITS_TradeStats_Trade']
dimension_list = ['FREQ', 'REPORTER', 'PARTNER', 'PRODUCTCODE', 'INDICATOR']
country_list = ['India', 'China', 'United States', 'Germany', 'United Kingdom', 'France', 'Italy', 'Canada', 'Australia', 'Brazil']
query = {k:None for k in dimension_list}

    
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
    path = '/'.join([endpoint, 'codelist/WBG_WITS', codelist_name])
    try:
        response = requests.get(path)
        response.raise_for_status()
        logger.info(f"{codelist_name} queried loaded from {path}")
        response_dict = xmltodict.parse(response.text)['Structure']
        codelists = response_dict['Structures']['Codelists']['Codelist']
        codelist_df = getCodelist(codelists)
        if country_list is not None:
            codelist_df = codelist_df[codelist_df['name'].isin(country_list)]
        return codelist_df
    except Exception as e:
        logger.info(f"Error getting {codelist_name}")
        raise e


def get_tariff_aggregates(reporter:str, partner_list:list):
    query['FREQ'] = 'A'
    reporter_code = get_codelist(base_endpoint, 'CL_TS_COUNTRY_WITS', [reporter])['id'].values[0]
    query['REPORTER'] = reporter_code    
    partner_codes = get_codelist(base_endpoint, 'CL_TS_COUNTRY_WITS', partner_list)['id'].tolist()
    query['PARTNER'] = '+'.join(partner_codes)
    dataset_id = 'DF_WITS_TradeStats_Trade'
    product_list = get_codelist(base_endpoint, 'CL_TS_PRODUCTCODE_WITS')['id'].tolist()
    aggregate_data = pd.DataFrame()
    for product in product_list:
        query['PRODUCTCODE'] = product
        query['INDICATOR'] = 'XPRT-TRD-VL'
        query_string = '.'.join([value for value in query.values()])
        logger.info(f"Fetching data for query string: {query_string}")
        period_parameter = '?startPeriod=2015&endPeriod=2025'
        path = '/'.join([base_endpoint, 'data', dataset_id, query_string, period_parameter])
        try:
            response = requests.get(path)
            response.raise_for_status()
            logger.info(f"Data fetched successfully for {path}")
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
            logger.info(f"Data for product {product} appended successfully") 
        except Exception as e:
            logger.info(f"Error fetching data for path: {path}, error: {e}")
    return aggregate_data

def main():
    reporter = 'India'
    partner_list = ['China', 'United States', 'Germany']
    data = get_tariff_aggregates(reporter, partner_list)
    print(data)
    
    
if __name__ == '__main__':
    main()