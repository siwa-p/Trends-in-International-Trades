# def get_datastructure(endpoint):
#     path = '/'.join([endpoint, "dataflow"])
#     try:
#         response = requests.get(path)
#         response.raise_for_status()
#         logger.info(f"Fetching dataflow from {path}")
#         response_dict = xmltodict.parse(response.text)
#         dataflows = pd.json_normalize(response_dict['Structure']['Structures']['Dataflows']['Dataflow'])
#         dataflows = dataflows[(dataflows['Name.@xml:lang'] == 'en') &
#                                 (dataflows['Description.@xml:lang'] == 'en')
#                             ]
#         dataflows = dataflows[['@id', '@agencyID', '@version', '@isFinal', 'Description.#text', 'Structure.Ref.@id']]
#         dataflows = dataflows.rename(columns={
#             '@id': 'id',
#             '@agencyID': 'agencyID',
#             '@version': 'version',
#             '@isFinal':'isFinal',
#             'Description.#text': 'description',
#             'Structure.Ref.@id': 'datastructure'
#         })
#         datastructure = dataflows[['id', 'datastructure', 'description']]
#         return datastructure
#     except Exception as e:
#         logger.info(f"Error fetching datastructure: {e}")
#         raise e
    
# def get_dimensions(endpoint, datastructure_id):
#     path = '/'.join([endpoint, 'datastructure/WBG_WITS', datastructure_id])
#     try:
#         response = requests.get(path)
#         response.raise_for_status()
#         logger.info(f"Loading data from {path}")
#         response_dict = xmltodict.parse(response.text)
#         datastructure = response_dict['Structure']['Structures']['DataStructures']['DataStructure']
#         dimensions = pd.json_normalize(datastructure['DataStructureComponents']['DimensionList']['Dimension'])[['@id', '@position', 'ConceptIdentity.Ref.@maintainableParentID', 'LocalRepresentation.Enumeration.Ref.@id']]
#         dimensions = dimensions.rename(columns={
#             '@id': 'id',
#             '@position': 'position',
#             'ConceptIdentity.Ref.@maintainableParentID': 'conceptscheme',
#             'LocalRepresentation.Enumeration.Ref.@id': 'codelist'
#         })
#         return dimensions
#     except Exception as e:
#         logger.info(f"Error fetching datastructure: {e}")
#         raise e