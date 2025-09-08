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


def create_iceberg_table(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.total_export_value_by_port (
        YEAR INT,
        MONTH INT,
        CTY_CODE INT,
        CTY_NAME STRING,
        PORT STRING,
        PORT_NAME STRING,
        ALL_VAL_MO LONG
    ) USING ICEBERG
    PARTITIONED BY (YEAR, MONTH)
    """)
    logger.info("Iceberg table 'total_export_value_by_port' created or already exists.")
             
def write_iceberg(data, spark):
    columns = data[0]
    rows = data[1:]
    spark_data = spark.createDataFrame(rows, schema=columns)
    spark_data = spark_data.withColumn("YEAR", spark_data["YEAR"].cast("int")) \
                       .withColumn("MONTH", spark_data["MONTH"].cast("int")) \
                       .withColumn("CTY_CODE", spark_data["CTY_CODE"].cast("int")) \
                       .withColumn("ALL_VAL_MO", spark_data["ALL_VAL_MO"].cast("long"))
    spark_data = spark_data.dropDuplicates(["YEAR", "MONTH", "PORT", "CTY_CODE"])
    spark_data.createOrReplaceTempView("temp_new_data")
    spark.sql("""
    MERGE INTO nessie.total_export_value_by_port t
    USING temp_new_data n
    ON t.YEAR = n.YEAR AND t.MONTH = n.MONTH AND t.PORT = n.PORT AND t.CTY_CODE = n.CTY_CODE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)