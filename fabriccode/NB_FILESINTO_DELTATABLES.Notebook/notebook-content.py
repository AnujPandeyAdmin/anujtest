# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "184a4695-86af-4954-846a-f2ad02e212d5",
# META       "default_lakehouse_name": "LKH_RAWDATA",
# META       "default_lakehouse_workspace_id": "5c30c332-8b55-4f54-bc37-f83b84701582"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
def loadFullDataFromSource(table_name):
    df = spark.read.option("inferSchema","true").option("header","true").format("csv").load("Files/output/" + table_name +".csv")
    df.write.format("delta") \
    .mode('overwrite') \
    .option("delta.columnMapping.mode", "name") \
    .option('delta.minReaderVersion', '2') \
    .option('delta.minWriterVersion', '5') \
    .save("Tables/" + table_name)
full_tables = [
    "FactCallCenter",
    "FactCurrencyRate",
    "FactFinance",
    "FactInternetSales",
    "FactInternetSales",
    "FactProductInventory",
    "FactResellerSales",
    "FactSalesQuota",
    "FactSurveyResponse"   
    ]
for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
