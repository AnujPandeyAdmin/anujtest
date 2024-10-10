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

import requests
url ='https://reqres.in/api/users?page=2'

#Making a Get Request
response= requests.get(url)

#Process the request data (Which is usually JSON)
data=response.json()

print (data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JSON to DataFrame ##
from pyspark.sql import SparkSession
from pyspark.sql import Row


#Initialize Spark Session 
Spark =SparkSession.builder.appName("JsonToDelta").getOrCreate()

# Assumung Data is the Dictionary you showed in your Print Data 
results_list=data['data']

# Convert the list of dictionaries to a DataFrame
df=spark.createDataFrame(Row(**x) for x in results_list)

df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Store in Delta Table ##

#Specify the path where the delta table will be stored
delta_table_path ="Tables/EmpAPIDATA"

# Write the dataframe to a Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path)

#Query the delta table
users_df =spark.sql("SELECT * FROM LKH_RAWDATA.EmpAPIDATA LIMIT 1000")
display(users_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
