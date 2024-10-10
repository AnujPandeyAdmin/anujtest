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

df = spark.read.format("csv").option("header","true").load("Files/raw/orders.csv")
# df now is a Spark DataFrame containing CSV data from "Files/raw/orders.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path ="Tables/Customerdata"
df.write.format("delta").mode("overwrite").save(delta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LKH_RAWDATA.Customerdata LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
