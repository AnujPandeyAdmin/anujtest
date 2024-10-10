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

# MARKDOWN ********************

# #### Why? 
# 
# Dealing with missing values is a common data processing task in preparation for machine learning. 
# 
# Most machine learning algorithms don't like it when you have missing values. 
# 
# So there are a variety of methods we can use to deal with this. 
# 
# Which one you choose depends greatly on: 
# - the business question you are answering, 
# - what 'makes sense' for your domain. 

# MARKDOWN ********************

# #### Let's get some data 

# CELL ********************

df = spark.read.csv('Files/raw/property-sales-missing.csv', header=True, inferSchema=True)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Dropping rows with nulls

# CELL ********************

#most basic/drastic drop NAs 
df.na.drop().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### How='any' vs how='all'

# CELL ********************

#drop the row if ANY values are null  
df.na.drop(how='any').show()


#drop the row if ALL values are null  
df.na.drop(how='all').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Dropping with a threshold 

# CELL ********************

df.na.drop(thresh=3).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Only drop if NULL in certain columns (subset)

# CELL ********************

df.na.drop(subset=["SalePrice_USD","Address"]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Filling missing values

# CELL ********************

#two methods, same result
# note: the columns in the subset must be the same data type as the value, otherwise it will be ignored. 
df.na.fill(value='Unknown Address',subset=["Address"]).show()

df.fillna(value='Unknown Address',subset=["Address"]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#also works for numbers 

df.na.fill(value=0,subset=["SalePrice_USD", "Address"]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Mean Imputation

# CELL ********************

from pyspark.ml.feature import Imputer

# Initialize the Imputer
imputer = Imputer(
    inputCols= ['SalePrice_USD'], #specifying the input column names
    outputCols=['SalesPriceImputed_USD'], #specifying the output column names
    strategy="mean"                  # or "median" if you want to use the median value
)

# Fit the Imputer
model = imputer.fit(df)

#Transform the dataset
imputed_df = model.transform(df)

imputed_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# #### Your final tasks: 
# 
# ✅ View this notebook and all others in this series on GitHub. 
# 
# ✅ Leave a comment below
# 
# ✅ Subscribe 
# 
# ✅ Like
