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

# # Schema-validation of incoming files with Great Expectations
# eated by Will Needham, [Learn Microsoft Fabric](https://youtube.com/@learnmicrosoftfabric) 
# Goal: show you a simple way to setup Great Expectations to perform schema validation on incoming files.** 
# ote: the approach shown in this notebook is what I would call 'GX Lite'. GX is a vast library with lots of different features, but it can be overwhelming for beginners. I have developed the approach below to strip out a lot of the complexity and make it as easy as possible to get started with GX. Once you get the basics, it would be worthwhile learning about getting up Data Sources, Expectation Suites, Checkpoints etc, as these unlock more of the GX advanced features, like Actions, Data Docs etc._
#  the example, we are using CSV file formats, but GX can validate pretty much any file type that can be read into a pandas dataframe. See [here](https://docs.greatexpectations.io/docs/oss/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data/) for more details. 
#  Prerequisites
# Download the [sample CSV from GitHub](https://drive.google.com/uc?id=1zO8ekHWx9U7mrbx_0Hoxxu6od7uxJqWw&export=download) and upload to a Lakehouse Files area. I saved the file to this location: '/lakehouse/default/Files/landing/hubspot/csv/customers-100.csv' so if you want to follow along without changing any of the code below, you will also need to create the same folder structure. Or update the first paramter cell in this notebook, pointing to where you saved the csv. 
#  Define path to file to be validation 
#  make use of a parameter cell in Fabric to be able to parameterize the notebook (i.e. so we can embed this validation notebook into a Fabric Data Pipeline)


# PARAMETERS CELL ********************

file_path_to_be_validated = '/lakehouse/default/Files/raw/customers-100.csv'
output_table_name = 'raw_customers'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Install GX (if not installed at the workspace level)

# CELL ********************

%pip install --q great_expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialise GX context and a validator

# CELL ********************

import great_expectations as gx
context = gx.get_context()
validator = context.sources.pandas_default.read_csv(file_path_to_be_validated)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Adding some 'expectations' of the schema/ structure of this file
# 
# We define a number of 'expectations' of our dataset. 
# 
# These expectations are not meant to be exhaustive, just a few expectations so you get the idea. 
# 
# You will need to create your own expectations based on your data and the domain you are working in. 
# 
# The full list of expectations can be found [here](https://greatexpectations.io/expectations), and you can also [create your own](https://docs.greatexpectations.io/docs/oss/guides/expectations/custom_expectations_lp).  

# CELL ********************

# expect the columns to be from the expected column_set
expected_column_set = ["Index", "Customer Id", "First Name", "Last Name", "Company", "City", "Country", "Phone 1", "Phone 2","Email", "Subscription Date", "Website"]
validator.expect_table_columns_to_match_set(expected_column_set, exact_match=True)

# expect the following column values to not be null (perhaps these are needed for downstream analytics)
validator.expect_column_values_to_not_be_null(column="Customer Id")
validator.expect_column_values_to_not_be_null(column="Subscription Date")

# expect date format to match expected strftime format YYYY-MM-DD
validator.expect_column_values_to_match_strftime_format('Subscription Date', "%Y-%m-%d")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the validator

# CELL ********************

# run the validator
validation_results = validator.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Handle results
# 
# 


# MARKDOWN ********************

# #### Define a function to write data to Lakehouse table (raw)

# CELL ********************

from datetime import datetime 
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# you might want to store this schema in a schemas Lakehouse table for easy retrieval
customers_schema = StructType(
    [StructField('Index', IntegerType(), False),
    StructField('CustomerId', StringType(), False),
    StructField('FirstName', StringType(), False),
    StructField('LastName', StringType(), False),
    StructField('Company', StringType(), False),
    StructField('City', StringType(), False),
    StructField('Country', StringType(), False),
    StructField('Phone1', StringType(), False),
    StructField('Phone2', StringType(), False),
    StructField('Email', StringType(), False),
    StructField('SubscriptionDate', DateType(), False),
    StructField('Website', StringType(), False)])

def write_data_to_lakehouse_table(file_path_to_be_validated, table_name, schema) -> None:
    ''' Input: file_path_to_be_validated (str), table_name (str)
    Function: Reads data from CSV file path, adds a record_creation_date field and writes to Lakehouse table. 
    Output: None 
    '''

    time_now = datetime.now() 
    trimmed_file_path = file_path_to_be_validated.replace('/lakehouse/default/','')
    customers_df = spark.read.option("header", True).option("inferSchema", "true").schema(schema).csv(trimmed_file_path)
    customers_df = customers_df.withColumn('record_creation_date', lit(time_now))
    customers_df.write.format("delta").mode("overwrite").save(f'Tables/{table_name}')

def move_file_to_validated_folder(): 
    ''' Not currently implemented
    '''
    pass 

def write_validation_results_centrally(): 
    ''' Not currently implemented
    '''
    pass 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This section calls the function(s) above, if the validation result is successful

# CELL ********************

def handle_sucess() -> None:
    ''' Function to handle a successful validation run
    This could include: 
    1) moving file to a validation_passed folder path
    2) write validation results to a central validation results lakehouse
    3) write file contents to raw lakehouse table
    '''

    # move_file_to_validated_folder() (not implemented)
    move_file_to_validated_folder()

    # write_validation_results_centrally() (not_implemented)
    write_validation_results_centrally()

    # write_data_to_lakehouse_table() 
    write_data_to_lakehouse_table(file_path_to_be_validated, output_table_name, customers_schema)
     

def handle_failure() -> None: 
    ''' Function to handle a failed validation run
    This could include: 
    1) custom logging
    2) write validation results to a central validation results lakehouse 
    '''
    # write_validation_results_centrally() (not_implemented)
    write_validation_results_centrally()

    pass
    
if validation_results.success == True: 
    print('Passed validation')
    handle_sucess() 
else: 
    handle_failure() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
