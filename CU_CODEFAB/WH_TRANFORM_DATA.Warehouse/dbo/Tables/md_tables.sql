CREATE TABLE [dbo].[md_tables] (

	[ID] int NOT NULL, 
	[integration_name] varchar(100) NULL, 
	[source_table_name] varchar(100) NULL, 
	[source_table_schema] varchar(100) NULL, 
	[source_connection] varchar(100) NULL, 
	[source_connection_type] varchar(255) NULL, 
	[source_database] varchar(255) NULL, 
	[source_data_store_type] varchar(255) NULL, 
	[source_workspace_data_store_type] varchar(255) NULL, 
	[data_store] varchar(255) NULL, 
	[source_root_folder] varchar(255) NULL, 
	[use_query] varchar(255) NULL, 
	[source_table_extraction_mode] varchar(255) NULL, 
	[target_table_name] varchar(255) NULL, 
	[target_data_store_type] varchar(100) NULL, 
	[target_workspace_data_store_type] varchar(100) NULL, 
	[target_root_folder] varchar(100) NULL, 
	[target_table_alias] varchar(100) NULL, 
	[target_table_schema] varchar(100) NULL, 
	[target_connection_dataset] varchar(100) NULL, 
	[target_connection_linked_service] varchar(100) NULL, 
	[refresh_frequency] varchar(255) NULL, 
	[landing_zone_enabled] bit NULL, 
	[bronze_layer_enabled] bit NULL, 
	[silver_layer_enabled] bit NULL, 
	[gold_layer_enabled] bit NULL, 
	[last_successful_load_time_landing] datetime2(6) NULL, 
	[last_successful_load_time_bronze] datetime2(6) NULL, 
	[last_successful_load_time_silver] datetime2(6) NULL, 
	[last_successful_load_time_gold] datetime2(6) NULL, 
	[comments] varchar(100) NULL, 
	[target_file_system] varchar(100) NULL, 
	[target_directory] varchar(100) NULL, 
	[target_file_name] varchar(100) NULL, 
	[target_lakehouse] varchar(100) NULL, 
	[bronze_layer_file_system] varchar(100) NULL, 
	[bronze_layer_directory] varchar(100) NULL, 
	[silver_layer_file_system] varchar(100) NULL, 
	[silver_layer_directory] varchar(100) NULL, 
	[bronze_layer_lakehouse] varchar(100) NULL, 
	[silver_layer_lakehouse] varchar(100) NULL
);

