CREATE TABLE [dbo].[SalesOrderHeader] (

	[OrderID] int NULL, 
	[CustomerID] int NULL, 
	[OrderDate] datetime2(6) NULL, 
	[TotalAmount] decimal(10,2) NULL, 
	[Status] varchar(50) NULL, 
	[SalesPersonID] int NULL
);

