CREATE TABLE [dbo].[Trips] (

	[vendorID] varchar(5) NULL, 
	[tpepPickupDateTime] datetime2(6) NULL, 
	[tpepDropoffDateTime] datetime2(6) NULL, 
	[passengerCount] int NULL, 
	[tripDistance] float NULL, 
	[puLocationId] varchar(5) NULL, 
	[doLocationId] varchar(5) NULL, 
	[startLon] float NULL, 
	[startLat] float NULL, 
	[endLon] float NULL, 
	[endLat] float NULL, 
	[rateCodeId] int NULL, 
	[storeAndFwdFlag] varchar(5) NULL, 
	[paymentType] varchar(5) NULL, 
	[fareAmount] float NULL, 
	[extra] float NULL, 
	[mtaTax] float NULL, 
	[improvementSurcharge] varchar(10) NULL, 
	[tipAmount] float NULL, 
	[tollsAmount] float NULL, 
	[totalAmount] float NULL
);

