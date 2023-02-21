-- In order to proceed with the development of next project, ensure that after having imported the data you change the name of the tables: so for the 
-- "NYC Property" excel file, please name it "properties_NYC" and for the "NYC Sales" excel file, please name it "sales_NYC".

SELECT TOP 100 *
FROM Portfolio.dbo.properties_NYC

SELECT TOP 100 *
FROM Portfolio.dbo.sales_NYC

-- Since we do not have a proper id between the 2 tables, we proceed to join both tables by the 'F1' column, that in spite of not being an Id
-- itself, we can combine it with the 'address' column inside the join to get the uniqueness character of the rows of the tables.
-- Additionally, it is worth mentioning that we use a INNER JOIN so that only the records that match data in the 2 tables are displayed.

SELECT *
FROM Portfolio.dbo.sales_NYC AS sales
INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] 

-- Now, one can proceed to explore the data contained in the join of the tables. However, it is important to take into account that the data
-- have not been cleaned yet, therefore we proceed to clean them.

-- Fill the empty fields of apartment number from the NYC_properties table with the data from the address column and correct the fields
-- with wrong information in a new apartment number column

SELECT [APARTMENT NUMBER], SUBSTRING([APARTMENT NUMBER], 1, CHARINDEX('.', [APARTMENT NUMBER]) +2)
FROM Portfolio.dbo.properties_NYC
WHERE [APARTMENT NUMBER] LIKE ('%.[0-9]%')

SELECT [APARTMENT NUMBER], CASE WHEN [APARTMENT NUMBER] LIKE ('%`%') THEN  'N/A' ELSE [APARTMENT NUMBER] END
FROM Portfolio.dbo.properties_NYC
WHERE [APARTMENT NUMBER] LIKE ('%`%')

SELECT [ADDRESS], [APARTMENT NUMBER], CASE WHEN CHARINDEX(',', ADDRESS) <> 0 AND [APARTMENT NUMBER] = '' THEN SUBSTRING(ADDRESS, CHARINDEX(',', ADDRESS) +2, LEN(ADDRESS))
										WHEN [APARTMENT NUMBER] = '' THEN 'N/A'
										WHEN [APARTMENT NUMBER] LIKE ('%.[0-9]%') THEN SUBSTRING([APARTMENT NUMBER], 1, CHARINDEX('.', [APARTMENT NUMBER]) +2)
										WHEN [APARTMENT NUMBER] LIKE ('%`%') THEN 'N/A'
										ELSE [APARTMENT NUMBER]
										END  ApartmentNumber_filled
FROM Portfolio.dbo.properties_NYC

-- Next it is created and filled a new column that will store the correct data of the apartment number

ALTER TABLE Portfolio.dbo.properties_NYC
ADD ApartmentNumber_new nvarchar(255);

UPDATE Portfolio.dbo.properties_NYC
SET ApartmentNumber_new = CASE WHEN CHARINDEX(',', ADDRESS) <> 0 AND [APARTMENT NUMBER] = '' THEN SUBSTRING(ADDRESS, CHARINDEX(',', ADDRESS) +2, LEN(ADDRESS))
										WHEN [APARTMENT NUMBER] = '' THEN 'N/A'
										WHEN [APARTMENT NUMBER] LIKE ('%.[0-9]%') THEN SUBSTRING([APARTMENT NUMBER], 1, CHARINDEX('.', [APARTMENT NUMBER]) +2)
										WHEN [APARTMENT NUMBER] LIKE ('%`%') THEN 'N/A'
										ELSE [APARTMENT NUMBER]
										END

SELECT [APARTMENT NUMBER], ApartmentNumber_new
FROM Portfolio.dbo.properties_NYC

-- Since there was one records changed by the CASE statement previously performed that was left empty, we correct it with the next query. This happened, because
-- the record had a comma at the end of the address record, but said comma was not followed by additional information regarding the apartment number of that address.

SELECT [APARTMENT NUMBER], ApartmentNumber_new
FROM Portfolio.dbo.properties_NYC
WHERE ApartmentNumber_new = ''

UPDATE Portfolio.dbo.properties_NYC
SET ApartmentNumber_new = 'N/A'
WHERE ApartmentNumber_new = ''

-- As previously seen, there are certain addresses that contain the apartment number indexed inside themselves, therefore, in order to make 
-- the column cleaner, one proceeds to split the address (of the properties tables) from those remaining apartment numbers

SELECT ADDRESS
FROM Portfolio.dbo.properties_NYC
WHERE CHARINDEX(',', ADDRESS) <> 0

ALTER TABLE Portfolio.dbo.properties_NYC
ADD Address_correct nvarchar(255);

SELECT ADDRESS, Address_correct, CASE WHEN CHARINDEX(',', ADDRESS) <> 0 THEN SUBSTRING(ADDRESS, 1, CHARINDEX(',', ADDRESS) -1) ELSE ADDRESS END
FROM Portfolio.dbo.properties_NYC
WHERE CHARINDEX(',', ADDRESS) <> 0

UPDATE Portfolio.dbo.properties_NYC
SET Address_correct = CASE WHEN CHARINDEX(',', ADDRESS) <> 0 THEN SUBSTRING(ADDRESS, 1, CHARINDEX(',', ADDRESS) -1) ELSE ADDRESS END

SELECT ADDRESS, Address_correct
FROM Portfolio.dbo.properties_NYC
WHERE CHARINDEX(',', ADDRESS) <> 0

-- Conversion of the SALE DATE field into a date datatype field

SELECT [SALE DATE], CONVERT(date, [SALE DATE])
FROM Portfolio.dbo.sales_NYC

ALTER TABLE Portfolio.dbo.sales_NYC
ADD SaleDate_converted date;

UPDATE Portfolio.dbo.sales_NYC
SET SaleDate_converted = CONVERT(date, [SALE DATE])

SELECT [SALE DATE], SaleDate_converted
FROM Portfolio.dbo.sales_NYC

-- Conversion of the SALE PRICE field into a number datatype field

SELECT [SALE PRICE], CONVERT(bigint, [SALE PRICE])
FROM Portfolio.dbo.sales_NYC

ALTER TABLE Portfolio.dbo.sales_NYC
ADD SalePrice_converted bigint;

UPDATE Portfolio.dbo.sales_NYC
SET SalePrice_converted = CONVERT(bigint, [SALE PRICE])

-- Find duplicate rows

-- In order to identify which records are duplicated, a query is made to help identify which rows are in fact duplicated.For that We utilize
-- the windows function ROW_NUMBER partitioned by the parameters that are to a certain extent 'unique' to the property sold, in this case:
-- apartment number (completed with the information from the address field), sale date (converted to date datatype from the NYC_sales table), 
-- address and lot) and these partitions are ordered by the address and the sale price of the sale. To do this, it is required to use joins 
-- between the table NYC_properties and the table NYC_sales. Later, we will use this information in a temporary table, to remove said duplicates.

WITH Dup_CTE AS (
			SELECT sales.[F1] AS Sales_ID, sales.SaleDate_converted, properties.Address_correct, properties.[LOT],
					ROW_NUMBER () OVER (PARTITION BY properties.ApartmentNumber_new, sales.SaleDate_converted, properties.Address_correct,
					properties.[LOT]  ORDER BY properties.Address_correct, sales.[SALE PRICE] DESC) ROWS_DUP
			FROM Portfolio.dbo.sales_NYC AS sales
			INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] 
)
SELECT *
FROM Dup_CTE
WHERE ROWS_DUP > 1 

-- Replacing blank values

SELECT *
FROM Portfolio.dbo.properties_NYC
WHERE [TAX CLASS AT PRESENT] IN ('')

UPDATE Portfolio.dbo.properties_NYC
SET [TAX CLASS AT PRESENT] = 'N/A'
WHERE [TAX CLASS AT PRESENT] IN ('')

-- Determination of the building's age at the moment of sale
-- Since the data type of the field 'Year Built' is float, the year is extracted of the sale data, so that this year
-- value is afterwards turned into a float value, to perform the calculation.

SELECT properties.[YEAR BUILT], YEAR(sales.SaleDate_converted) AS Sale_Year, CONVERT(float, YEAR(SaleDate_converted)) -  properties.[YEAR BUILT] AS AgeOfBuilding_AtMomentOfSale
FROM Portfolio.dbo.sales_NYC AS sales
INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] 

SELECT properties.[YEAR BUILT], YEAR(sales.SaleDate_converted) AS Sale_Year, CONVERT(float, YEAR(SaleDate_converted)) -  properties.[YEAR BUILT] AS AgeOfBuilding_AtMomentOfSale,  
		CASE WHEN properties.[YEAR BUILT] = 0 THEN 'N/A' ELSE CONVERT(nvarchar, CONVERT(float, YEAR(SaleDate_converted)) -  properties.[YEAR BUILT]) END AgeOfBuilding_corrected
FROM Portfolio.dbo.sales_NYC AS sales
INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] 

ALTER TABLE Portfolio.dbo.sales_NYC
ADD AgeOfBuilding_AtMomentOfSale nvarchar(255);

UPDATE Portfolio.dbo.sales_NYC
SET AgeOfBuilding_AtMomentOfSale = CASE WHEN properties.[YEAR BUILT] = 0 THEN 'N/A' ELSE CONVERT(nvarchar, CONVERT(float, YEAR(SaleDate_converted)) -  properties.[YEAR BUILT]) END
FROM Portfolio.dbo.sales_NYC AS sales
INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS]

-- Replace Zip codes = 0

SELECT *
FROM Portfolio.dbo.properties_NYC
WHERE [ZIP CODE] = 0

ALTER TABLE Portfolio.dbo.properties_NYC
ADD ZIP_CODE_Converted nvarchar(255)

UPDATE Portfolio.dbo.properties_NYC
SET ZIP_CODE_Converted = CAST([ZIP CODE] AS nvarchar)

SELECT [ZIP CODE], REPLACE(ZIP_CODE_Converted, '0', 'N/A')
FROM Portfolio.dbo.properties_NYC
WHERE [ZIP CODE] = 0

UPDATE Portfolio.dbo.properties_NYC
SET ZIP_CODE_Converted = REPLACE(ZIP_CODE_Converted, '0', 'N/A')
WHERE [ZIP CODE] = 0

-- Now that the zip code field has been reformed so that instead of zeroes, one would get 'N/A' for the row, the original zip code could be deleted
-- with the next query

--ALTER TABLE Portfolio.dbo.NYC_sales
--DROP COLUMN [ZIP CODE]

-- Provided that there are values of the columns 'Land Square Feet' and 'Gross Square Feet' that have values = 0, it would make more sense to have them
-- as 'NULL' values when they are not given, since these columns should remain with a number datatype such as float (the default) or int, instead 
-- with a zero value. With that in mind, every field with value = 0 in the 'Land Square Feet' or 'Gross Square Feet', will be replaced with a NULL value.

SELECT [LAND SQUARE FEET], CASE WHEN [LAND SQUARE FEET] = 0 THEN NULL ELSE [LAND SQUARE FEET] END
FROM Portfolio.dbo.properties_NYC

UPDATE Portfolio.dbo.properties_NYC
SET [LAND SQUARE FEET] = CASE WHEN [LAND SQUARE FEET] = 0 THEN NULL ELSE [LAND SQUARE FEET] END

SELECT [GROSS SQUARE FEET], CASE WHEN [GROSS SQUARE FEET] = 0 THEN NULL ELSE [GROSS SQUARE FEET] END
FROM Portfolio.dbo.properties_NYC

UPDATE Portfolio.dbo.properties_NYC
SET [GROSS SQUARE FEET] = CASE WHEN [GROSS SQUARE FEET] = 0 THEN NULL ELSE [GROSS SQUARE FEET] END

-- Since there is a large number of records with the columns 'Residential Units', 'Commercial Units' and 'Total Units' equal to zero,
-- These values make more sense to have them as NULL instead as zeroes in case that both the 'Residential Units' and 'Commercial Units' are equal to zero.
-- Otherwise, when one of the columns actually has a not null value different from zero, it is better to keep the 'null' value as zero.

SELECT [RESIDENTIAL UNITS], [COMMERCIAL UNITS], [TOTAL UNITS]
FROM Portfolio.dbo.properties_NYC
WHERE [TOTAL UNITS] = 0

SELECT [RESIDENTIAL UNITS], [COMMERCIAL UNITS], [TOTAL UNITS]
FROM Portfolio.dbo.properties_NYC
WHERE [RESIDENTIAL UNITS] = 0

--First, residential units is modified

SELECT F1, [RESIDENTIAL UNITS], [COMMERCIAL UNITS], [TOTAL UNITS], CASE WHEN [RESIDENTIAL UNITS] = 0 AND [COMMERCIAL UNITS] = 0 AND [TOTAL UNITS] = 0 THEN NULL
												WHEN [RESIDENTIAL UNITS] = 0  AND ([COMMERCIAL UNITS] <> 0 AND [TOTAL UNITS] <> 0) THEN 0
												ELSE [RESIDENTIAL UNITS] END
FROM Portfolio.dbo.properties_NYC
WHERE [RESIDENTIAL UNITS] IS NULL

UPDATE Portfolio.dbo.properties_NYC
SET [RESIDENTIAL UNITS] = CASE WHEN [RESIDENTIAL UNITS] = 0 AND [COMMERCIAL UNITS] = 0 AND [TOTAL UNITS] = 0 THEN NULL
												WHEN [RESIDENTIAL UNITS] = 0  AND ([COMMERCIAL UNITS] <> 0 AND [TOTAL UNITS] <> 0) THEN 0
												ELSE [RESIDENTIAL UNITS] END

-- Second, commercial units is modified

SELECT [RESIDENTIAL UNITS], [COMMERCIAL UNITS],[TOTAL UNITS], CASE WHEN [COMMERCIAL UNITS] = 0 AND ([RESIDENTIAL UNITS] = 0 OR [RESIDENTIAL UNITS] IS NULL) AND [TOTAL UNITS] = 0 THEN NULL
												WHEN [COMMERCIAL UNITS] = 0 AND (([RESIDENTIAL UNITS] <> 0 OR [RESIDENTIAL UNITS] IS NOT NULL) AND [TOTAL UNITS] <> 0) THEN 0
												ELSE [COMMERCIAL UNITS] END
FROM Portfolio.dbo.properties_NYC
WHERE [COMMERCIAL UNITS] = 0

UPDATE Portfolio.dbo.properties_NYC
SET [COMMERCIAL UNITS] = CASE WHEN [COMMERCIAL UNITS] = 0 AND ([RESIDENTIAL UNITS] = 0 OR [RESIDENTIAL UNITS] IS NULL) AND [TOTAL UNITS] = 0 THEN NULL
												WHEN [COMMERCIAL UNITS] = 0 AND (([RESIDENTIAL UNITS] <> 0 OR [RESIDENTIAL UNITS] IS NOT NULL) AND [TOTAL UNITS] <> 0) THEN 0
												ELSE [COMMERCIAL UNITS] END

-- In third place, we proceed to change the zero numbers from the 'Total Units' column

SELECT [RESIDENTIAL UNITS], [COMMERCIAL UNITS], [TOTAL UNITS]
FROM Portfolio.dbo.properties_NYC
WHERE [TOTAL UNITS] IN (NULL, 0)

SELECT [RESIDENTIAL UNITS], [COMMERCIAL UNITS], [TOTAL UNITS], CASE WHEN [TOTAL UNITS] = 0 AND ([RESIDENTIAL UNITS] = 0 OR [RESIDENTIAL UNITS] IS NULL) AND ([COMMERCIAL UNITS] = 0 OR [COMMERCIAL UNITS] IS NULL) THEN NULL
																ELSE [TOTAL UNITS] END
FROM Portfolio.dbo.properties_NYC
--WHERE [TOTAL UNITS] IN (NULL, 0)


UPDATE Portfolio.dbo.properties_NYC
SET [TOTAL UNITS] = CASE WHEN [TOTAL UNITS] = 0 AND ([RESIDENTIAL UNITS] = 0 OR [RESIDENTIAL UNITS] IS NULL) AND ([COMMERCIAL UNITS] = 0 OR [COMMERCIAL UNITS] IS NULL) THEN NULL
																ELSE [TOTAL UNITS] END

-- Delete unused columns
-- Since the the column 'EASE-MENT' does not have any data whatsoever, it is deleted with the following query

SELECT TOP 100 *
FROM Portfolio.dbo.properties_NYC

ALTER TABLE Portfolio.dbo.properties_NYC
DROP COLUMN [EASE-MENT]

-- Remove duplicates
-- As previously mentioned, we will find the duplicates and then delete them.
-- In order o make an exploration of the data we proceed to create a coyp of the data in a temp table where the duplicates can be removed, and then get a 
-- better understanding of the information that the dataset holds.

SELECT sales.F1, properties.BOROUGH, properties.NEIGHBORHOOD, properties.[BUILDING CLASS CATEGORY], properties.[BLOCK], properties.[LOT],
	properties.Address_correct, properties.ApartmentNumber_new, properties.[RESIDENTIAL UNITS], properties.[COMMERCIAL UNITS], 
	properties.[TOTAL UNITS], properties.[LAND SQUARE FEET], properties.[GROSS SQUARE FEET], properties.ZIP_CODE_Converted,
	properties.[BUILDING CLASS AT PRESENT], sales.[BUILDING CLASS AT TIME OF SALE], properties.[TAX CLASS AT PRESENT], sales.[TAX CLASS AT TIME OF SALE],
	sales.SaleDate_converted, properties.[YEAR BUILT], sales.AgeOfBuilding_AtMomentOfSale, sales.SalePrice_converted,
	ROW_NUMBER () OVER (PARTITION BY properties.ApartmentNumber_new, sales.SaleDate_converted, properties.Address_correct,
								properties.[LOT]  ORDER BY properties.Address_correct, sales.[SALE PRICE] DESC) ROWS_DUP
FROM Portfolio.dbo.sales_NYC AS sales
INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] 

-- We proceed to create a temp table that stores the data from the previous query

DROP TABLE IF EXISTS #Temp_PropertiesSales_NYC

Create TABLE #Temp_PropertiesSales_NYC (
F1 float,
Borough float,
Neighborhood nvarchar(255),
BuildingClassCategory nvarchar(255),
Block float,
Lot float,
AddressCorrect nvarchar(255),
ApartmentNumberCorrect nvarchar(255),
ResidentialUnits float,
CommercialUnits float,
TotalUnits float,
LandSquareFeet float,
GrossSquareFeet float,
ZipCodeConverted nvarchar(255),
BuildingClassAtPresent nvarchar(255),
BuildingClassAtTimeOfSale nvarchar(255),
TaxClassAtPresent nvarchar(255),
TaxClassAtTimeOfSale float,
SaleDateCorrect date,
YearBuilt float,
AgeOfBuildingAtSale nvarchar(255),
SalePriceConverted bigint,
NumberOfRepetitions int
)

INSERT INTO #Temp_PropertiesSales_NYC
SELECT *
FROM (SELECT sales.F1, properties.BOROUGH, properties.NEIGHBORHOOD, properties.[BUILDING CLASS CATEGORY], properties.[BLOCK], properties.[LOT],
	properties.Address_correct, properties.ApartmentNumber_new, properties.[RESIDENTIAL UNITS], properties.[COMMERCIAL UNITS], 
	properties.[TOTAL UNITS], properties.[LAND SQUARE FEET], properties.[GROSS SQUARE FEET], properties.ZIP_CODE_Converted,
	properties.[BUILDING CLASS AT PRESENT], sales.[BUILDING CLASS AT TIME OF SALE], properties.[TAX CLASS AT PRESENT], sales.[TAX CLASS AT TIME OF SALE],
	sales.SaleDate_converted, properties.[YEAR BUILT], sales.AgeOfBuilding_AtMomentOfSale, sales.SalePrice_converted,
	ROW_NUMBER () OVER (PARTITION BY properties.ApartmentNumber_new, sales.SaleDate_converted, properties.Address_correct,
								properties.[LOT]  ORDER BY properties.Address_correct, sales.[SALE PRICE] DESC) ROWS_DUP
	FROM Portfolio.dbo.sales_NYC AS sales
	INNER JOIN Portfolio.dbo.properties_NYC AS properties ON sales.F1 = properties.F1 AND sales.[ADDRESS] = properties.[ADDRESS] ) Sub

-- Now that the copy of the data has been created, next step is to remove the data contained within said copy. First, we verify the data

SELECT *
FROM #Temp_PropertiesSales_NYC

-- Then, we select the data that we are interested in deleting

SELECT *
FROM #Temp_PropertiesSales_NYC
WHERE NumberOfRepetitions > 1

-- Finally, we delete it. And with the previous 2 queries, one can confirm that the data we meant to delete was actually deleted.

DELETE
FROM #Temp_PropertiesSales_NYC
WHERE NumberOfRepetitions > 1

-- EXPLORATION

-- Now, we can continue with the exploration of the cleaned data

SELECT * 
FROM #Temp_PropertiesSales_NYC

-- A client has asked us to determine the total amount of money spent per borough and Negihborhood
SELECT *
FROM #Temp_PropertiesSales_NYC

SELECT Borough, SUM(SalePriceConverted)
FROM #Temp_PropertiesSales_NYC
GROUP BY Borough

SELECT Borough, Neighborhood, AVG(SalePriceConverted) OVER (PARTITION BY Neighborhood ORDER BY Neighborhood)AS AverageMoneySpentPerNeighborhood ,
							  SUM(SalePriceConverted) OVER (PARTITION BY Neighborhood ORDER BY Neighborhood) AS TotalMoneySpentPerNeighborhood
FROM #Temp_PropertiesSales_NYC

-- Area per Unit of property

SELECT TotalUnits, LandSquareFeet,  LandSquareFeet/TotalUnits AS AreaPerUnitOfProperty
FROM #Temp_PropertiesSales_NYC
WHERE TotalUnits IS NOT NULL

-- Client asks us to look for properties where there are more than 50 units whether they are residential or commercial and their corresponding average price historically.

SELECT AddressCorrect, SUM(ResidentialUnits) TotalResidentialUnits, SUM(CommercialUnits) TotalCommercialUnits, SUM(ResidentialUnits) + SUM(CommercialUnits) AS TotalNumberOfUnits,
AVG(SalePriceConverted) AveragePrice
FROM #Temp_PropertiesSales_NYC
GROUP BY AddressCorrect
HAVING (SUM(ResidentialUnits)+SUM(CommercialUnits)) > 50
ORDER BY AveragePrice

-- Average number of units per Building class category

SELECT BuildingClassCategory, AVG(TotalUnits)
FROM #Temp_PropertiesSales_NYC
GROUP BY BuildingClassCategory

-- The client has requested to see buildings built only after the year 2010, belonging to tax class 2 and a price under $ 150.000

SELECT *
FROM #Temp_PropertiesSales_NYC
WHERE TaxClassAtPresent LIKE '2%' AND YearBuilt > 2010 AND SalePriceConverted <= 150000
ORDER BY SalePriceConverted

-- A client is interested in knowing the ranking 5 of the most expensive properties sold per borough

WITH Ranks AS (
SELECT*,  RANK() OVER(PARTITION BY Borough ORDER BY SalePriceConverted DESC) AS ranking
FROM #Temp_PropertiesSales_NYC
)
SELECT * 
FROM Ranks 
WHERE ranking BETWEEN 1 AND 5
ORDER BY Borough