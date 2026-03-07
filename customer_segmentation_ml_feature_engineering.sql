/*PROJECT: Master Dataset Preparation for ML (Customer Segmentation)
AUTHOR: Zuzana Spisakova Ferreira
GOAL: Transform raw sales data into a "Master Dataset" for Machine Learning.
*/
------------------------------------------------------------------------------------------------------------------
/* 
PART 1: Customer Identity & Demographics
 */ 
------------------------------------------------------------------------------------------------------------------
-- setting up analysis environmnet and source database

CREATE DATABASE IF NOT EXISTS analysis;
USE classicmodels;

-- Build the first layer of the master dataset: Identity Layer

SELECT customerName, customerNumber, contactFirstName, contactLastName,phone, country, creditLimit
FROM customers;

DROP TABLE IF EXISTS analysis.CustomerBasicInfo;

CREATE TABLE analysis.CustomerBasicInfo
AS
SELECT 
	customerNumber,
	customerName, 
    contactFirstName, 
    contactLastName,
    phone,
    country,
    creditLimit
FROM customers;

-- Veryfying Identity Layer

SELECT * FROM analysis.customerbasicinfo;
-------------------------------------------------------------------------------------
/*
PART 2: Transactional Intelligence & RFM Analysis
*/
--------------------------------------------------------------------------------------
-- 2.1: Creating the RFM table
-- Aggregating purchase history to calculate Frequency and Monetary
-- Logic: Joing orders and orderdetails on 'orderNumber'

DROP TABLE IF EXISTS analysis.customerrfm;


CREATE TABLE analysis.customerrfm AS
SELECT customerNumber,
MAX(O.OrderDate) as lastpurchasedate,
COUNT(O.orderNumber) as frequency,
SUM(OD.quantityOrdered * OD.PriceEach) as monetory
FROM classicmodels.orders O
JOIN classicmodels.orderdetails OD
ON O.orderNumber = OD.orderNumber
GROUP BY customerNumber;


-- 2.2: Calculating Recency
-- Adding a column to measure time elapsed since the last purchase

ALTER TABLE analysis.customerrfm
ADD recency INT;

-- Managing Safe Updates to allow the recency calculation

SET SQL_SAFE_UPDATES=0;
UPDATE analysis.customerrfm
SET recency = datediff(CURDATE(),lastpurchasedate);

SET SQL_SAFE_UPDATES=1;


-- 2.3: Additional Transactional Metrcics
-- Adding depth to the customer profile with average order values and quantities

DROP TABLE IF EXISTS analysis.customeradditionalmetric;

CREATE TABLE analysis.customeradditionalmetric  AS
SELECT customerNumber, 
avg(OD.quantityOrdered * OD.PriceEach) AS averageordervalue,
avg(OD.quantityOrdered) AS averageorderquantity
FROM classicmodels.orders O
JOIN classicmodels.orderdetails OD
ON O.orderNumber = OD.orderNumber
GROUP BY customerNumber;

-- Final verification of Part 2 behavior metrics

SELECT * FROM analysis.customerrfm;
SELECT * FROM analysis.customeradditionalmetric;

-------------------------------------------------------------------------------------
/*
PART 3: PRODUCT INTERESTS (Pivot Table)
*/
--------------------------------------------------------------------------------------
-- Purpose: Identifying spending patterns across different product lines.
-- Logic: Joining 3 tables (Orders, OrderDetails, Products) to pivot sales data.

DROP TABLE IF EXISTS analysis.customerproductlines;

CREATE TABLE analysis.customerproductlines
SELECT customerNumber,
SUM(CASE WHEN productline = 'Classic Cars'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS ClassiccarsSales,
SUM(CASE WHEN productline = 'Motorcycles'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS MotorcycleSsales,
SUM(CASE WHEN productline = 'Planes'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS PlaneSales,
SUM(CASE WHEN productline = 'Ships'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS ShipsSales,
SUM(CASE WHEN productline = 'Trains'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS TrainsSales,
SUM(CASE WHEN productline = 'Trucks and Buses'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS TruckandBusesSales,
SUM(CASE WHEN productline = 'Vintage Cars'
THEN OD.quantityOrdered *OD.PriceEach ELSE 0 END)
AS VintageCarsSales
FROM
classicmodels.orders O
JOIN classicmodels.orderdetails OD
on O.orderNumber= OD.orderNumber
JOIN classicmodels. products P
ON P.productCode= OD.productCode
GROUP BY customerNumber;

-- Verification of Part 3
SELECT * 
FROM analysis.customerproductlines;

-------------------------------------------------------------------------------------
/*
FINAL STEP: ASSEMBLING THE MASTER DATASET FOR MACHINE LEARNING
*/
------------------------------------------------------------------------------------
-- Purpose: Creating the "Main Dataset" by merging all  created tables.
-- Logic: Using LEFT JOIN to ensure we retain all customers from our Identity Layer.

DROP TABLE IF EXISTS analysis.finaltableformachinelearning;


CREATE TABLE analysis.finaltableformachinelearning
SELECT cf.* ,
rfm.frequency, 
rfm.monetary,rfm.recency,
amc.averageordervalue,
amc.averageorderquantity,
cpl.ClassiccarsSales,
cpl.MotorcycleSsales,
cpl.PlaneSales,
cpl.ShipsSales,
cpl.TrainsSales,
cpl.TruckandBusesSales,
cpl.VintageCarsSales
from customerbasicinfo cf
LEFT JOIN customerrfm rfm
on cf.customerNumber= rfm.customerNumber
LEFT JOIN customeradditionalmetric amc
on cf.customerNumber= amc.customerNumber
LEFT JOIN customerproductlines cpl
on cf.customerNumber= cpl.customerNumber;

-- Final verification: Confirming 100% accuracy in customer counts and data integrity

SELECT * 
FROM finaltableformachinelearning;


