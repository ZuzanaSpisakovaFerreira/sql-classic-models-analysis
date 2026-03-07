# sql-classic-models-analysis
## **Project Goal**

The goal of this project is to transform raw data from sales system into a comprehensive **Master Dataset** ready for ML


## **1. Database Schema Overview & EER Diagram**
This project  uses the classicmodels database, a standard industry schema representing global sales operations. The system consists of eight interconnected tables, including customers, employees, offices, orderdetails, orders, payments, productlines and products. The database use the **InnoDB engine** with strict **Foreign Key constrains** to ensure the data remains accurate across all tables.

![Classic Models ER Diagram](database_schema.png)


## **2. ML Data Preparation (Customer -Level Pipeline)**

The objective of this project is to transform raw sales data into a comprehensive **Master Dataset** that is 100% ready Machine Learning use. Iam responsible for the **feature engineering and data architecture** required to make a Customer Segmentation model successful.

**The "Right Question" Logic**

Before starting, I had to answer a citical question: Which subject areas do we need to prepare? To support a segmentation use case, I identified and consolidated four key areas into  a single, customer-level view:
- **Customer Personal Details** (The Identity Layer)
- **Customer RFM Metrics** (Behavioral Layer)
- **Transaction Details** (Sales & Orders)
- **Order Line Details** (Product preferences)

- I specifically prepared this data at the **Customer Level** because segmentation requires every row to represent one unique customer, with all their details spread across multiple columns.
- ---------------------------------------------------------------------
**Building the Pipeline**


### **Part 1: Customer Identity & Demographics**
I estabilished the foundation by creating the `analysis.CustomerBasicInfo` table. This extracts the unique `customerNumber` and other identity and demographic features like `customerName, contactFirstName, contactLastName,phone, country, creditLimit`.

```
CREATE DATABASE IF NOT EXISTS analysis;

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
```

### **Part 2: Understanding Customer Buying Habits (RFM Analysis)**

This layer adds behavioral depth to the master dataset. While in Part 1 I estabilished who the customer is, this part fosuces on how they actually interact with the business. I used the **RFM method** (*Recency, Frequency, and Monetary) to prepare the most important features for a segmentation model.

- **Step-by-Step Logic:**
1. **Gathering Transaction Totals:** I joined the `orders`and `orderdetails`tables using the `orderNumber`to see the complete history for every customer.

2. **Calculating Key Values**: I calculated the total count of orders (**Frequency**) and the total lifetime (**Monetary**).

3. **Measuring Time (Recency):** I added a `recency`column to calculate the days passed since each customer`s `lastpurchasedate`.

4. **Managing Database Safety:** To perform these updates accurately, I briefly disabled "safemode" (`SET SQL_SAFE_UPDATES =0`) and reactivated it immediately after. 

5. **Deeper Behavioral Insights:** I created an additional table to calculate the **average order value** and **average quantity**. This helps a machine learning model distinguish between customers who place many small orders and those who make large, bulk purchases.

```
--1. Building the RFM foundation (Frequency and Monetary)

CREATE TABLE analysis.customerrfm AS
SELECT customerNumber,
MAX(O.OrderDate) as lastpurchasedate,
COUNT(O.orderNumber) as frequency,
SUM(OD.quantityOrdered * OD.PriceEach) as monetory
FROM classicmodels.orders O
JOIN classicmodels.orderdetails OD
ON O.orderNumber = OD.orderNumber
GROUP BY customerNumber;

--2. Calculating Recency
ALTER TABLE analysis.customerrfm
ADD recency INT;

-- Managing environmental safety to allow the update

SET SQL_SAFE_UPDATES=0;
UPDATE analysis.customerrfm
SET recency = datediff(CURDATE(),lastpurchasedate);

SET SQL_SAFE_UPDATES=1;

--3. Adding Additional Metrics (Averages)

SET SQL_SAFE_UPDATES=0;
CREATE TABLE analysis.customeradditionalmetric  AS
SELECT customerNumber, 
avg(OD.quantityOrdered * OD.PriceEach) AS averageordervalue,
avg(OD.quantityOrdered) AS averageorderquantity
FROM classicmodels.orders O
JOIN classicmodels.orderdetails OD
ON O.orderNumber = OD.orderNumber
GROUP BY customerNumber;
<img width="667" height="243" alt="image" src="https://github.com/user-attachments/assets/f9cf44da-42ed-4c7b-9674-52ff0789fcf8" />

```

### **Part 3:** Order Line (OL) & Producy Behavior

In this final technical phase I move from "how much" customers spend to **"what"**they are actually buying.
By creating order-line level details, I can identify which product categories a customer is most (or least) interested in.

- **Step-by-Step Logic:**
1. **Connecting the Details:** I joined three tables -`orders, orderdetails,` and `products`  to see the specific items associated with every customer

2. **Product Line Pivoting:** I used Pivot logic `(using SUM(CASE WHEN...))` to transform granular transaction rows into clean, customer-level columns. This allows us to see total spending across all seven product lines (such as `Classic Cars`, `Motorcycles`,`Planes`, etc.)

3. **The Results:**  I created the `analysis.customerproductlines`table, which provides a clear "Interest Profile" for every customer

---
```
-- I join 3 tables to pivot sales data into customer-level features.

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

SELECT * 
FROM analysis.customerproductlines;
```
---

### **FINAL STEP:** Assembling the Master Dataset

This is the most critical step of my project where I combine all four prepared tables into one Massive Dataset.

1. **Left Join Logic:** I used LEFT JOIN starting from the customerbasicinfo table. This insures that I retain all information from the original customer list.

2. **Data Integrity Check:** Important part of my process was veryfying the row counts. The final table must have the exact same number of rows as the basic info tables. This ensures that every customer is accounted for, regardless of their purchase history.

3. **The Results:** I created the `analysis.finaltableformachinelearning`. This table is now a high dimensional record where every row represents one unique customer, fully ready for analysis and machine learning

---
```
-- ASSEMLING THE MASTER DATASET FOR ML
-- Merging all layers/tables into he final "Master Dataset" using LEFT JOIN to ensure no customer is lost in the process.

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

--Final verification: This dataset is now 100% ready for Machine Learning use.

SELECT * FROM finaltableformachinelearning;
```
---

###**Conclusion:**
By transforming raw data into this structured **Master Dataset**, I have provided a verified, audit-reday foundation. 
This allows the bussiness to move directly into building a **Customer Segmentation** model with total confidencein the data´s integrity





## **3. Busines Insights (SQÇ Analysis)**
