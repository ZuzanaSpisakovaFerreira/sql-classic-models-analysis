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
I estabilished the foundation by creating the ```analysis.CustomerBasicInfo``` table. This extracts the unique ```customerNumber```and other identity and demographic features like ```customerName, contactFirstName, contactLastName,phone, country, creditLimit``` 

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

### **Part 2:** Transcactional Intelligence & RFM Analysis

### **Part 3:** Order Line (OL) & Producy Behavior


## **3. Busines Insights (SQÇ Analysis)**
