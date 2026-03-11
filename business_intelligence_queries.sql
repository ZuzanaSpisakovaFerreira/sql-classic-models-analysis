USE classicmodels;

#1: Tracking Active Orders
SELECT orderNumber, status, orderDate, customerNumber
FROM orders
WHERE status NOT IN ('Shipped', 'Resolved');

SELECT COUNT(*) AS total_active_orders
FROM orders
WHERE status NOT IN ('Shipped', 'Resolved');

#2: Geographic Market Footprint
SELECT country, COUNT(*) AS totalCustomers
FROM customers
GROUP BY country
ORDER BY totalCustomers DESC;

#3: Inventory Risk: Low Stock Audit
SELECT productName, productLINE, quantityInStock
FROM products
WHERE quantityInStock < 1000
ORDER BY quantityInStock ASC;


#4: Financial Audit: Spend vs. Credit Limit Discrepancy
SELECT c.customerName, c.creditLimit, SUM(p.amount) AS totalSpent
FROM customers c
JOIN payments p ON c.customerNumber=p.customerNumber
GROUP BY c.customerName, c.creditLimit
HAVING totalSpent> c.creditLimit;

#5: Sales Representative KPI Performance
SELECT CONCAT(e.firstName,'',e.lastName) AS salesRep,
	   SUM(p.amount) AS totalRevenue
FROM employees e
JOIN customers c ON e.employeeNumber=c.salesRepEmployeeNumber
JOIN payments p ON c.customerNumber = p.customerNumber
GROUP BY salesRep
ORDER BY totalRevenue DESC;

#6:Revenue Performance by Customer (Top 5)
SELECT c.customerName, SUM(p.amount) AS totalPaid
FROM customers c
JOIN payments p ON c.customerNumber = p.customerNumber
GROUP BY c.customerName
ORDER BY totalPaid DESC
LIMIT 5;

#7: Logistics Audit: Late Shimpments & Ownership
SELECT o.orderNumber, c.customerName, 
       CONCAT(e.firstName, ' ', e.lastName) AS salesRepName
FROM orders o
JOIN customers c ON o.customerNumber = c.customerNumber
JOIN employees e ON c.salesRepEmployeeNumber = e.employeeNumber
WHERE o.shippedDate > o.requiredDate;

#8: Market Segmentation: Credit Risk Analysis
SELECT 
    CASE 
        WHEN creditLimit > 100000 THEN 'Platinum'
        WHEN creditLimit BETWEEN 50000 AND 100000 THEN 'Gold'
        ELSE 'Silver'
    END AS customerTier,
    COUNT(*) AS customerCount
FROM customers
GROUP BY customerTier;

#9: Highest Revenue by Product Line
SELECT p.productLine, SUM(od.quantityOrdered * od.priceEach) AS totalRevenue
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
GROUP BY p.productLine
ORDER BY totalRevenue DESC;


#10: Organizational Hierarchy Audit (Self-Join)
SELECT 
    CONCAT(e.firstName, ' ', e.lastName) AS employeeName,
    e.jobTitle,
    CONCAT(m.firstName, ' ', m.lastName) AS reportsToManager
FROM employees e
LEFT JOIN employees m ON e.reportsTo = m.employeeNumber;




