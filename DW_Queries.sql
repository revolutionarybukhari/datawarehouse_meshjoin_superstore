-- Question 1
select st.StoreName as StoreName , sum(ft.Sales) as Sales
from fact_table as ft  
join store_table st on (ft.StoreID=st.StoreID)
join time_table as tt on (ft.TimeID=tt.TimeID) where tt.month=9 and tt.year=2017
group by st.StoreID
order by Sales desc limit 3; 

-- Question 2 
select sp.SupplierName as SupplierName , sum(ft.Sales) as Sales
from fact_table as ft  
join supplier_table sp on (ft.SupplierID=sp.SupplierID)
join time_table as tt on (ft.TimeID=tt.TimeID) where tt.day='Sunday' or tt.day='Saturday'
group by sp.SupplierID
order by Sales desc limit 10; 

-- We will check for all the weekends the top suppliers and after getting that we can calculate the probability for each supplier and the one with the highest probability will be most likely to be the highest again 
-- We can use this technique for the predictions/forcast!

-- Question 3
select sp.SupplierName as SupplierName , sum(ft.Sales) as Sales,tt.month,tt.qtr as Quater
from fact_table as ft  
join supplier_table sp on (ft.SupplierID=sp.SupplierID)
join time_table as tt on (ft.TimeID=tt.TimeID) 
group by tt.month,sp.SupplierID,tt.qtr; 

-- Question 4 

select st.StoreName as StoreName ,p.ProductName as ProductName, sum(ft.Sales) as Sales
from fact_table as ft  
join store_table st on (ft.StoreID =st.StoreID)
join product_table as p on (ft.ProductID=p.ProductID) 
group by st.StoreID,p.ProductID; 

-- Question 5 

select st.StoreName as StoreName,tt.qtr as quarter , sum(ft.Sales) as sales
from fact_table as ft  
join store_table st on (ft.StoreID =st.StoreID)
join time_table as tt on (ft.TimeID=tt.TimeID) 
group by st.StoreID,tt.year,tt.qtr;



SELECT s.StoreName,
       Sum(CASE
             WHEN tt.qtr = 1 THEN SALES
             ELSE 0
           end) AS "Qtr_1",
       Sum(CASE
             WHEN tt.qtr = 2 THEN SALES
             ELSE 0
           end) AS "Qtr_2",
       Sum(CASE
             WHEN tt.qtr = 3 THEN SALES
             ELSE 0
           end) AS "Qtr_3",
       Sum(CASE
             WHEN tt.qtr = 4 THEN SALES
             ELSE 0
           end) AS "Qtr_4"
FROM   fact_table as ft JOIN store_table s on (ft.StoreID =s.StoreID) JOIN time_table tt on (ft.TimeID=tt.TimeID) 
GROUP  BY   tt.YEAR , StoreName;


--  Question 6
 select p.ProductName as ProductName , sum(ft.quantity) as Total_Quantity
from fact_table as ft  
join product_table p on (ft.ProductID=p.ProductID)
join time_table as tt on (ft.TimeID=tt.TimeID) where tt.day='Sunday' or tt.day='Saturday'
group by p.ProductID
order by Total_Quantity desc limit 5; 
 
-- Question 7
-- If any arguments have a super-aggregate NULL value, the result of such GROUPING() is non-zero.
-- In this case, it will return only the super-aggregate rows and filter the regular grouped rows using the following query:
SELECT st.StoreName as StoreName,sp.SupplierName as SupplierName,p.ProductName as ProductName 
FROM store_table st, supplier_table sp, product_table p , fact_table ft 
where ft.ProductID=p.ProductID and ft.SupplierID=sp.SupplierID and ft.StoreID=st.StoreID
GROUP BY st.StoreID, sp.SupplierID,p.ProductID WITH ROLLUP
HAVING GROUPING(st.StoreID, sp.SupplierID,p.ProductID) <>0; 

-- it shows the data of  store,supplier and  product, lastly it aggregrates and shows  the results to avoid duplications and give a collective aggregated output 

-- Question 8
 
 
select p.ProductName as ProductName ,Sum(CASE
             WHEN tt.qtr = 1 or tt.qtr = 2 THEN SALES
             ELSE 0
           end) AS "First Half",
       Sum(CASE
             WHEN tt.qtr = 3 or tt.qtr = 4 THEN SALES
             ELSE 0
           end) AS "Second Half", 
           sum(case
		when  tt.qtr = 1 or tt.qtr = 2 or tt.qtr = 3 or tt.qtr = 4
        Then SALES 
        Else 0
	END
) as total_yearly_sales

from fact_table as ft  
join product_table p on (ft.ProductID=p.ProductID)
join time_table as tt on (ft.TimeID=tt.TimeID) where  tt.year=2017
group by p.ProductID; 


-- Question 9

Select p.ProductName , count(p.ProductName) from product_table p group by   p.ProductName;
-- We see that tomatoes has a repetition it has a count 2.
 
Select * from product_table where ProductName = "Tomatoes";
-- We can see this product has two different prices that is not feasible hence this is an anomoly.

-- Question 10
DROP VIEW IF EXISTS STORE_PRODUCT_ANALYSIS;
create view STORE_PRODUCT_ANALYSIS as
(
select st.StoreName,p.ProductName,sum(ft.sales) as sales
from fact_table ft, store_table st , product_table p 
where ft.StoreID=st.StoreID and ft.ProductID=p.ProductID
group by st.StoreName,p.ProductName
order by st.StoreName , p.ProductName
);

select * from STORE_PRODUCT_ANALYSIS;

-- It helps in reducing execution time for the complex queries.
-- It helps in precalculating expensive join and aggregation operations on the databas.
-- prior to execution and storing the results in the database  

