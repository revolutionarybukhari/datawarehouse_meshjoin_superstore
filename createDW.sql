
drop schema if exists dw_project;
create schema dw_project;
use dw_project;
drop table if exists fact_table;
drop table if exists Customer_Table;
drop table if exists Time_Table;
drop table if exists Product_Table;
drop table if exists Store_Table;
drop table if exists Supplier_Table;



CREATE TABLE Product_Table (
    ProductID Varchar(255) NOT NULL PRIMARY KEY ,
    ProductName varchar(255),
    PRICE DOUBLE(5,2) DEFAULT 0.0
    
);

CREATE TABLE Customer_Table (
    CustomerID Varchar(255) NOT NULL PRIMARY KEY,
    CustomerName varchar(255)
);

CREATE TABLE Time_Table (
    TimeID varchar(255) NOT NULL PRIMARY KEY,
    day varchar(255),
    qtr int,
    month int,
    year integer,
    date DATE
);


CREATE TABLE Store_Table (
    StoreID Varchar(255) NOT NULL PRIMARY KEY ,
    StoreName varchar(30)
);

CREATE TABLE Supplier_Table (
    SupplierID Varchar(255) NOT NULL PRIMARY KEY ,
    SupplierName varchar(30)
);


CREATE TABLE fact_table (
TRANSACTION_ID DOUBLE(8,0),
    ProductID Varchar(30),
    CustomerID Varchar(30),
    TimeID Varchar(30),
    StoreID Varchar(30),
    SupplierID Varchar(30),
    Sales double,
    quantity int,
    primary key (TRANSACTION_ID,TimeID,ProductID,StoreID,CustomerID,SupplierID)

        

);
ALTER TABLE fact_table
ADD FOREIGN KEY (ProductID) REFERENCES Product_Table(ProductID);

ALTER TABLE fact_table
ADD FOREIGN KEY (CustomerID) REFERENCES Customer_Table(CustomerID);

ALTER TABLE fact_table
ADD FOREIGN KEY (TimeID) REFERENCES Time_Table(TimeID);

ALTER TABLE fact_table
ADD FOREIGN KEY (StoreID) REFERENCES Store_Table(StoreID);

ALTER TABLE fact_table
ADD FOREIGN KEY (SupplierID) REFERENCES Supplier_Table(SupplierID);

