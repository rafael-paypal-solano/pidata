/*
[mysqld]
secure_file_priv               = ''
*/

DELETE FROM superstore_sales;
LOAD DATA LOCAL 
INFILE 'SUPERSTORE-SALES.csv'
INTO TABLE superstore_sales
(
	`ROW_ID`, ORDER_ID, @ORDER_DATE,`ORDER_PRIORITY`,`ORDER_QUANTITY`,`SALES`,`DISCOUNT`,`SHIPMODE`,`PROFIT`,`UNIT_PRICE`,`SHIPPING_COST`,`CUSTOMER_NAME`,`PROVINCE`,`REGION`,`CUSTOMER_SEGMENT`,`PRODUCT_CATEGORY`,`PRODUCT_SUB_CATEGORY`,`PRODUCT_NAME`,`PRODUCT_CONTAINER`,`PRODUCT_BASE_MARGIN`,@SHIP_DATE
)

SET ORDER_DATE = STR_TO_DATE( @ORDER_DATE, '%m/%d/%Y' ), SHIP_DATE =  STR_TO_DATE( @SHIP_DATE, '%m/%d/%Y' )