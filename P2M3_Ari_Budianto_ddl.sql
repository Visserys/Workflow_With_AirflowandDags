'''
DDL SQL Postgres
Create by Ari Budianto
Aribudiantog3@gmail.com
'''


-- Membuat table --
CREATE TABLE table_M3 (
	ID Serial PRIMARY KEY,
	Warehouse_block VARCHAR (50),
	Mode_of_Shipment VARCHAR (50),
	Customer_care_calls INT,
	Customer_rating INT,
	Cost_of_the_Product INT,
	Prior_purchases INT,
	Product_importance VARCHAR (50),
	Gender VARCHAR (50),
	Discount_offered INT,
	Weight_in_gms INT,
	"Reached.on.Time_Y.N" INT
)

-- Copy table dan load data csv --
COPY table_M3(
	ID,
	Warehouse_block,
	Mode_of_Shipment,
	Customer_care_calls,
	Customer_rating ,
	Cost_of_the_Product,
	Prior_purchases,
	Product_importance,
	Gender,
	Discount_offered,
	Weight_in_gms,
	"Reached.on.Time_Y.N")
FROM 'C:\tmp\P2M3_Ari_Budianto_data_raw.csv'
DELIMITER ','
CSV HEADER;

-- Check table --
SELECT * FROM table_M3