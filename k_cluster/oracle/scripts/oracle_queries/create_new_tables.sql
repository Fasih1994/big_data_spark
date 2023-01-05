/*
Firt log in with user FASIH
sqlplus FASIH/FASIHTEST@localhost:1521/ORCLPDB1

*/

-- create table 1
CREATE TABLE orders(
    transaction_id NUMBER PRIMARY KEY,
    qty NUMBER(8,0),
    sku_name VARCHAR(100),
    order_value NUMBER(8,2), -- 20% less than purchase value
    ordered_at TIMESTAMP
    );

-- create table 2
CREATE TABLE purchase(
    transaction_id NUMBER PRIMARY KEY,
    username VARCHAR(50),
    currency VARCHAR(12),
    purchase_value NUMBER(8,2),
    country VARCHAR(50),
    PURCHASE_TIME TIMESTAMP
    );