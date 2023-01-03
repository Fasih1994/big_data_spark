CREATE TABLE orders(
    transaction_id NUMBER PRIMARY KEY,
    qty NUMBER(8,0),
    sku_name VARCHAR(100),
    order_value NUMBER(8,2) -- 20% less than purchase value
    );


CREATE TABLE purchase(
    transaction_id NUMBER PRIMARY KEY,
    username VARCHAR(50),
    currency VARCHAR(12),
    purchase_value NUMBER(8,2),
    country VARCHAR(50)
    );