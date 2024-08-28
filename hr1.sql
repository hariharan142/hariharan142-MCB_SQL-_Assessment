-- Creating SUPPLIERS table
CREATE TABLE SUPPLIERS (
    SUPPLIER_ID         NUMBER, -- Auto-increment in Oracle 12c+ only
    SUPPLIER_NAME       VARCHAR2(1000) NOT NULL,
    SUPP_CONTACT_NAME   VARCHAR2(1000) NOT NULL,
    SUPP_ADDRESS        VARCHAR2(2000),
    SUPP_CONTACT_NUMBER VARCHAR2(20),
    SUPP_EMAIL          VARCHAR2(500),
    CONSTRAINT SUPPLIERS_PK PRIMARY KEY (SUPPLIER_ID)
);

-- Since Oracle 11g doesn't support IDENTITY columns, you need to create a sequence and trigger
CREATE SEQUENCE SUPPLIERS_SEQ START WITH 1 INCREMENT BY 1;

CREATE TRIGGER SUPPLIERS_BI
BEFORE INSERT ON SUPPLIERS
FOR EACH ROW
BEGIN
    SELECT SUPPLIERS_SEQ.NEXTVAL INTO :NEW.SUPPLIER_ID FROM DUAL;
END;
/

-- Creating ORDERS table
CREATE TABLE ORDERS (
    ORDER_ID            NUMBER,
    ORDER_REF           VARCHAR2(2000) NOT NULL,
    ORDER_DATE          DATE NOT NULL,
    SUPPLIER_ID         NUMBER NOT NULL,
    ORDER_TOTAL_AMOUNT  NUMBER(15, 2),
    ORDER_DESCRIPTION   VARCHAR2(2000),
    ORDER_STATUS        VARCHAR2(100),
    CONSTRAINT ORDERS_PK PRIMARY KEY (ORDER_ID),
    CONSTRAINT ORDERS_SUPPLIER_FK FOREIGN KEY (SUPPLIER_ID)
        REFERENCES SUPPLIERS (SUPPLIER_ID)
);

-- Creating the sequence and trigger for Oracle 11g
CREATE SEQUENCE ORDERS_SEQ START WITH 1 INCREMENT BY 1;

CREATE TRIGGER ORDERS_BI
BEFORE INSERT ON ORDERS
FOR EACH ROW
BEGIN
    SELECT ORDERS_SEQ.NEXTVAL INTO :NEW.ORDER_ID FROM DUAL;
END;
/

-- Creating INVOICES table
CREATE TABLE INVOICES (
    INVOICE_ID          NUMBER,
    ORDER_ID            NUMBER NOT NULL,
    INVOICE_REF         VARCHAR2(2000),
    INVOICE_DATE        DATE,
    INVOICE_STATUS      VARCHAR2(100),
    INVOICE_AMOUNT      NUMBER(15, 2),
    INVOICE_HOLD_REASON VARCHAR2(2000),
    INVOICE_DESCRIPTION VARCHAR2(2000),
    CONSTRAINT INVOICES_PK PRIMARY KEY (INVOICE_ID),
    CONSTRAINT INVOICES_ORDER_FK FOREIGN KEY (ORDER_ID)
        REFERENCES ORDERS (ORDER_ID)
);

-- Creating the sequence and trigger for Oracle 11g
CREATE SEQUENCE INVOICES_SEQ START WITH 1 INCREMENT BY 1;

CREATE TRIGGER INVOICES_BI
BEFORE INSERT ON INVOICES
FOR EACH ROW
BEGIN
    SELECT INVOICES_SEQ.NEXTVAL INTO :NEW.INVOICE_ID FROM DUAL;
END;
/
INSERT INTO SUPPLIERS (SUPPLIER_NAME, SUPP_CONTACT_NAME, SUPP_ADDRESS, SUPP_CONTACT_NUMBER, SUPP_EMAIL)
SELECT DISTINCT
    SUPPLIER_NAME,
    SUPP_CONTACT_NAME,
    SUPP_ADDRESS,
    SUPP_CONTACT_NUMBER,
    SUPP_EMAIL
FROM XXBCM_ORDER_MGT
WHERE SUPPLIER_NAME IS NOT NULL;
/
INSERT INTO ORDERS (ORDER_REF, ORDER_DATE, SUPPLIER_ID, ORDER_TOTAL_AMOUNT, ORDER_DESCRIPTION, ORDER_STATUS)
SELECT DISTINCT
    XO.ORDER_REF,
    CASE
        WHEN REGEXP_LIKE(XO.ORDER_DATE,'^\D{2}-[A-Z]{3}-\D{4}-$')
        THEN 
    TO_DATE(TRIM(XO.ORDER_DATE), 'DD-MON-YYYY')
    ELSE TO_DATE('01-JAN-1990','DD-MM-YYYY')
    END,
    S.SUPPLIER_ID,
    TO_NUMBER(REPLACE(TRIM(XO.ORDER_TOTAL_AMOUNT), ',', '')),
    XO.ORDER_DESCRIPTION,
    XO.ORDER_STATUS
FROM
    XXBCM_ORDER_MGT XO
    JOIN SUPPLIERS S ON XO.SUPPLIER_NAME = S.SUPPLIER_NAME
WHERE
    XO.ORDER_REF IS NOT NULL
    AND XO.ORDER_DATE IS NOT NULL
    AND XO.ORDER_STATUS IS NOT NULL;
/

INSERT INTO INVOICES (ORDER_ID, INVOICE_REF, INVOICE_DATE, INVOICE_STATUS, INVOICE_AMOUNT, INVOICE_HOLD_REASON, INVOICE_DESCRIPTION)
SELECT DISTINCT
    O.ORDER_ID,
    XO.INVOICE_REFERENCE,
    TRIM(XO.INVOICE_DATE),
    XO.INVOICE_STATUS,
    TO_NUMBER(REPLACE(XO.INVOICE_AMOUNT, ',', '')),
    XO.INVOICE_HOLD_REASON,
    XO.INVOICE_DESCRIPTION
FROM
    XXBCM_ORDER_MGT XO
    JOIN ORDERS O ON XO.ORDER_REF = O.ORDER_REF
WHERE
    XO.INVOICE_REFERENCE IS NOT NULL;
/

SELECT * FROM INVOICES;
SELECT * FROM SUPPLIERS;
SELECT * FROM ORDERS;

COMMIT;
/
CREATE OR REPLACE PACKAGE XXBCM_ORDER_MGT_PKG AS

    -- FUNCTION for Question 4
    FUNCTION GET_ORDER_SUMMARY RETURN SYS_REFCURSOR;

    -- Function for Question 5
    FUNCTION GET_SECOND_HIGHEST_ORDER RETURN SYS_REFCURSOR;

    -- FUNCTION for Question 6
    FUNCTION GET_SUPPLIER_ORDER_SUMMARY(
        START_DATE DATE, 
        END_DATE DATE
    )RETURN SYS_REFCURSOR; 

END XXBCM_ORDER_MGT_PKG;

/
CREATE OR REPLACE PACKAGE BODY XXBCM_ORDER_MGT_PKG AS

    -- Function to return a cursor with summary of Orders and their corresponding list of distinct invoices
    FUNCTION get_order_summary RETURN SYS_REFCURSOR IS
        cur SYS_REFCURSOR;
    BEGIN
        OPEN cur FOR
            SELECT 
                -- Exclude prefix PO and return only numeric value
                REGEXP_SUBSTR(ORDER_REF, 'PO', '') AS ORDER_REF_NUM,
                -- Period based on Order Date (MON-YYYY)
                TO_CHAR(ORDER_DATE, 'DD-MM-YYYY') AS ORDER_PERIOD,
                -- First character in each word to uppercase and the rest to lowercase
                INITCAP(SUPPLIERS.SUPPLIER_NAME) AS SUPPLIER_NAME,
                -- Format order total amount
                TO_CHAR(ORDERS.ORDER_TOTAL_AMOUNT, '999,999,990.00') AS ORDER_TOTAL_AMOUNT,
                -- Order Status
                ORDERS.ORDER_STATUS AS ORDER_STATUS,
                -- List all invoice references for that specific Order, pipe-delimited
                LISTAGG(INVOICES.INVOICE_REF, '|') WITHIN GROUP (ORDER BY INVOICES.INVOICE_REF) AS INVOICE_REFERENCES,
                -- Action based on the invoice statuses
                CASE 
                    WHEN COUNT(CASE WHEN INVOICES.INVOICE_STATUS = 'Pending' THEN 1 END) > 0 THEN 'To follow up'
                    WHEN COUNT(CASE WHEN INVOICES.INVOICE_STATUS IS NULL THEN 1 END) > 0 THEN 'To verify'
                    ELSE 'OK'
                END AS ACTION
            FROM 
                ORDERS
            JOIN 
                SUPPLIERS ON ORDERS.SUPPLIER_ID = SUPPLIERS.SUPPLIER_ID
            LEFT JOIN 
                INVOICES ON ORDERS.ORDER_ID = INVOICES.ORDER_ID
            GROUP BY 
                ORDER_REF, ORDER_DATE, SUPPLIERS.SUPPLIER_NAME, ORDERS.ORDER_TOTAL_AMOUNT, ORDERS.ORDER_STATUS
            ORDER BY 
                ORDER_DATE DESC;
        RETURN cur;
    END get_order_summary;

    -- Function to return the details for the second highest Order Total Amount
    FUNCTION get_second_highest_order RETURN SYS_REFCURSOR IS
        cur SYS_REFCURSOR;
    BEGIN
        OPEN cur FOR
            SELECT 
                -- Exclude prefix PO and return only numeric value
                REGEXP_SUBSTR(ORDER_REF, '(\d+)', 1, 1) AS ORDER_REF_NUM,
                -- Format Order Date
                TO_CHAR(ORDER_DATE, 'Month DD, YYYY') AS ORDER_DATE,
                -- Supplier Name in upper case
                UPPER(SUPPLIERS.SUPPLIER_NAME) AS SUPPLIER_NAME,
                -- Format Order Total Amount
                TO_CHAR(ORDERS.ORDER_TOTAL_AMOUNT, '999,999,990.00') AS ORDER_TOTAL_AMOUNT,
                -- Order Status
                ORDERS.ORDER_STATUS AS ORDER_STATUS,
                -- List all invoice references for that specific Order, pipe-delimited
                LISTAGG(INVOICES.INVOICE_REF, '|') WITHIN GROUP (ORDER BY INVOICES.INVOICE_REF) AS INVOICE_REFERENCES
            FROM 
                ORDERS
            JOIN 
                SUPPLIERS ON ORDERS.SUPPLIER_ID = SUPPLIERS.SUPPLIER_ID
            LEFT JOIN 
                INVOICES ON ORDERS.ORDER_ID = INVOICES.ORDER_ID
            WHERE
                ORDERS.ORDER_TOTAL_AMOUNT = (SELECT MAX(ORDER_TOTAL_AMOUNT) 
                                       FROM ORDERS 
                                       WHERE ORDER_TOTAL_AMOUNT < (SELECT MAX(ORDER_TOTAL_AMOUNT) FROM ORDERS))
            GROUP BY 
                ORDER_REF, ORDER_DATE, SUPPLIERS.SUPPLIER_NAME, ORDERS.ORDER_TOTAL_AMOUNT, ORDERS.ORDER_STATUS;
        RETURN cur;
    END get_second_highest_order;

    -- Function to return the list of suppliers with their number of orders and total amount ordered
    FUNCTION GET_SUPPLIER_ORDER_SUMMARY(start_date DATE, end_date DATE) RETURN SYS_REFCURSOR IS
        cur SYS_REFCURSOR;
    BEGIN
        OPEN cur FOR
            SELECT 
                SUPPLIERS.SUPPLIER_NAME,
                SUPPLIERS.SUPP_CONTACT_NAME AS SUPPLIER_CONTACT_NAME,
                -- Format contact numbers
                REGEXP_SUBSTR(SUPPLIERS.SUPP_CONTACT_NUMBER, '^\d{3}-\d{4}$') AS SUPPLIER_CONTACT_NO_1,
                REGEXP_SUBSTR(SUPPLIERS.SUPP_CONTACT_NUMBER, '^\d{3}-\d{4}$') AS SUPPLIER_CONTACT_NO_2,
                -- Total number of orders
                COUNT(ORDERS.ORDER_ID) AS TOTAL_ORDERS,
                -- Format total amount ordered
                TO_CHAR(SUM(ORDERS.ORDER_TOTAL_AMOUNT), '999,999,990.00') AS ORDER_TOTAL_AMOUNT
            FROM 
                SUPPLIERS
            LEFT JOIN 
                ORDERS ON SUPPLIERS.SUPPLIER_ID = ORDERS.SUPPLIER_ID
            WHERE 
                ORDERS.ORDER_DATE BETWEEN start_date AND end_date
            GROUP BY 
                SUPPLIERS.SUPPLIER_NAME, SUPPLIERS.SUPP_CONTACT_NAME, SUPPLIERS.SUPP_CONTACT_NUMBER, SUPPLIERS.SUPP_CONTACT_NUMBER;
        RETURN cur;
    END GET_SUPPLIER_ORDER_SUMMARY;

END XXBCM_ORDER_MGT_PKG;
/


-- To get order summary
DECLARE
    cur SYS_REFCURSOR;
    rec ORDER_SUMMARY_REC%ROWTYPE;  -- You need to define this record type based on your function output structure
BEGIN
    cur := XXBCM_ORDER_MGT_PKG.get_order_summary;
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN cur%NOTFOUND;
        -- Display the record, adjust according to your output structure
        DBMS_OUTPUT.PUT_LINE('Order Ref: ' || rec.ORDER_REF_NUM ||
                             ', Period: ' || rec.ORDER_PERIOD ||
                             ', Supplier: ' || rec.SUPPLIER_NAME ||
                             ', Total Amount: ' || rec.ORDER_TOTAL_AMOUNT ||
                             ', Status: ' || rec.ORDER_STATUS ||
                             ', Invoices: ' || rec.INVOICE_REFERENCES ||
                             ', Action: ' || rec.ACTION);
    END LOOP;
    CLOSE cur;
END;
/

-- To get the second highest order
DECLARE
    cur SYS_REFCURSOR;
    rec SECOND_HIGHEST_ORDER_REC%ROWTYPE;  -- You need to define this record type based on your function output structure
BEGIN
    cur := XXBCM_ORDER_MGT_PKG.get_second_highest_order;
    FETCH cur INTO rec;
    -- Display the record, adjust according to your output structure
    DBMS_OUTPUT.PUT_LINE('Order Ref: ' || rec.ORDER_REF_NUM ||
                         ', Order Date: ' || rec.ORDER_DATE ||
                         ', Supplier: ' || rec.SUPPLIER_NAME ||
                         ', Total Amount: ' || rec.ORDER_TOTAL_AMOUNT ||
                         ', Status: ' || rec.ORDER_STATUS ||
                         ', Invoices: ' || rec.INVOICE_REFERENCES);
    CLOSE cur;
END;
/

-- To get supplier order stats
DECLARE
    cur SYS_REFCURSOR;
    rec SUPPLIER_ORDER_STATS_REC%ROWTYPE;  -- You need to define this record type based on your function output structure
BEGIN
    cur := XXBCM_ORDER_MGT_PKG.get_supplier_order_stats('01-JAN-2022', '31-AUG-2022');
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN cur%NOTFOUND;
        -- Display the record, adjust according to your output structure
        DBMS_OUTPUT.PUT_LINE('Supplier Name: ' || rec.SUPPLIER_NAME ||
                             ', Contact Name: ' || rec.SUPPLIER_CONTACT_NAME ||
                             ', Contact No. 1: ' || rec.SUPPLIER_CONTACT_NO_1 ||
                             ', Contact No. 2: ' || rec.SUPPLIER_CONTACT_NO_2 ||
                             ', Total Orders: ' || rec.TOTAL_ORDERS ||
                             ', Total Amount: ' || rec.ORDER_TOTAL_AMOUNT);
    END LOOP;
    CLOSE cur;
END;
/