-- SHEETS/L1_status
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_status` AS
SELECT
  CAST(id_status AS INT64) AS product_status_id --PK
  -- INT = INT64, BigQuery mapuje všechny celočíselné typy na INT64 (data pak komprimuje)
  ,LOWER(status_name) AS product_status_name
  ,DATE(TIMESTAMP(date_update),"Europe/Prague") AS product_status_update_date
FROM `united-skyline-463312-b6.L0_google_sheets.status`
WHERE id_status IS NOT NULL
  AND status_name IS NOT NULL
  --QUALIFY ROW_NUMBER() OVER(PARTITION BY product_status_id) = 1  -- unique id 
  -- na lekci uvedená operace výše hrozí potenciální ztrátou validních hodnot 
  -- při vypouštění duplicitních IDs bez bližší kontroly je lépe zachovat alespoň nejnovější záznam:
QUALIFY ROW_NUMBER() OVER(PARTITION BY id_status ORDER BY date_update DESC,status_name DESC) = 1
;


-- SHEETS/L1_branch
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_branch` AS
SELECT
  CAST(id_branch AS INT64) AS branch_id --PK
  ,branch_name
  ,DATE(TIMESTAMP(date_update),"Europe/Prague") AS branch_update_date -- totožný s product_status_update_date v tabulce 'status'?
FROM `united-skyline-463312-b6.L0_google_sheets.branch`
WHERE id_branch IS NOT NULL
  AND id_branch != 'NULL'
  AND branch_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY id_branch ORDER BY date_update DESC) = 1
;


-- SHEETS/L1_product
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_product` AS
SELECT
  CAST(id_product AS INT64) AS product_id --PK
  ,name AS product_name
  ,type AS product_type
  ,category AS product_category
  ,is_vat_applicable AS product_is_vat_applicable
  ,DATE(TIMESTAMP(date_update),"Europe/Prague") AS product_update_date
FROM `united-skyline-463312-b6.L0_google_sheets.product`
WHERE id_product IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_product ORDER BY date_update DESC) = 1
;


-- ACCOUNT/L1_invoice
CREATE OR REPLACE VIEW united-skyline-463312-b6.L1.L1_invoice AS
SELECT
  CAST(id_invoice AS INT64) AS invoice_id --PK
  ,CAST(id_invoice_old AS INT64) AS invoice_previous_id
  ,CAST(invoice_id_contract AS INT64) AS contract_id --FK
  ,CAST(id_branch AS INT64) AS branch_id --FK
  ,status AS invoice_status_id
  -- values: 1,2,3,100,300
  -- invoice status < 100  have been issued,status >= 100 is not issued
  ,IF(status < 100,TRUE,FALSE) AS flag_invoice_issued
  ,DATE(TIMESTAMP(date),"Europe/Prague") AS issue_date
  ,DATE(TIMESTAMP(scadent),"Europe/Prague") AS due_date
  ,DATE(TIMESTAMP(date_paid),"Europe/Prague") AS paid_date
  ,DATE(TIMESTAMP(start_date),"Europe/Prague") AS start_date
  ,DATE(TIMESTAMP(end_date),"Europe/Prague") AS end_date
  ,DATE(TIMESTAMP(date_insert),"Europe/Prague") AS insert_date
  ,DATE(TIMESTAMP(date_update),"Europe/Prague") AS update_date
  ,value AS amount_w_vat
  ,payed AS amount_payed
  ,flag_paid_currier
  ,invoice_type AS invoice_type_id 
  -- invoice_type: 1 - invoice,3 - credit_note,2 - return,4 - other
  ,CASE
      WHEN invoice_type = 1 THEN 'invoice'
      WHEN invoice_type = 2 THEN 'return'
      WHEN invoice_type = 3 THEN 'credit_note'
      WHEN invoice_type = 4 THEN 'other'
    END AS invoice_type
  ,number AS invoice_number
  ,value_storno AS return_w_vat
FROM `united-skyline-463312-b6.L0_accounting_system.invoice`
WHERE id_invoice IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_invoice ORDER BY date_update DESC) = 1
;


-- ACCOUNT/L1_invoice_load
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_invoice_load` AS
SELECT
  CAST(id_load AS INT64) AS invoice_load_id --PK
  ,CAST(id_contract AS INT64) AS contract_id --FK
  ,CAST(id_package AS INT64) AS product_purchase_id --FK
  ,CAST(id_invoice AS INT64) AS invoice_id --FK
  ,CAST(id_package_template AS INT64) AS product_id --FK
  -- sloupeček 'currency' obsahuje jen USD 
  -- proto zrušen a značka měny doplněna do názvu sloupečků s cenou
  ,notlei AS price_wo_vat_usd
  ,tva AS vat_rate
  ,value AS price_w_vat_usd
  ,payed AS paid_w_vat_usd
  -- ošetření manuálně zadávaných hodnot a převod do univerzální angličtiny
  -- pro nová data možné doplnit víc variant potenciálních překlepů
  ,CASE
      WHEN um IN ('měsíce','mesice') THEN 'month'
      WHEN um = 'kus' THEN 'item'
      WHEN um = 'den' THEN 'day'
      WHEN um = 'min' THEN 'minute'
      WHEN um = '0' THEN NULL
      ELSE um
    END AS unit
  ,quantity
  ,DATE(TIMESTAMP(start_date),"Europe/Prague") AS start_date
  ,DATE(TIMESTAMP(end_date),"Europe/Prague") AS end_date
  ,DATE(TIMESTAMP(date_insert),"Europe/Prague") AS date_insert
  ,DATE(TIMESTAMP(date_update),"Europe/Prague") AS date_update
  -- pokud je hodnota load_date důležitá, ideálně by měla být vygenerovaná ve správné časové zóně ještě před importem dat
  ,load_date
FROM `united-skyline-463312-b6.L0_accounting_system.invoice_load`
WHERE id_load IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_load ORDER BY date_update DESC) = 1
;


-- CRM/L1_contract
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_contract` AS
SELECT
  CAST(id_contract AS INT64) AS contract_id --PK
  ,CAST(id_branch AS INT64) AS branch_id --FK
  ,DATE(TIMESTAMP(date_contract_valid_from),"Europe/Prague") AS contract_valid_from
  ,DATE(TIMESTAMP(date_contract_valid_to),"Europe/Prague") AS contract_valid_to
  ,DATE(TIMESTAMP(date_registered),"Europe/Prague") AS registered_date
  ,DATE(TIMESTAMP(date_signed),"Europe/Prague") AS signed_date
  ,DATE(TIMESTAMP(activation_process_date),"Europe/Prague") AS activation_process_date
  ,DATE(TIMESTAMP(prolongation_date),"Europe/Prague") AS prolongation_date
  ,registration_end_reason
  ,flag_prolongation
  ,flag_send_inv_email AS flag_send_email
  ,contract_status
  ,load_date
FROM `united-skyline-463312-b6.L0_crm.contract`
WHERE id_contract IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_contract ORDER BY load_date DESC) = 1
;


-- CRM/L1_product_purchase
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L1.L1_product_purchase` AS
SELECT
  CAST(pp.id_package AS INT64) AS product_purchase_id --PK
  ,CAST(pp.id_contract AS INT64) AS contract_id --FK
  ,CAST(pp.id_package_template AS INT64) AS product_id --FK
  ,DATE(TIMESTAMP(pp.date_insert),"Europe/Prague") AS create_date
  ,DATE(TIMESTAMP(pp.start_date),"Europe/Prague") AS product_valid_from
  ,DATE(TIMESTAMP(pp.end_date),"Europe/Prague") AS product_valid_to
  ,pp.fee AS price_wo_vat
  ,DATE(TIMESTAMP(pp.date_update),"Europe/Prague") AS update_date
  ,CAST(pp.package_status AS INT64) AS product_status_id --FK
  -- ošetření manuálně zadávaných hodnot a převod do univerzální angličtiny
  ,CASE
      WHEN pp.measure_unit IN ('měsíce') THEN 'month'
      WHEN pp.measure_unit = 'kus' THEN 'item'
      WHEN pp.measure_unit = 'den' THEN 'day'
      WHEN pp.measure_unit = 'min' THEN 'minute'
      WHEN pp.measure_unit = '0' THEN NULL
      ELSE pp.measure_unit
    END AS unit
  ,CAST(pp.id_branch AS INT64) AS branch_id --FK
  ,pr.product_name
  ,pr.product_type
  ,pr.product_category
  ,s.product_status_name AS product_status
FROM `united-skyline-463312-b6.L0_crm.product_purchase` AS pp
LEFT JOIN `united-skyline-463312-b6.L1.L1_product` AS pr
  ON pp.id_package_template = pr.product_id
LEFT JOIN `united-skyline-463312-b6.L1.L1_status` AS s
  ON pp.package_status = s.product_status_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY pp.id_package ORDER BY pp.date_update DESC) = 1
;
