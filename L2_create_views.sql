/*
5 tabulek: branch, contract, invoice, product, product_purchase
L1->L2 transformace dle zadání zákazníka/prezentace
*/

-- L2 branch
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L2.L2_branch` AS
SELECT
  branch_id
  ,branch_name
  ,branch_update_date
FROM `united-skyline-463312-b6.L1.L1_branch`
-- nechceme branch_name 'uknown'
WHERE branch_name != 'unknown'
;

-- L2_contract
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L2.L2_contract` AS
SELECT
  contract_id
  ,branch_id
  ,contract_valid_from
  ,contract_valid_to
  ,registered_date
  ,signed_date
  ,activation_process_date
  ,prolongation_date
  ,registration_end_reason
  ,flag_prolongation
  ,flag_send_email
  ,contract_status
FROM `united-skyline-463312-b6.L1.L1_contract`
-- chceme jen kontrakty s validním registered_date
WHERE registered_date IS NOT NULL
;


-- L2_invoice
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L2.L2_invoice` AS
SELECT
  invoice_id
  ,invoice_previous_id
  ,contract_id
  ,invoice_status_id
  ,invoice_type
  ,flag_invoice_issued
  ,issue_date
  ,due_date
  ,paid_date
  ,start_date
  ,end_date
  ,insert_date
  ,update_date
  -- zbavíme se záporných hodnot
  ,GREATEST(amount_w_vat,0) AS amount_w_vat
  ,return_w_vat
  -- dopočítáme amount without VAT (VAT = 20 %)
  ,(amount_w_vat / 1.2) AS amount_wo_vat
  -- pořadí faktur na úrovni kontraktu podle data vydání
  ,ROW_NUMBER() OVER(PARTITION BY contract_id ORDER BY issue_date) AS invoice_order
FROM `united-skyline-463312-b6.L1.L1_invoice`
-- chceme jen vystavené faktury
WHERE invoice_type = 'invoice'
  AND flag_invoice_issued = TRUE
;


-- L2_product
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L2.L2_product` AS
SELECT
  product_id
  ,product_name
  ,product_type
  ,product_category
FROM `united-skyline-463312-b6.L1.L1_product`
-- chceme produkty z následujících dvou kategorií
WHERE product_category IN ('product','rent')
;


-- L2_product_purchase
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L2.L2_product_purchase` AS
SELECT
  product_purchase_id
  ,contract_id
  ,product_id
  ,create_date
  ,product_valid_from
  ,product_valid_to
  ,update_date
  ,product_status
  ,product_name
  ,product_type
  ,product_category
  -- využití unit k prodebatování se zákazníkem v rámci L3
  ,unit
  -- zbavíme se záporných hodnot
  ,GREATEST(price_wo_vat,0) AS price_wo_vat
  -- dopočítáváme VAT
  ,(price_wo_vat * 1.2) AS price_w_vat
  -- přidáme příznak pro unlimited product
  ,IF(product_valid_from = DATE '2035-12-31',TRUE,FALSE) AS flag_unlimited_product
FROM `united-skyline-463312-b6.L1.L1_product_purchase`
-- chceme jen produkty z kategorie 'product' nebo 'rent'
-- product_status nesmí být prázdný ani být zrušený
WHERE LOWER(product_category) IN ('product','rent')
  AND product_status IS NOT NULL
  AND LOWER(product_status) NOT IN ('canceled','canceled registration','disconnected')
;
