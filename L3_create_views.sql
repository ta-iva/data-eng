/*
Use case: 
data pro základní report ohledně stávajících zákazníků,produktů a prodejů (pro posouzení, zda současná strategie přináší očekávané výsledky)

-> zákaznická perspektiva (tabulka contract)
+ pro zjednodušení: 1 smlouva = 1 zákazník

Otázky:
1) Kteří zákazníci a kdy odcházejí? Upřesnění: na základě kterých balíčků?  
(contract status,package name, package status, packages valid from/to, no. of customers) 
2) Nakupují zákazníci málo/hodně?
3) Jaké existují ještě produkty, které zákazníci nakupovali? Upřesnění: jaké balíčky byl nakupovány v kterých letech?

Model:
jedna ústřední tabulka (v našem případě contract), na ni napojené další - v praxi údajně běžný postup, i když nedopovídá úplně teorii (snowflake, příp. OBT by byl víc korektní přístup)
*/


--L3.UC1_contract
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L3.L3_contract` AS
SELECT
  contract_id
  ,branch_id
  ,contract_valid_from
  ,contract_valid_to
  ,registration_end_reason
  ,contract_status
  ,flag_prolongation

  -- kategorizovaná délka kontraktu
  ,CASE
      WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) <= 6 THEN 'less than 6 months'
      WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN 7 AND 18 THEN '1 year'
      WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN 19 AND 30 THEN '2 years'
      WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) > 30 THEN 'more than 2 years'
      ELSE 'invalid value' -- pojistka
  END AS contract_duration

  -- rok začátku platnosti smlouvy
  ,EXTRACT(YEAR FROM contract_valid_from) AS start_year_of_contract
FROM 
  `united-skyline-463312-b6.L2.L2_contract`
  
-- uplatněno jen pro záznamy s validními daty platnosti smlouvy
WHERE
    contract_valid_from IS NOT NULL
    AND contract_valid_to IS NOT NULL
    AND contract_valid_to >= contract_valid_from
;


--L3.UC1_invoice
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L3.L3_invoice` AS
SELECT
  invoice_id
  ,contract_id
  ,amount_w_vat
  ,return_w_vat
  ,paid_date
  -- celková zaplacená částka v USD
  ,(amount_w_vat - return_w_vat) AS total_paid
FROM 
  `united-skyline-463312-b6.L2.L2_invoice`
;


--L3.UC1_product_purchase
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L3.L3_product_purchase` AS
SELECT
  product_purchase_id
  ,contract_id
  ,product_valid_from
  ,product_valid_to
  ,product_name
  ,product_type
  ,unit
  ,flag_unlimited_product
FROM 
  `united-skyline-463312-b6.L2.L2_product_purchase`
-- název balíčku/produktu musí existovat
WHERE 
  product_name IS NOT NULL
;


--L3.UC1_branch
CREATE OR REPLACE VIEW `united-skyline-463312-b6.L3.L3_branch` AS
SELECT
  branch_id
  ,branch_name
FROM 
  `united-skyline-463312-b6.L2.L2_branch`
;