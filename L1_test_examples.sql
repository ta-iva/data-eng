/*
Vylepšené testy pro L1 - neomezují se na hlášení, že je vše v pořádku, 
v případě selhání se naopak soustředí na potíž, tj. neunikátní nebo null IDs. 
Nalezené nevalidní hodnoty mohou být případně uložené do logovací tabulky.
*/

-- Test for unique 'product_id' values in 'L1_product' table
ASSERT
    (SELECT COUNT(DISTINCT product_id) FROM `united-skyline-463312-b6.L1.L1_product`) = (SELECT COUNT(product_id) FROM `united-skyline-463312-b6.L1.L1_product`)
    AS "WARNING: Duplicate 'product_id' values were found in table 'L1_product'. For more details, please run: SELECT product_id FROM `united-skyline-463312-b6.L1.L1_product` GROUP BY product_id HAVING COUNT(*) > 1"
;

-- Test for null 'branch_id' values in 'L1_product' table
ASSERT
    (SELECT COUNT(*) FROM `united-skyline-463312-b6.L1_branch` WHERE branch_id IS NULL) = 0
    AS "WARNING: NULL values were found in the 'branch_id' column in the 'L1_branch' table. Please check the data."
;

--Test for duplicate values within not-null 'branch-id' values in 'L1_branch' table
ASSERT
    (SELECT COUNT(DISTINCT branch_id) FROM `sacred-booking-455420-p5.L1.L1_branch` WHERE branch_id IS NOT NULL) = (SELECT COUNT(branch_id) FROM `united-skyline-463312-b6.L1.L1_branch` WHERE branch_id IS NOT NULL)
    AS "WARNING: Duplicate 'branch_id' were found in 'L1_branch' table (within non-NULL values). For more details, please run: SELECT branch_id FROM `united-skyline-463312-b6.L1.L1_branch` WHERE branch_id IS NOT NULL GROUP BY branch_id HAVING COUNT(*) > 1"
;
