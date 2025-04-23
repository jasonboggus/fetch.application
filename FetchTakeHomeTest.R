## TITLE: Fetch Take Home Test
## AUTHOR: Jason Boggus
## CREATE DATE: 4/22/25
## UPDATE DATE: 4/23/25
## LAST RUNTIME: 18 seconds

###################################

## Install libraries

install.packages('sqldf')
install.packages('lubridate')

## Load libraries

library(sqldf)
library(lubridate)

## Set path to where input data is stored

setwd('/Users/me/Desktop/FetchTakeHomeTest')

## Import data

u <- read.csv('USER_TAKEHOME.csv') ## 100,000 rows
p <- read.csv('PRODUCTS_TAKEHOME.csv') ## 845,552
t <- read.csv('TRANSACTION_TAKEHOME.csv') ## 50,000

## Investigate data model relationships

  ## User ID on Users and Transactions

  t(sqldf('SELECT count(*) AS blanks FROM u WHERE id IS NULL or id = ""')) ## No blank IDs on Users table
  
  t(sqldf('SELECT count(*) AS blanks FROM t WHERE user_id IS NULL or user_id = ""')) ## No blank User IDs on Transactions table

  t(sqldf('
          SELECT count(id) AS row_count,
              count(DISTINCT id) AS id_count,
              ------------
              count(CASE WHEN user_id IS NOT NULL THEN id END) AS row_count_ont,
              count(DISTINCT CASE WHEN user_id IS NOT NULL THEN id END) AS id_count_ont,
              count(CASE WHEN user_id IS NULL THEN id END) AS row_count_notont,
              count(DISTINCT CASE WHEN user_id IS NULL THEN id END) AS id_count_notont,
              ------------
              count(CASE WHEN user_id2 IS NOT NULL THEN lower(id) END) AS row_count_ont2,
              count(DISTINCT CASE WHEN user_id2 IS NOT NULL THEN lower(id) END) AS id_count_ont2,
              count(CASE WHEN user_id2 IS NULL THEN lower(id) END) AS row_count_notont2,
              count(DISTINCT CASE WHEN user_id2 IS NULL THEN lower(id) END) AS id_count_notont2
          FROM u
          LEFT JOIN (SELECT user_id FROM t GROUP BY 1) t1 
              ON id = user_id
          LEFT JOIN (SELECT lower(user_id) AS user_id2 FROM t GROUP BY 1) t2 
              ON lower(id) = user_id2
          '))
  
      ## No IDs repeated on User table
      ## Only 91 of 100,000 IDs found on Transaction table
      ## Case statements aren't necessary to normalize ID
  
  t(sqldf('
          SELECT count(user_id) AS row_count,
              count(DISTINCT user_id) AS id_count,
              ------------
              count(CASE WHEN id IS NOT NULL THEN user_id END) AS row_count_onu,
              count(DISTINCT CASE WHEN id IS NOT NULL THEN user_id END) AS userid_count_onu,
              count(CASE WHEN id IS NULL THEN user_id END) AS row_count_notonu,
              count(DISTINCT CASE WHEN id IS NULL THEN user_id END) AS userid_count_notonu
          FROM t
          LEFT JOIN (SELECT id FROM u GROUP BY 1) u 
              ON user_id = id
          '))
  
      ## 17,694 Unique IDs exist on Transaction Table
      ## 91 are on the User Table and these comprise 262 rows
      ## The remaining 49,738 rows correspond to the 17,603 IDs not found on the User Table
  
  
  ## Barcode on Products and Transactions
  
  t(sqldf('SELECT count(*) AS blanks FROM p WHERE barcode IS NULL or barcode = ""')) ## 4025 blank barcodes on Products table
  
  t(sqldf('SELECT count(*) AS blanks FROM t WHERE barcode IS NULL or barcode = ""')) ## 5762 blank barcodes on Transactions table
  
  t(sqldf('
          SELECT 
              count(barcode) AS row_count,
              count(DISTINCT barcode) barcode_count,
              ------------
              count(CASE WHEN bc IS NOT NULL THEN barcode END) AS row_count_ont,
              count(DISTINCT CASE WHEN bc IS NOT NULL THEN barcode END) AS barcode_count_ont,
              count(CASE WHEN bc IS NULL THEN barcode END) AS row_count_notont,
              count(DISTINCT CASE WHEN bc IS NULL THEN barcode END) AS barcode_count_notont
          FROM p
          LEFT JOIN (SELECT barcode AS bc FROM t GROUP BY 1) t1 
              ON barcode = bc
              AND coalesce(barcode,"") <> ""
          '))
  
  p_blankbarcode <- sqldf('SELECT * FROM p WHERE barcode IS NULL OR barcode = ""')
  
  p_dupebarcode <- sqldf('SELECT * FROM p WHERE barcode IN (SELECT barcode FROM p GROUP BY 1 HAVING count(*) > 1)')
  
      ## The product data is messy. Some rows have no barcode. Some rows are entire duplicates. 
      ## Some duplicate barcodes have rows which are more complete. Will want to take best in joins.

  
####################
  

## Tack birthdate, createdate and brand onto transactions. 
combo <- sqldf('
               SELECT t.*, BIRTH_DATE, CREATED_DATE, brand, category,
                  CASE WHEN coalesce(u1.id,u2.id) IS NULL THEN "NOT ON FILE" ELSE "ON FILE" END AS ind_users_table
               FROM t
               LEFT JOIN (
                  SELECT id, 
                      min(BIRTH_DATE) AS BIRTH_DATE
                  FROM u
                  WHERE BIRTH_DATE IS NOT NULL 
                    AND BIRTH_DATE <> ""
                  GROUP BY 1
               ) u1
                  ON user_id = u1.id
               LEFT JOIN (
                  SELECT id, 
                      min(CREATED_DATE) AS CREATED_DATE
                  FROM u
                  WHERE CREATED_DATE IS NOT NULL 
                    AND CREATED_DATE <> ""
                  GROUP BY 1
               ) u2
                  ON user_id = u2.id
               LEFT JOIN (
                  SELECT barcode AS bc, 
                      min(brand) AS brand, 
                      min(CASE WHEN lower(category_1) = "snacks" THEN category_1 || " - " || category_2 ELSE category_1 END) AS category
                  FROM p 
                  WHERE brand IS NOT NULL
                    AND brand <> ""
                  GROUP BY 1
               ) p1
                  ON barcode = bc
               ')

  
## Convert relevant dates, Calculate user age (years) and signup duration (months) at time of scan 
  
combo$birthdate <- as.Date(substring(combo$BIRTH_DATE,1,10))
combo$createdate <- as.Date(substring(combo$CREATED_DATE,1,10))
combo$scandate <- as.Date(substring(combo$SCAN_DATE,1,10))
combo$ageatscan <- as.numeric(difftime(combo$scandate, combo$birthdate, units = c("days"))) / 365.25
combo$monthssignedupatscan <- as.numeric(difftime(combo$scandate, combo$createdate, units = c("days"))) / ( 365.25 / 12 )


## Aggregate to get a feel for distribution
## Sometimes the Sale amount on a table is the sellng price and must be multiplied by the quantity
## to get the total quantity of the sale. I want to investigate if that is necessary here. I don't
## think it will because many of the quantities are not integers and are less than 1. I suspect
## these are products that are sold by weight or some other measure. Thus I'll take final_sale to
## be the total sales for that product barcode on that receipt. I also see that FINAL_QUANTITY can
## sometimes be zero.. further discounting it's effectiveness for answering questions until I have 
## an opportunity to ask questions about this column to the data owner.

rollup <- sqldf('
               SELECT 
                  CASE WHEN final_quantity = "zero" THEN "zero" ELSE "positive" END AS ind_final_quantity,
                  CASE WHEN coalesce(ageatscan,0) = 0 THEN "BLANK" WHEN ageatscan < 21 THEN "<21yrs" ELSE ">=21yrs" END AS ind_ageatscan,
                  CASE WHEN coalesce(monthssignedupatscan,0) = 0 THEN "BLANK" WHEN monthssignedupatscan < 6 THEN "<6mos" ELSE ">=6mos" END AS ind_monthssignedupatscan,
                  CASE WHEN coalesce(category,"") = "" THEN "BLANK" WHEN category = "Health & Wellness" THEN "H&W" ELSE "OTHER" END AS ind_category,
                  CASE WHEN coalesce(brand,"") = "" THEN "BLANK" ELSE "branded" END AS ind_brand,
                  count(DISTINCT receipt_id) AS receipts,
                  sum(final_sale) AS final_sale,
                  sum(final_sale * (final_quantity * 1)) AS final_sale_ext
               FROM combo
               GROUP BY 1, 2, 3, 4, 5
               ') 

rollupltd <- sqldf('
               SELECT 
                  CASE WHEN final_quantity = "zero" THEN "zero" ELSE "positive" END AS ind_final_quantity,
                  CASE WHEN coalesce(ageatscan,0) = 0 THEN "BLANK" WHEN ageatscan < 21 THEN "<21yrs" ELSE ">=21yrs" END AS ind_ageatscan,
                  CASE WHEN coalesce(monthssignedupatscan,0) = 0 THEN "BLANK" WHEN monthssignedupatscan < 6 THEN "<6mos" ELSE ">=6mos" END AS ind_monthssignedupatscan,
                  CASE WHEN coalesce(category,"") = "" THEN "BLANK" WHEN category = "Health & Wellness" THEN "H&W" ELSE "OTHER" END AS ind_category,
                  CASE WHEN coalesce(brand,"") = "" THEN "BLANK" ELSE "branded" END AS ind_brand,
                  count(DISTINCT receipt_id) AS receipts,
                  sum(final_sale) AS final_sale,
                  sum(final_sale * (final_quantity * 1)) AS final_sale_ext
               FROM combo
               WHERE final_sale IS NOT NULL 
                 AND final_quantity <> "zero"
               GROUP BY 1, 2, 3, 4, 5
               ') 

## A closer look at the data shows me that most receipts are repeated. One line always looks legitimate quantity + sale.
## The other line tends to have either "zero" in the FQ column or NA in the FS column. Therefore I am going to focus on
## the more legitimate looking lines. The other likely serves a purpose, but not for calculating sales impact.

###############
## QUESTIONS ##
###############

## What are the top 5 brands by receipts scanned among users 21 and over?

q1 <- sqldf('
           SELECT CASE WHEN brand LIKE "%DEAN%DAIRY%" THEN "DEANS DAIRY" ELSE brand END AS brand,
              count(DISTINCT receipt_id) AS receipts,
              count(DISTINCT user_id) AS users
           FROM combo
           WHERE coalesce(ageatscan,0) >= 21
             AND coalesce(brand,"") <> ""
             AND final_sale IS NOT NULL 
             AND final_quantity <> "zero"
           GROUP BY 1
           ORDER BY 2 DESC, 3 DESC
           ') 

write.csv(q1,'q1.csv')

## What are the top 5 brands by sales among users that have had their account for at least six months?

q2 <- sqldf('
           SELECT CASE WHEN brand LIKE "%DEAN%DAIRY%" THEN "DEANS DAIRY" ELSE brand END AS brand,
              sum(final_sale) AS final_sale,
              count(DISTINCT user_id) AS users
           FROM combo
           WHERE coalesce(monthssignedupatscan,0) >= 6
             AND coalesce(brand,"") <> ""
             AND final_sale IS NOT NULL 
             AND final_quantity <> "zero"
           GROUP BY 1
           ORDER BY 2 DESC, 3 DESC
           ') 

write.csv(q2,'q2.csv')

## What is the percentage of sales in the Health & Wellness category by generation?

q3 <- sqldf('
            SELECT *,
                final_sale * 1.000 / sum(final_sale) OVER () AS pct_final_sale
            FROM (
                SELECT 
                    CASE WHEN ageatscan < 30 THEN "a <30"
                        WHEN ageatscan < 40 THEN "b 30-40"
                        WHEN ageatscan < 50 THEN "c 40-50"
                        WHEN ageatscan < 60 THEN "d 50-60"
                        WHEN ageatscan < 70 THEN "e 60-70"
                        WHEN ageatscan < 80 THEN "f 70-80"
                        WHEN ageatscan < 90 THEN "g 80-90"
                        ELSE NULL END AS generation,
                    sum(final_sale) AS final_sale,
                    count(DISTINCT user_id) AS users
                FROM combo
                WHERE coalesce(ageatscan,0) > 0
                  AND category = "Health & Wellness"
                  AND final_sale IS NOT NULL 
                  AND final_quantity <> "zero"
                GROUP BY 1
            ) z
            ORDER BY 2 DESC, 3 DESC
            ') 

write.csv(q3,'q3.csv')

## Who are Fetch’s power users?

q4 <- sqldf('
            SELECT *,
                final_sale * 1.000 / sum(final_sale) OVER () AS pct_final_sale
            FROM (
               SELECT user_id,
                  count(DISTINCT receipt_id) AS receipts,
                  sum(final_sale) AS final_sale
               FROM combo
               WHERE coalesce(user_id,"") <> ""
                 AND final_sale IS NOT NULL 
                 AND final_quantity <> "zero"
                 AND ind_users_table = "ON FILE"
               GROUP BY 1
            ) x
            ') 

write.csv(q4,'q4.csv')

## Which is the leading brand in the Dips & Salsa category?

q5 <- sqldf('
            SELECT *,
                final_sale * 1.000 / sum(final_sale) OVER () AS pct_final_sale,
                final_sale2 * 1.000 / sum(final_sale2) OVER () AS pct_final_sale2
            FROM (
               SELECT CASE WHEN brand LIKE "%DEAN%DAIRY%" THEN "DEANS DAIRY" ELSE brand END AS brand, 
                  count(DISTINCT receipt_id) AS receipts,
                  sum(final_sale) AS final_sale,
                  count(DISTINCT CASE WHEN ind_users_table = "ON FILE" THEN receipt_id END) AS receipts2,
                  sum(CASE WHEN ind_users_table = "ON FILE" THEN final_sale ELSE 0 END) AS final_sale2
               FROM combo
               WHERE lower(category) LIKE "%dip%"
                 AND coalesce(brand,"") <> ""
                 AND final_sale IS NOT NULL 
                 AND final_quantity <> "zero"
               GROUP BY 1
            ) x
            ') 

write.csv(q5,'q5.csv')