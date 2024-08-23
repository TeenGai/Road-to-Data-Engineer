# Data Analysis

In this Data Analysis section, I will attach the SQL code that I used to query the result for further Visualization according to the requirements as following:

## Requirements
### Revenue
- Total Sales Over Time
- Total Sales by Year
- Total Sales by Month
- Total Sales by Product
- Sales Distribution by Country
- Countries with the Highest and Lowest Sales

### Customer 
- Top Customers by Purchased Amount
- Average Amount Per Transaction
- Top 10 Customers Purchased Frequency

### Product
- Most Expensive and Cheapest Products by Country
- Top 100 Products Overall by Quantity
- Top 10 Products by Quantity for Each Country
- Top 100 Products Overall by Total Sales
- Top 10 Products by Total Sales for Each Country

## SQL Code
*There are some results with too many rows, so, I will limit to only 5 rows.*

`Total Sales Over Time`
```
SELECT
  ROUND(SUM(total_amount), 2) total_sales
FROM transaction
```
**Result**
| total_sales  |
| ------------ |
| 2691511249.8 |
______________________________________________________________________________________________________________________________
`Total Sales by Year`
```
SELECT
  EXTRACT(year FROM date) year,
  ROUND(SUM(total_amount), 2) total_sales,
  COUNT(DISTINCT EXTRACT(month FROM date)) total_months
FROM transaction
GROUP BY year
ORDER BY year
```

**Result**
| year | total_sales  | total_months |
| ---- | ------------ | ------------ |
| 2023 | 1424860390.8 | 8            |
| 2024 | 1266650859.0 | 5            |
______________________________________________________________________________________________________________________________
`Total Sales by Month`
```
SELECT
  EXTRACT(year FROM date) year,
  CASE
    WHEN EXTRACT(month FROM date) = 5 AND EXTRACT(year FROM date) = 2023 THEN 'May'
    WHEN EXTRACT(month FROM date) = 6 AND EXTRACT(year FROM date) = 2023 THEN 'June'
    WHEN EXTRACT(month FROM date) = 7 AND EXTRACT(year FROM date) = 2023 THEN 'July'
    WHEN EXTRACT(month FROM date) = 8 AND EXTRACT(year FROM date) = 2023 THEN 'August'
    WHEN EXTRACT(month FROM date) = 9 AND EXTRACT(year FROM date) = 2023 THEN 'September'
    WHEN EXTRACT(month FROM date) = 10 AND EXTRACT(year FROM date) = 2023 THEN 'October'
    WHEN EXTRACT(month FROM date) = 11 AND EXTRACT(year FROM date) = 2023 THEN 'November'
    WHEN EXTRACT(month FROM date) = 12 AND EXTRACT(year FROM date) = 2023 THEN 'December'
    WHEN EXTRACT(month FROM date) = 1 AND EXTRACT(year FROM date) = 2024 THEN 'January'
    WHEN EXTRACT(month FROM date) = 2 AND EXTRACT(year FROM date) = 2024 THEN 'February'
    WHEN EXTRACT(month FROM date) = 3 AND EXTRACT(year FROM date) = 2024 THEN 'March'
    WHEN EXTRACT(month FROM date) = 4 AND EXTRACT(year FROM date) = 2024 THEN 'April'
    WHEN EXTRACT(month FROM date) = 5 AND EXTRACT(year FROM date) = 2024 THEN 'May'
    ELSE 'Other' 
  END AS month,
  ROUND(SUM(total_amount), 2) total_sales,
  EXTRACT(month FROM date) month_num
FROM transaction
GROUP BY year, month, month_num
ORDER BY year, month_num
```

**Result**
| year | month      | total_sales  | month-num |
| ---- | ---------- | ------------ | --------- |
| 2023 | May        | 179635161.58 | 5         |
| 2023 | June       | 152447033.51 | 6         |
| 2023 | July       | 154835448.99 | 7         |
| 2023 | August     | 193101001.21 | 8         |
| 2023 | September  | 151196608.13 | 9         |

...
> *Display only 5 rows...*
_____________________________________________________________________________________________________________________________
`Total Sales by Product`
```
SELECT
  product_name,
  ROUND(SUM(total_amount), 2) total_sales
FROM transaction
GROUP BY product_name
ORDER BY product_name
```

**Result** 
| product_name                 | total_sales |
| ---------------------------- | ----------- |
| 10 Colour Spaceboy Pen       | 3132186.45  |
| 12 Coloured Party Balloons   | 1005682.09  |
| 12 Daisy Pegs In Wood Box    | 183282.03   |
| 12 Egg House Painted Wood    | 168034.04   |
| 12 Hanging Eggs Hand Painted | 16571.67    |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Sales Distribution by Country`
```
SELECT
  country,
  ROUND(SUM(total_amount), 2) total_sales
FROM transaction
GROUP BY country
ORDER BY country
```

**Result** 
| country   | total_sales |
| --------- | ----------- |
| Australia | 44008366.64 |
| Austria   | 3090603.88  |
| Bahrain   | 145623.62   |
| Belgium   | 12087384.2  |
| Brazil    | 205361.3    |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Countries with the Highest and Lowest Sales`
```
WITH total_sales_by_country AS (
  SELECT
    country,
    ROUND(SUM(total_amount), 2) total_sales
  FROM transaction
  GROUP BY country
)

# Highest Sales
SELECT 
  country,
  total_sales
FROM total_sales_by_country
WHERE total_sales = (SELECT MAX(total_sales) FROM total_sales_by_country)

# Lowest Sales
SELECT 
  country,
  total_sales
FROM total_sales_by_country
WHERE total_sales = (SELECT MIN(total_sales) FROM total_sales_by_country)
```

**Result of Highest Sales**
| country        | total_sales   |
| -------------- | ------------- |
| United Kingdom | 2231811256.35 |

**Result of Lowest Sales**
| country      | total_sales |
| ------------ | ----------- |
| Saudi Arabia | 39786.3     |
______________________________________________________________________________________________________________________________
`Top Customers by Purchased Amount`
```
SELECT
  customer_name,
  country,
  ROUND(SUM(total_amount), 2) total_purchased
FROM transaction
GROUP BY customer_name, country
ORDER BY total_purchased DESC, customer_name
```

**Result**
| customer_name | country        | total_purchased |
| ------------- | -------------- | --------------- |
| name1         | Netherlands    | 94543982.99     |
| name2         | United Kingdom | 40132234.23     |
| name3         | Australia      | 39849056.93     |
| name4         | United Kingdom | 39385532.17     |
| name5         | EIRE           | 39197261.33     |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Average Amount Per Transaction`
```
SELECT  
  ROUND(SUM(total_amount) / COUNT(DISTINCT transaction_id), 2) average_per_transaction
FROM transaction
```
**Result**
| average_per_transaction  |
| ------------------------ |
| 116173.66                |
______________________________________________________________________________________________________________________________
`Top 10 Customers Purchased Frequency`
```
SELECT  
  customer_name,
  COUNT(DISTINCT transaction_id) total_transactions
FROM transaction
GROUP BY customer_name
ORDER BY total_transactions DESC
LIMIT 10
```

**Result of Lowest Sales**
| customer_name | total_transactions |
| ------------- | ------------------ |
| name1         | 241                |
| name2         | 218                |
| name3         | 170                |
| name4         | 125                |
| name5         | 119                |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Most Expensive and Cheapest Products by Country`
```
# Highest Sales
SELECT 
  t1.product_name,
  t1.price,
  t1.country
FROM transaction t1
JOIN (SELECT 
        country,
        MAX(price) AS max_price
      FROM 
        transaction 
      GROUP BY 
        country) t2
ON t1.country = t2.country
  AND t1.price = t2.max_price
ORDER BY t1.country, t1.product_name

# Lowest Sales
SELECT 
  t1.product_name,
  t1.price,
  t1.country
FROM transaction t1
JOIN (SELECT 
        country,
        MIN(price) AS min_price
      FROM 
        transaction 
      GROUP BY 
        country) t2
ON t1.country = t2.country
  AND t1.price = t2.min_price
ORDER BY t1.country, t1.product_name
```

**Result of Products with Lowest Price by Country**
| product_name                       | price             | country   |
| ---------------------------------- | ----------------- | --------- |
| Jumbo Bag Spaceboy Design          | 236.78            | Australia |
| Gift Bag Psychedelic Apples        | 252.69            | Austria   |
| Mini Cake Stand With Hanging Cakes | 519.06            | Bahrain   |
| Lunch Bag Apple Design             | 260.88            | Belgium   |
| Lunch Bag Cars Blue                | 260.88            | Belgium   |

...
> *Display only 5 rows...*

**Result of Products with Highest Price by Country**
| product_name                    | price             | country   |
| ------------------------------- | ----------------- | --------- |
| Bread Bin Diner Style Pink      | 1971.45           | Australia |
| Zinc Top 2 Door Wooden Shelf    | 1971.45           | Australia |
| 3 Tier Cake Tin Green And Cream | 1826.17           | Bahrain   |
| 3 Tier Cake Tin Red And Cream   | 1826.17           | Belgium   |
| Regency Cakestand 3 Tier        | 1054.55           | Belgium   |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Top 100 Products Overall by Quantity`
```
SELECT
  product_name,
  country,
  SUM(quantity) total_quantity
FROM transaction
GROUP BY product_name, country
ORDER BY total_quantity DESC
LIMIT 100
```

**Result**
| product_name                       | country        | total_quantity |
| ---------------------------------- | -------------- | -------------- |
| Popcorn Holder                     | United Kingdom | 52802          |
| World War 2 Gliders Asstd Designs  | United Kingdom | 48408          |    
| Jumbo Bag Red Retrospot            | United Kingdom | 43004          |
| Assorted Colour Bird Ornament      | United Kingdom | 33470          |
| Cream Hanging Heart T-Light Holder | United Kingdom | 33209          |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Top 10 Products by Quantity for Each Country`
```
SELECT
  product_name,
  country filtered_country,
  SUM(quantity) total_quantity
FROM transaction
WHERE country = 'filtered_country'
GROUP BY product_name, country
ORDER BY total_quantity DESC
LIMIT 10
```

**Result**
| product_name | filtered_country | total_quantity |
| ------------ | ---------------- | -------------- |
| product_name | filtered_country | total_quantity |
| product_name | filtered_country | total_quantity |    
| product_name | filtered_country | total_quantity |
| product_name | filtered_country | total_quantity |
| product_name | filtered_country | total_quantity |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Top 100 Products Overall by Total Sales`
```
SELECT
  product_name,
  ROUND(SUM(total_amount), 2) total_sales
FROM transaction
GROUP BY product_name
ORDER BY total_sales DESC
LIMIT 100
```

**Result** 
| product_name                       | total_sales |
| ---------------------------------  | ----------- |
| Popcorn Holder                     | 26180005.69 |
| World War 2 Gliders Asstd Designs  | 24824048.51 |
| Paper Craft Little Birdie          | 22946693.45 |
| Cream Hanging Heart T-Light Holder | 20082501.07 |
| Assorted Colour Bird Ornament      | 18742477.06 |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
`Top 10 Products by Total Sales for Each Country`
```
SELECT
  product_name,
  country filtered_country,
  ROUND(SUM(total_amount), 2) total_sales
FROM transaction
WHERE country = 'filtered_country'
GROUP BY product_name, country
ORDER BY total_sales DESC
LIMIT 10
```

**Result**
| product_name | filtered_country | total_sales |
| ------------ | ---------------- | ----------- |
| product_name | filtered_country | total_sales |
| product_name | filtered_country | total_sales |    
| product_name | filtered_country | total_sales |
| product_name | filtered_country | total_sales |
| product_name | filtered_country | total_sales |

...
> *Display only 5 rows...*
______________________________________________________________________________________________________________________________
