create database maven_coffee_analytics;
use maven_coffee_analytics;
create table transactions (
	transaction_id int,
	transaction_date varchar(30),
    transaction_time time,
	transaction_qty int,
	store_id int,
	store_location varchar(100),
	product_id int,
	unit_price decimal(10,2),
	product_category varchar(50),
	product_type varchar(100),
	product_detail varchar(100)
);

-- Analysis Start


-- total rows
select
count(*) as `Total Rows`
from transactions;
-- Start and End Date
select
	min(str_to_date(transaction_date,'%m/%d/%Y')) as `Start Date`,
	max(str_to_date(transaction_date,'%m/%d/%Y')) as `End Date`
from transactions;

-- Inspect The Data
select
*
from transactions
limit 5;

-- NULL values
SELECT 
    COUNT(*) AS total_rows,
    SUM(transaction_id IS NULL) AS null_txn_id,
    SUM(transaction_date IS NULL) AS null_date,
    SUM(transaction_qty IS NULL) AS null_qty
FROM transactions;

-- Duplicate transaction IDs
SELECT transaction_id, COUNT(*) 
FROM transactions 
GROUP BY transaction_id 
HAVING COUNT(*) > 1;

-- Sales Summary
select
	count(distinct transaction_id) as 'Total Transactions',
	sum(transaction_qty) as 'Total Units Sold',
	ceil(sum(transaction_qty*unit_price)) 'Total Revenue'
from transactions;

-- Revenue Heatmap: Day of Week vs Hour

select 
	dayname(str_to_date(transaction_date, '%m/%d/%Y')) AS day_of_week,
	sum(case when hour(transaction_time) = 7 then ceil(transaction_qty * unit_price) else 0 end) as `07`,
    sum(case when hour(transaction_time) = 8 then ceil(transaction_qty * unit_price) else 0 end) as `08`,
    sum(case when hour(transaction_time) = 9 then ceil(transaction_qty * unit_price) else 0 end) as `09`,
    sum(case when hour(transaction_time) = 10 then ceil(transaction_qty * unit_price) else 0 end) as `10`,
    sum(case when hour(transaction_time) = 11 then ceil(transaction_qty * unit_price) else 0 end) as `11`,
    sum(case when hour(transaction_time) = 12 then ceil(transaction_qty * unit_price) else 0 end) as `12`,
    sum(case when hour(transaction_time) = 13 then ceil(transaction_qty * unit_price) else 0 end) as `13`,
    sum(case when hour(transaction_time) = 14 then ceil(transaction_qty * unit_price) else 0 end) as `14`,
    sum(case when hour(transaction_time) = 15 then ceil(transaction_qty * unit_price) else 0 end) as `15`,
    sum(case when hour(transaction_time) = 16 then ceil(transaction_qty * unit_price) else 0 end) as `16`,
    sum(case when hour(transaction_time) = 17 then ceil(transaction_qty * unit_price) else 0 end) as `17`,
    sum(case when hour(transaction_time) = 18 then ceil(transaction_qty * unit_price) else 0 end) as `18`,
    sum(case when hour(transaction_time) = 19 then ceil(transaction_qty * unit_price) else 0 end) as `19`,
    sum(case when hour(transaction_time) = 20 then ceil(transaction_qty * unit_price) else 0 end) as `20`
from transactions
group by day_of_week
order by FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');

-- Product Summary
select
	count(distinct product_category) as 'Total Product Category',
    count(distinct product_type) as 'Product Type'
from transactions;

-- Product Count under Category
select
distinct product_category as 'Category',
count(distinct product_type) as 'Total Products'
from transactions
group by 1
order by 2 desc;

-- Products Under Category
select
distinct product_category as 'Category Name',
group_concat(distinct product_type separator ', ') as 'Products'
from transactions
group by 1;

-- Top Selling Products
select
product_type as 'Product Name',
sum(transaction_qty) as 'Total Unit Sold'
from transactions
group by 1
order by 2 desc
limit 5;

-- Top Revenue Generating Products
select
product_type as 'Product Name',
ceil(sum(transaction_qty*unit_price)) as 'Total Revenue'
from transactions
group by 1
order by 2 desc
limit 5;

-- Compare Each Product's Average Price to Category Average Price
with category_avg as (
  select 
    product_category,
    round(avg(unit_price), 2) as category_avg_price
  from transactions
  group by product_category
)
select 
  t.product_category,
  t.product_type,
  round(avg(t.unit_price), 2) as avg_price,
  c.category_avg_price,
  case
	when round(avg(t.unit_price), 2) > c.category_avg_price 
		then 'Above Category Avg'
		else 'At or Below Category Avg'
	end as price_flag
from transactions t
join category_avg c
  on t.product_category = c.product_category
group by t.product_category, t.product_type, c.category_avg_price
order by t.product_category, avg_price desc;




-- Categorywise MoM growth

with category_monthly_revenue as (
	select
		product_category,
        DATE_FORMAT(STR_TO_DATE(transaction_date, '%m/%d/%Y'), '%Y-%m') AS month,
        ceil(SUM(transaction_qty * unit_price)) AS revenue
	from transactions
    group by product_category, month
),
catrgory_growth as (
	select
		product_category,
		month,
		revenue,
        ceil(lag(revenue) over (partition by product_category order by month)) as prev_month_revenue
	from category_monthly_revenue
)
select
	product_category,
	month,
	revenue,
	prev_month_revenue,
	ROUND((revenue - prev_month_revenue)/prev_month_revenue * 100, 2) AS `mom_growth(%)`
from catrgory_growth;

-- Top 3 revenue generating products from each store

with product_revenue as(
	select
    store_location,
    product_category,
    ceil(SUM(transaction_qty * unit_price)) AS revenue
    from transactions
    group by 1,2
),
ranked_products as (
	select
    *,
    rank() over (partition by store_location order by revenue desc) as `rank`
    from product_revenue
)
select
*
from ranked_products
where `rank`<=3;

-- Price sensitivity per product
select
  product_type,
  ROUND(avg(unit_price), 2) as avg_price,
  sum(transaction_qty) AS total_qty,
  ceil(sum(transaction_qty*unit_price)) as 'Total Revenue',
  ceil(sum(transaction_qty) / avg(unit_price)) AS qty_per_price_unit,
  ceil(sum(transaction_qty*unit_price) / avg(unit_price)) AS rev_per_price_unit
from transactions
group by product_type
order by qty_per_price_unit desc;

-- Product Type Drop-off Detection
with sales_range as (
  select
    product_type,
    min(str_to_date(transaction_date, '%m/%d/%Y')) as first_sold,
    max(str_to_date(transaction_date, '%m/%d/%Y')) as last_sold
  from transactions
  group by product_type
)
select
  product_type,
  first_sold,
  last_sold,
  datediff(last_sold, first_sold) as active_days
from sales_range
order by active_days asc;

-- Store Summary
select
count(distinct store_location) as 'Store Location Count',
count(distinct store_id) as 'Total No. of Stores'
from transactions;

-- Revenue by Stores
select
store_location as 'Store Name',
sum(transaction_qty*unit_price) as 'Total Revenue'
from transactions
group by 1
order by 2 desc;

-- Time Series Analysis

-- Peak Hour Analysis
select
hour(transaction_time) as 'Hour',
count(transaction_qty) as 'Quantity Sold'
from transactions
group by 1
order by 1;

-- Monthly Sales Trends
-- With CTE

with monthly_revenue as (
 select
	date_format(str_to_date(transaction_date,'%m/%d/%Y'),'%Y-%m') as 'Month',
	sum(transaction_qty*unit_price) as 'Total Revenue'
from transactions
group by 1
)
select
`Month`,
`Total Revenue`,
lag(`Total Revenue`) over (order by `Month`) as `Previous Month Revenue`,
round(
	(`Total Revenue` -  lag( `Total Revenue` ) over (order by `Month`)) /
	(lag(`Total Revenue`) over (order by `Month`))*100
	,2) as 'MoM (%) Change'
from monthly_revenue;

-- Basic

select
date_format(str_to_date(transaction_date,'%m/%d/%Y'),'%Y-%m') as 'Month',
sum(transaction_qty*unit_price) as 'Total Revenue',
lag(
	sum(transaction_qty*unit_price))
    over
    (order by(date_format(str_to_date(transaction_date,'%m/%d/%Y'),'%Y-%m'))
    ) as 'Previous Month Revenue',
round((sum(transaction_qty*unit_price) -  
lag(
	sum(transaction_qty*unit_price))
    over
    (order by(date_format(str_to_date(transaction_date,'%m/%d/%Y'),'%Y-%m'))
    )
) /
(lag(
	sum(transaction_qty*unit_price))
    over
    (order by(date_format(str_to_date(transaction_date,'%m/%d/%Y'),'%Y-%m'))
    )
)*100,2) as 'MoM (%) Change'
from transactions
group by 1;

-- Avg. Daily Sell of Products and avg daily revenue

select
	product_type as `Product Name`,
	sum(transaction_qty) as `Sold Quantity`,
	count(distinct date(str_to_date(transaction_date,'%m/%d/%Y'))) as `Days Sold`,
	floor(sum(transaction_qty)/count(distinct date(str_to_date(transaction_date,'%m/%d/%Y')))) as `Avg. Daily Sold`,
	floor(sum(transaction_qty*unit_price)/count(distinct date(str_to_date(transaction_date,'%m/%d/%Y')))) as `Avg. Daily Revenue`
from transactions
group by 1
order by 5 desc;