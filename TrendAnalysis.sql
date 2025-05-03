select * from gold.dim_customers;

select * from gold.dim_products;

select * from gold.fact_sales;

-- Measuring change time trends (total sales by year , average cost by month)
-- analyze sales performance over time


select 
	extract( year from order_date) as "Year" , 
	extract( month from order_date) as "Month",
	sum(sales_amount) as Total_sales, 
	count(distinct customer_key) as Total_customers, 
	sum(quantity) as Total_Quantity
from gold.fact_sales
where order_date is not null
group by  
	extract( year from order_date) , 
	extract( month from order_date)
order by 1 ,2 asc;

-- using date trunc

select 
	date_trunc( 'month', order_date) as order_date, 	
	sum(sales_amount) as Total_sales, 
	count(distinct customer_key) as Total_customers, sum(quantity) as Total_Quantity
from gold.fact_sales
where order_date is not null
group by  
	date_trunc( 'month', order_date) 
order by date_trunc( 'month', order_date);



-- using to_char

select 
	to_char(  order_date, 'yyyy-mon') as order_date, 	
	sum(sales_amount) as Total_sales, 
	count(distinct customer_key) as Total_customers, sum(quantity) as Total_Quantity
from gold.fact_sales
where order_date is not null
group by  
	to_char(  order_date, 'yyyy-mon')
order by to_char(  order_date, 'yyyy-mon');





-- CUMULATIVE ANALYSIS
--RUNNIN TOTAL SALES BY YEAR
--MOVING AVERAGES OF SALES  BY MONTH
-- calculate total sales per month , the running total of sales overtime


-- Runnin total (1)

select customer_key,min(order_date) as min_date
from (select *, sum(sales_amount)over(partition by customer_key order by order_date) as total
from gold.fact_sales) as sub_query 
where total >= 10000
group by 1
order by 2;

select *, sum(sales_amount)over(partition by customer_key order by order_date) as total
from gold.fact_sales;

--Runnin total (2)
select 
	order_date,
	Total_sales,
	avg_price,
	sum(Total_sales)over( order by order_date) as runnin_total_sales,
	avg(avg_price)over( order by order_date) as moving_avg_price
from
(
	select date_trunc( 'year', order_date) as order_date , sum(sales_amount) as Total_sales,
	avg(price) as avg_price
	from gold.fact_sales
	group by 1
	order by 1
) where order_date is not null

-- performance anlysis
-- current measure - target measure
-- analyze the yearly perfomamce of products by comparing each products sales to both its average sales performance and the previous years sales 


select 
	extract(year from f.order_date) as order_year,
	p.product_name,
	sum(f.sales_amount) as current_sales
from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
	where f.order_date is not null
group by 1,2
order by 3;


select order_year,
	product_name,
	current_sales, 
	lag(current_sales,1,0) over(partition by product_name order by order_year) as prev_sales,
	current_sales - lag(current_sales,1,0) over(partition by product_name order by order_year) as sales_diff
from (
	select 
	extract(year from f.order_date) as order_year,
	p.product_name,
	sum(f.sales_amount) as current_sales
	from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
	where f.order_date is not null
	group by 1,2
	order by 3
) subq;

--- using a cte 

with yearly_product_sales as (
select 
	extract(year from f.order_date) as order_year,
	p.product_name,
	sum(f.sales_amount) as current_sales
	from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
	where f.order_date is not null
	group by 1,2
) 
select order_year,
	product_name,
	current_sales,
	avg(current_sales)over(partition by product_name) avg_sales,
	current_sales - avg(current_sales)over(partition by product_name) avg_diff,
	case 
		when  current_sales - avg(current_sales)over(partition by product_name)  > 0 then 'Above_avg'
		when current_sales - avg(current_sales)over(partition by product_name)  < 0 then 'below_avg'
		else 'Average'
	end Avg_change,
	lag(current_sales,1,0) over(partition by product_name order by order_year) as prev_sales,
	current_sales - lag(current_sales,1,0) over(partition by product_name order by order_year) as sales_diff,
	case 
		when current_sales - lag(current_sales,1,0) over(partition by product_name order by order_year)  > 0 then 'Increase'
		when current_sales - lag(current_sales,1,0) over(partition by product_name order by order_year) < 0 then 'decrease'
		else 'No change'
	end Sales_change
from yearly_product_sales 
order by product_name, order_year;

-- Part to whole analysis (how an individual category is contributing to the whole (measure/total measure)*100)
-- sales/total sales 
-- quantity / total quantity

with sales_and_quantity as (
select 
	category,sum(f.sales_amount) as total_sales,
	count(f.quantity) as total_quantity
from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
group by 1
)

select *,
	sum(total_sales)over()  overrall_sales,
	round(total_sales/sum(total_sales)over(),2)*100 sales_percentage,
	count(total_quantity)over() overall_qunatity,
	round(total_quantity/count(total_quantity)over(),2)*100 quantity_percentage
from sales_and_quantity;

-- which categories contribute to overall sales
with category_sales as (
select 
	p.category,sum(sales_amount) as Total_sales
from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
	group by 1
)

select 
	category,
	Total_sales, 
	sum(Total_sales)over() overall_sales,
	round((Total_sales/sum(Total_sales)over())*100,2) percentage_sales
from category_sales
order by 1 desc;


-- data segmentation
-- segment products inot cost ranges and count how many products fall into each segment
with product_segments as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'below 100'
	when cost between 100 and 500 then '100-500'
	when cost between 500 and 1000 then '500-1000'
	else 'Above 1000'
end cost_range
from gold.dim_products
)

select 
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products;

-- Group customers into three segments based on their spending behavior:
-- VIP: Customers with at least 12 months of history and spending more than (5,800. 
-- Regular: Customers with at least 12 months of history but spending (5,888 or less. 
-- New: Customers with a lifespan Tess than 12 months.
-- And find the total number of customers by each group

with customer_spending as (
select 
	c.customer_key,
	min(order_date) first_order, 
	max(order_date) last_order,
	sum(f.sales_amount) total_spending,
	 date_part('year', AGE(MAX(order_date), MIN(order_date))) * 12 + date_part('month',age(max(order_date),min(order_date))) life_span
from gold.fact_sales f
	left join gold.dim_customers c 
	on f.customer_key = c.customer_key
group by 1 
order by 1 asc
) 
select 
	customer_segment,
	count(customer_key) total_customers
from 
(
select 
	customer_key,
	case 
		when life_span >= 12 and total_spending > 5000 then 'VIP'
		when life_span >= 12 and total_spending <= 5000 then 'Regular'
		else 'New'
	end customer_segment
	from customer_spending
) group by 1 
  order by 2;


-- customer report
create view gold_report_customers as
with base_query as (
-- Base query to retrieve core columns from table
select f.order_number,
	f.product_key,
	f.order_date,
	f.sales_amount,
	f.quantity,
	c.customer_key,
	c.customer_number,
	CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
	EXTRACT(YEAR FROM AGE(current_date, c.birthdate)) AS age
from gold.fact_sales f
	left join gold.dim_customers c
	on c.customer_key = f.customer_key
	where order_date is not null
), customer_aggregation as (
-- summarizes key metrics at the customer level
select 
	customer_key,
	customer_name,
	customer_number,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) as total_products,
	max(order_date) last_order_date,
	date_part('year', AGE(MAX(order_date), MIN(order_date))) * 12 + date_part('month',age(max(order_date),min(order_date))) life_span
from base_query
group by 1,2,3,4

) select 
	customer_key,
	customer_name,
	customer_number,
	age,
		case 
			when  age < 20  then 'Under 20'
			when  age between 20 and 29 then '20-29'
			when  age between 30 and 39 then '30-39'
			when  age between 40 and 49 then '40-49'
		else '50 and above'
	end age_group,
	total_orders,
    total_sales,
	case 
			when life_span >= 12 and total_sales > 5000 then 'VIP'
			when life_span >= 12 and total_sales <= 5000 then 'Regular'
		else 'New'
	end customer_segment,
	total_quantity,
	total_products,
	last_order_date,
	DATE_PART('year', AGE(current_date, last_order_date)) * 12 +
    DATE_PART('month', AGE(current_date, last_order_date)) AS recency,
	life_span,
	-- computing average order value (avo)
	case 
		when total_sales = 0 or total_orders = 0 then 0
		else total_sales/total_orders
	end as avg_order_value,
	-- computing average monthly spent
	case 
		when total_sales = 0 or life_span = 0 then 0
		else total_sales/life_span
	end as avg_monthly_spending 
from customer_aggregation;


-- product report 

create view gold_products_report as 
with base_query as (
select  
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
	f.sales_amount,
	f.order_number,
	f.quantity,
	f.customer_key,
	f.order_date
from gold.fact_sales f
left join gold.dim_products p 
on  f.product_key = p.product_key
), 
product_aggregation as (
select 	
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	sum(sales_amount) as total_sales,
	count(distinct order_number) as total_orders,
	sum(quantity) as total_quantity,
	count(distinct customer_key)as total_customers,
	max(order_date) as last_sale_date,
	date_part('year', AGE(MAX(order_date), MIN(order_date))) * 12 + date_part('month',age(max(order_date),min(order_date))) life_span,
	round(avg(sales_amount/nullif(quantity,0)),1) as avg_selling_Price
from base_query
group by 1,2,3,4,5
)
	select 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	case
		when total_sales > 50000 then 'High Performer'
		when total_sales < 500000 then 'Mid Ranger'
		else 'Low Performer'
	end product_segment,
	total_orders,
    total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- computing average order value (aor)
	case 
		when total_sales = 0 or total_orders = 0 then 0
		else total_sales/total_orders
	end as avg_order_revenue,
	-- computing average monthly revenue
	case 
		when total_sales = 0 or life_span = 0 then 0
		else total_sales/life_span
	end as avg_monthly_revenue 
from product_aggregation;
