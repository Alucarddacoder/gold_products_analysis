-- View: public.gold_report_customers

-- DROP VIEW public.gold_report_customers;

CREATE OR REPLACE VIEW public.gold_report_customers
 AS
 WITH base_query AS (
         SELECT f.order_number,
            f.product_key,
            f.order_date,
            f.sales_amount,
            f.quantity,
            c.customer_key,
            c.customer_number,
            concat(c.first_name, ' ', c.last_name) AS customer_name,
            EXTRACT(year FROM age(CURRENT_DATE::timestamp with time zone, c.birthdate::timestamp with time zone)) AS age
           FROM gold.fact_sales f
             LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
          WHERE f.order_date IS NOT NULL
        ), customer_aggregation AS (
         SELECT base_query.customer_key,
            base_query.customer_name,
            base_query.customer_number,
            base_query.age,
            count(DISTINCT base_query.order_number) AS total_orders,
            sum(base_query.sales_amount) AS total_sales,
            sum(base_query.quantity) AS total_quantity,
            count(DISTINCT base_query.product_key) AS total_products,
            max(base_query.order_date) AS last_order_date,
            date_part('year'::text, age(max(base_query.order_date)::timestamp with time zone, min(base_query.order_date)::timestamp with time zone)) * 12::double precision + date_part('month'::text, age(max(base_query.order_date)::timestamp with time zone, min(base_query.order_date)::timestamp with time zone)) AS life_span
           FROM base_query
          GROUP BY base_query.customer_key, base_query.customer_name, base_query.customer_number, base_query.age
        )
 SELECT customer_key,
    customer_name,
    customer_number,
    age,
        CASE
            WHEN age < 20::numeric THEN 'Under 20'::text
            WHEN age >= 20::numeric AND age <= 29::numeric THEN '20-29'::text
            WHEN age >= 30::numeric AND age <= 39::numeric THEN '30-39'::text
            WHEN age >= 40::numeric AND age <= 49::numeric THEN '40-49'::text
            ELSE '50 and above'::text
        END AS age_group,
    total_orders,
    total_sales,
        CASE
            WHEN life_span >= 12::double precision AND total_sales > 5000::numeric THEN 'VIP'::text
            WHEN life_span >= 12::double precision AND total_sales <= 5000::numeric THEN 'Regular'::text
            ELSE 'New'::text
        END AS customer_segment,
    total_quantity,
    total_products,
    last_order_date,
    date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, last_order_date::timestamp with time zone)) * 12::double precision + date_part('month'::text, age(CURRENT_DATE::timestamp with time zone, last_order_date::timestamp with time zone)) AS recency,
    life_span,
        CASE
            WHEN total_sales = 0::numeric OR total_orders = 0 THEN 0::numeric
            ELSE total_sales / total_orders::numeric
        END AS avg_order_value,
        CASE
            WHEN total_sales = 0::numeric OR life_span = 0::double precision THEN 0::double precision
            ELSE total_sales::double precision / life_span
        END AS avg_monthly_spending
   FROM customer_aggregation;

ALTER TABLE public.gold_report_customers
    OWNER TO postgres;

