-- View: public.gold_products_report

-- DROP VIEW public.gold_products_report;

CREATE OR REPLACE VIEW public.gold_products_report
 AS
 WITH base_query AS (
         SELECT p.product_key,
            p.product_name,
            p.category,
            p.subcategory,
            p.cost,
            f.sales_amount,
            f.order_number,
            f.quantity,
            f.customer_key,
            f.order_date
           FROM gold.fact_sales f
             LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
        ), product_aggregation AS (
         SELECT base_query.product_key,
            base_query.product_name,
            base_query.category,
            base_query.subcategory,
            base_query.cost,
            sum(base_query.sales_amount) AS total_sales,
            count(DISTINCT base_query.order_number) AS total_orders,
            sum(base_query.quantity) AS total_quantity,
            count(DISTINCT base_query.customer_key) AS total_customers,
            max(base_query.order_date) AS last_sale_date,
            date_part('year'::text, age(max(base_query.order_date)::timestamp with time zone, min(base_query.order_date)::timestamp with time zone)) * 12::double precision + date_part('month'::text, age(max(base_query.order_date)::timestamp with time zone, min(base_query.order_date)::timestamp with time zone)) AS life_span,
            round(avg(base_query.sales_amount / NULLIF(base_query.quantity, 0)::numeric), 1) AS avg_selling_price
           FROM base_query
          GROUP BY base_query.product_key, base_query.product_name, base_query.category, base_query.subcategory, base_query.cost
        )
 SELECT product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
        CASE
            WHEN total_sales > 50000::numeric THEN 'High Performer'::text
            WHEN total_sales < 500000::numeric THEN 'Mid Ranger'::text
            ELSE 'Low Performer'::text
        END AS product_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
        CASE
            WHEN total_sales = 0::numeric OR total_orders = 0 THEN 0::numeric
            ELSE total_sales / total_orders::numeric
        END AS avg_order_revenue,
        CASE
            WHEN total_sales = 0::numeric OR life_span = 0::double precision THEN 0::double precision
            ELSE total_sales::double precision / life_span
        END AS avg_monthly_revenue
   FROM product_aggregation;

ALTER TABLE public.gold_products_report
    OWNER TO postgres;

