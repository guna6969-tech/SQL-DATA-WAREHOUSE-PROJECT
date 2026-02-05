-- ============================================================
-- GOLD LAYER (ANALYTICS & REPORTING)
-- ------------------------------------------------------------
-- The GOLD layer contains business-ready, curated views designed
-- specifically for end users, analysts, and BI tools.
-- 
-- • Data is cleaned, enriched, and standardized
-- • Uses surrogate keys for performance and consistency
-- • Organized in a star schema (Dimensions + Fact tables)
-- • Safe to use directly for reporting, dashboards, and analysis
--
-- End users should query ONLY GOLD views and avoid SILVER/BRONZE
-- layers, as those are raw and transformation-focused.
-- ============================================================

CREATE VIEW gold.dim_customers AS
select
ROW_NUMBER () OVER (ORDER BY cst_id) as customer_key,
ci.cst_id  as customer_id,
ci.cst_key as customer_num,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
la.cntry as country,
ci.cst_marital_status as marital_status,
   CASE WHEN ci.cst_gender !='n/a' THEN ci.cst_gender
      else  coalesce(ca.gen,'n/a')
 end as gender,
 ca.bdate as birthdate,
 ci.cst_create_date as create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid


SELECT * FROM gold.dim_customers


create view gold.dim_products as
select 
ROW_NUMBER() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as sub_category,
pc.maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
where prd_end_dt is null

create view gold.fact_sales as
select sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipe_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id



