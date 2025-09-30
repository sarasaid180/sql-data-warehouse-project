/* 
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
       ci.[cst_id] as customer_id
      ,ci.[cst_key] as customer_number
      ,ci.[cst_firstname] as first_name
      ,ci.[cst_lastname] as last_name
      ,la.cntry as country
      ,ci.[cst_marital_status] marital_status
      ,CASE
          WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
          ELSE COALESCE(ca.gen, 'n/a')               -- Fallback to ERP data
       END AS gender
      ,ca.bdate as birthdate
      ,ci.[cst_create_date] as create_date
      
  FROM [DataWarehouse].[silver].[crm_cust_info] as ci
  LEFT JOIN silver.erp_cust_az12 as ca
      ON ci.cst_key = ca.cid
  LEFT JOIN silver.erp_loc_a101 as la
      ON ci.cst_key = la.cid

  GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
CREATE VIEW gold.dim_product AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
       pn.[prd_id] as product_id
      ,pn.[prd_key] as product_number
      ,pn.[prd_nm] as product_name
      ,pn.cat_id as category_id
      ,pc.cat as category
      ,pc.subcat as subcategory
      ,pc.maintenance 
      ,pn.[prd_cost] as cost
      ,pn.[prd_line] as product_line
      ,pn.[prd_start_dt] as start_date

FROM [DataWarehouse].silver.[crm_prd_info] as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
    ON pn.cat_id = pc.id
-- if the end date is null, that means that the product is not ended yet, 
-- so we are removing all the historical products and displaying only the current products
WHERE prd_end_dt IS NULL 

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales AS
SELECT 
       sd.[sls_ord_num] as order_number
      ,pr.[product_key]
      ,cu.customer_key
      ,sd.[sls_order_dt] as order_date
      ,sd.[sls_ship_dt] as shipping_date
      ,sd.[sls_due_dt] as due_date
      ,sd.[sls_sales] as sales_amount
      ,sd.[sls_quantity] quantity
      ,sd.[sls_price]  as price
FROM [DataWarehouse].[silver].[crm_sales_details] sd 
LEFT JOIN gold.dim_product pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    ON sd.sls_cust_id = cu.customer_id

