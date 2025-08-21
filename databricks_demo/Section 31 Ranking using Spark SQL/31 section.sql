-- Databricks notebook source
select * from PARQUET.'dbfs:/public/retail_db/daily_product_revenue'
order by 1,3

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW daily_product_revenue
using parquet
options (
path= 'dbfs:/public/retail_db/daily_product_revenue'
)



-- COMMAND ----------

select * from daily_product_revenue
order by 1,3

-- COMMAND ----------

select dpr.*
from daily_product_revenue as dpr
where dpr.order_date = '2013-07-26 00:00:00.0'
order by 1,3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ranking using SPARK SQL Windows Functions

-- COMMAND ----------

select dpr.*,
  rank() over (order by revenue desc) as rnk
from daily_product_revenue as dpr
where dpr.order_date = '2013-07-26 00:00:00.0'
order by 1,3 desc

-- COMMAND ----------

select dpr.*,
  rank() over (partition by order_date order by revenue desc) as rnk
from daily_product_revenue as dpr
order by 1,3 desc

-- COMMAND ----------

with cte as (
SELECT 1 AS sale_rep_id, 1090 AS sale_revenue
UNION
SELECT 2 AS sale_rep_id, 1200 AS sale_revenue
UNION
SELECT 3 AS sale_rep_id, 1300 AS sale_revenue
UNION
SELECT 4 AS sale_rep_id, 125 AS sale_revenue
UNION
SELECT 5 AS sale_rep_id, 1300 AS sale_revenve
UNION
SELECT 6 AS sale_rep_id, 1200 AS sale_revenue
UNION
SELECT 7 AS sale_rep_id, 1200 AS sale_revenue
) SELECT cte.*,
    dense_rank() over (order by sale_revenue desc) as dens_rnk,
    rank() over (order by sale_revenue desc) as rnk 
FROM cte
ORDER BY 2 desc


-- COMMAND ----------

select dpr.*,
  rank() over (order by revenue desc) as rnk,
  dense_rank() over (order by revenue desc) as dens_rnk
from daily_product_revenue as dpr
where dpr.order_date = '2013-07-26 00:00:00.0'
order by 1,3 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # filtering on ranks

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Nested query below
-- MAGIC

-- COMMAND ----------

select * from (
  select dpr.*,
  rank() over (order by revenue desc) as rnk,
  dense_rank() over (order by revenue desc) as dens_rnk
  from daily_product_revenue as dpr
  where dpr.order_date = '2013-07-26 00:00:00.0'
)
where dens_rnk <= 3
order by 1,3 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using CTE`s

-- COMMAND ----------

with daily_product_revenue_ranked_cte as (
  select dpr.*,
  rank() over (order by revenue desc) as rnk,
  dense_rank() over (order by revenue desc) as dens_rnk
  from daily_product_revenue as dpr
) select * from daily_product_revenue_ranked_cte
where rnk <= 5
order by 1,3 desc

-- COMMAND ----------

CREATE or REPLACE TEMPORARY VIEW daily_product_revenue_ranked_v
AS
select dpr.*,
rank() OVER (partition by order_date ORDER BY revenue DESC) as rnk
FROM daily_product_revenue as dpr

-- COMMAND ----------

select * 
from daily_product_revenue_ranked_v
where rnk <= 3
order by 1,3 desc