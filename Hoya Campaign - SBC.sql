-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Hoya Campaign for Ótica Holy Glassses
-- MAGIC
-- MAGIC ## 
-- MAGIC
-- MAGIC #### Main objective: Get all customers that bought Hoya products before 2024.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Using Python to get the most updated sales file path
-- MAGIC
-- MAGIC all_files = dbutils.fs.ls('dbfs:/FileStore/tables/')
-- MAGIC sales_files = [file for file in all_files if file.name.startswith('exportacao_venda')]
-- MAGIC sales_files = sorted(sales_files, key=lambda file: file.modificationTime, reverse=True)
-- MAGIC last_file = sales_files[0].path
-- MAGIC last_file
-- MAGIC
-- MAGIC # Using PySpark to create the DataFrame using necessary encoding
-- MAGIC df = spark.read.csv(last_file, inferSchema=True, header=True, encoding='latin1')
-- MAGIC
-- MAGIC # Transforming to pandas to use ffill method
-- MAGIC df_pandas = df.toPandas()
-- MAGIC df_pandas.fillna(method='ffill', inplace=True)
-- MAGIC
-- MAGIC # Getting back to PySpark
-- MAGIC df = spark.createDataFrame(df_pandas)
-- MAGIC
-- MAGIC # Creating table to use SQL
-- MAGIC df.createOrReplaceTempView('hoya_campaign')

-- COMMAND ----------

-- 'Hoya Customers that bought before 2024 with valid contact number'

SELECT DISTINCT `Cliente`, `Telefones`, `Item - Descrição`
FROM hoya_campaign
  WHERE `Item - Descrição` LIKE '%Hoya%'
    AND YEAR(to_timestamp(`Data`, 'dd/MM/yyyy HH:mm')) != 2024
    AND `Telefones`!= '--- Telefone não informado ---'

-- COMMAND ----------

-- Saving results in a new table

CREATE OR REPLACE TABLE hoya_customers_campaign AS
SELECT DISTINCT `Cliente`, `Telefones`, `Item - Descrição` AS Item_Descricao
FROM hoya_campaign
  WHERE `Item - Descrição` LIKE '%Hoya%'
    AND YEAR(to_timestamp(`Data`, 'dd/MM/yyyy HH:mm')) != 2024
    AND `Telefones`!= '--- Telefone não informado ---'

-- COMMAND ----------

-- 'Hoya Customers that bought before 2024 with valid contact number' -- ### Hidding sensitive information for GitHub purpose ###

SELECT
  CONCAT(SPLIT(`Cliente`, ' ')[0], ' ##-CENSORED') AS Cliente,
  '99999-9999' AS Telefones,
  `Item_Descricao`
FROM hoya_customers_campaign


-- COMMAND ----------


