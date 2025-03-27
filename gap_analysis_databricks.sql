#Query 1 to be inserted into query 2

SELECT column_name,concat(table_catalog,'.',table_schema,'.',table_name) as table_name,full_data_type
FROM system.information_schema.columns 
WHERE regexp_replace(lower(column_name), '\\s+', '') like '%device_type%'

#Query 2 (insert query 1 between *****)

WITH table_columns AS (
    -- Inserta la Query 1 aquí
    SELECT column_name, table_name, full_data_type 
    FROM (
        --***************************************************************************************************************************************************
SELECT column_name,concat(table_catalog,'.',table_schema,'.',table_name) as table_name,full_data_type FROM system.information_schema.columns WHERE

 regexp_replace(lower(column_name), '\\s+', '') like '%device_type%'
        --***************************************************************************************************************************************************
    )
),
numbered_pairs AS (
    SELECT 
        CONCAT(
            'SELECT ', CHAR(39), column_name, CHAR(39), ' AS column_name, ',
            CHAR(39), table_name, CHAR(39), ' AS table_name, ',
            CHAR(39), full_data_type, CHAR(39), ' AS full_data_type, ',
            'COUNT(DISTINCT ', column_name, ') AS distinct_count, ',
            'SLICE(COLLECT_SET(', column_name, '), 1, 5) AS select_distinct ',
            'FROM ', table_name
        ) AS query_line,
        'UNION ALL' AS union_clause,
        ROW_NUMBER() OVER (ORDER BY table_name) AS rn,
        COUNT(*) OVER () AS total_rows 
    FROM table_columns
),
mx AS (SELECT DISTINCT total_rows FROM numbered_pairs)
SELECT array_join(
    collect_list(
        CASE 
            WHEN rn <> (SELECT * FROM mx) THEN CONCAT(query_line, ' ', union_clause) 
            ELSE query_line 
        END
    ), '\n'
) AS full_query
FROM numbered_pairs;

#Query 3 - insert OUTPUT of query 2 between ***

with t1 as(--*********************************************************************************************************************************
SELECT 'IdBusinessVertical' AS column_name, 'us_data_science.ai_test.daily_billing' AS table_name, 'int' AS full_data_type, COUNT(DISTINCT IdBusinessVertical) AS distinct_count, SLICE(COLLECT_SET(IdBusinessVertical), 1, 5) AS select_distinct FROM us_data_science.ai_test.daily_billing UNION ALL SELECT 'vertical' AS column_name, 'us_data_science.cyndiz.meta_campaigns' AS table_name, 'string' AS full_data_type, COUNT(DISTINCT vertical) AS distinct_count, SLICE(COLLECT_SET(vertical), 1, 5) AS select_distinct FROM us_data_science.cyndiz.meta_campaigns
--*********************************************************************************************************************************
),t2 as (select column_name, table_name, full_data_type, CAST(distinct_count AS STRING) AS distinct_count from t1) select ARRAY_JOIN(COLLECT_LIST(column_name), ' | ') as column_name,ARRAY_JOIN(COLLECT_LIST(table_name), ' | ') as table_name,ARRAY_JOIN(COLLECT_LIST(full_data_type), ' | ') as full_data_type, ARRAY_JOIN(COLLECT_LIST(distinct_count), ' | ') as distinct_count from t2





