SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKCalendario"
            ,"Data"
FROM {{ ref("temperatura_tratado") }}
