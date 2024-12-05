SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKTempo"
            ,"Tempo"
FROM {{ ref("temperatura_tratado") }}
