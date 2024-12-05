SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKRegiao"
            ,"UF"
            ,"Regiao"
FROM {{ ref("temperatura_tratado") }}
