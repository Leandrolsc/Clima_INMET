SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKEstacao"
            ,"Estacao"
            ,"Latitude"
            ,"Longitude"
            ,"Altitude"
FROM {{ ref("temperatura_tratado") }}