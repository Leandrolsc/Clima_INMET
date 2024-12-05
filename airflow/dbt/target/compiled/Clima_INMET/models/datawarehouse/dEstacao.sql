SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKEstacao"
            ,"Estacao"
            ,"Latitude"
            ,"Longitude"
            ,"Altitude"
FROM "INMET"."public"."temperatura_tratado"