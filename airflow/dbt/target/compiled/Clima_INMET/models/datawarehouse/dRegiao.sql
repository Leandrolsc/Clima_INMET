SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKRegiao"
            ,"UF"
            ,"Regiao"
FROM "INMET"."public"."temperatura_tratado"