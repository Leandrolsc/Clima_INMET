SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKTempo"
            ,"Tempo"
FROM "INMET"."public"."temperatura_tratado"