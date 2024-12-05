SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKCalendario"
            ,"Data"
FROM "INMET"."public"."temperatura_tratado"