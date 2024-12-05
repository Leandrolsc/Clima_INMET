
  
    

  create  table "INMET"."public"."dRegiao__dbt_tmp"
  
  
    as
  
  (
    SELECT DISTINCT
             ROW_NUMBER() OVER (ORDER BY NULL) as "SKRegiao"
            ,"Regiao"
FROM "INMET"."public"."Temperatura_historico"
  );
  