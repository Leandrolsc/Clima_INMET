SELECT DISTINCT
             "Data"
             , "Tempo"
             , "Temperatura"
             , "Regiao"
             , "UF"
             , "Estacao"
             , "Latitude"
             , "Longitude"
             , "Altitude"
FROM "INMET"."public"."Temperatura_historico" 
WHERE "Temperatura" is not null