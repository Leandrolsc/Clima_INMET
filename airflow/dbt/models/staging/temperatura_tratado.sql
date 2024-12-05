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
FROM {{ source('INMET', 'Temperatura_historico') }} 
WHERE "Temperatura" is not null
