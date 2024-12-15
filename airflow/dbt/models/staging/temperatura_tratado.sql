{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_data ON public.temperatura_tratado (\"Data\");",
        "CREATE INDEX IF NOT EXISTS idx_tempo ON public.temperatura_tratado (\"Tempo\");",
        "CREATE INDEX IF NOT EXISTS idx_regiao_uf ON public.temperatura_tratado (\"Regiao\", \"UF\");",
        "CREATE INDEX IF NOT EXISTS idx_estacao_geo ON public.temperatura_tratado (\"Estacao\", \"Latitude\", \"Longitude\", \"Altitude\");"
    ]
) }}

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
