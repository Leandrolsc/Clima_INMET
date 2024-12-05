SELECT DISTINCT
             dCalendario."SKCalendario"
             ,dTempo."SKTempo"
             ,dEstacao."SKEstacao"
             ,dRegiao."SKRegiao"          
            ,Temperatura
FROM {{ ref("temperatura_tratado") }} fato
INNER JOIN {{ ref("dCalendario") }} dCalendario
    ON dCalendario."Data" = fato."Data"
INNER JOIN {{ ref("dEstacao") }} dEstacao
    ON dEstacao."Estacao"  =  fato."Estacao" 
    AND dEstacao."Latitude" = fato."Latitude" 
    AND dEStacao."Longitude" = fato."Longitude"
    AND dEstacao."Altitude" = fato. "Altitude"
INNER JOIN {{ ref("dRegiao") }} dRegiao
    ON dRegiao."Regiao" = fato."Regiao"
    AND dRegiao."UF" = fato."UF"
INNER JOIN {{ ref("dTempo") }} dTempo
    ON dTempo."Tempo" = fato."Tempo"