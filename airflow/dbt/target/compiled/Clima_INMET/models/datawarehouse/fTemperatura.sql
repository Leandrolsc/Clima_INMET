SELECT DISTINCT
             dCalendario."SKCalendario"
             ,dTempo."SKTempo"
             ,dEstacao."SKEstacao"
             ,dRegiao."SKRegiao"          
            ,Temperatura
FROM "INMET"."public"."temperatura_tratado" fato
INNER JOIN "INMET"."public"."dCalendario" dCalendario
    ON dCalendario."Data" = fato."Data"
INNER JOIN "INMET"."public"."dEstacao" dEstacao
    ON dEstacao."Estacao"  =  fato."Estacao" 
    AND dEstacao."Latitude" = fato."Latitude" 
    AND dEStacao."Longitude" = fato."Longitude"
    AND dEstacao."Altitude" = fato. "Altitude"
INNER JOIN "INMET"."public"."dRegiao" dRegiao
    ON dRegiao."Regiao" = fato."Regiao"
    AND dRegiao."UF" = fato."UF"
INNER JOIN "INMET"."public"."dTempo" dTempo
    ON dTempo."Tempo" = fato."Tempo"