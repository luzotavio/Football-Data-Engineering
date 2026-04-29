/*
====================================================================================================
ANÁLISE DE DADOS - AZURE SYNAPSE ANALYTICS
====================================================================================================
Estas análises foram realizadas no Azure Synapse Analytics, utilizando o SQL Serverless.
O processamento ocorre diretamente sobre os dados armazenados no Azure Data Lake Storage Gen2 (ADLS Gen2),
após o fluxo completo da pipeline (Airflow + Python) extrair, processar e carregar os dados.
====================================================================================================
*/

-- 1. Identificação dos 5 estádios com a maior capacidade de público global.
SELECT TOP 5
    stadium,
    capacity
FROM stadiums
ORDER BY capacity DESC;


-- 2. Cálculo da média de capacidade dos estádios agrupada por região geográfica.
-- Útil para entender o porte médio das infraestruturas esportivas em diferentes continentes.
SELECT
    region,
    AVG(capacity) as avg_capacity
FROM
    stadiums
GROUP BY
    region
ORDER BY
    avg_capacity DESC;


-- 3. Contagem total de estádios mapeados em cada país.
-- Permite identificar quais nações possuem maior volume de estádios de grande porte registrados.
SELECT
    country,
    COUNT(stadium) as number_of_stadiums
FROM
    stadiums
GROUP BY
    country
ORDER BY
    number_of_stadiums DESC;


-- 4. Ranking dos 3 maiores estádios dentro de cada região.
-- Utiliza funções de janela (RANK) para isolar o "Top 3" de cada continente/região de forma independente.
SELECT * FROM (
    SELECT
        RANK() OVER (PARTITION BY region ORDER BY capacity DESC) as capacity_rank,
        stadium,
        capacity,
        region
    FROM stadiums
    GROUP BY region, capacity, stadium
) t
WHERE capacity_rank <= 3;


-- 5. Identificação de estádios que possuem capacidade acima da média da sua própria região.
-- Utiliza Window Functions para comparar o valor individual com a média calculada dinamicamente por partição.
SELECT * FROM (
    SELECT
        stadium,
        region,
        capacity,
        AVG(capacity) OVER(PARTITION BY region) as avg_region_capacity
    FROM stadiums
) t
WHERE capacity > avg_region_capacity;


-- 6. Localização de estádios com capacidade mais próxima da mediana regional.
-- Utiliza CTEs (Common Table Expressions) para primeiro calcular a mediana regional via PERCENTILE_CONT
-- e depois encontrar a diferença absoluta (distância) para identificar o "estádio típico" da região.
WITH MedianCalculation AS (
    SELECT
        stadium,
        region,
        capacity,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity)
            OVER (PARTITION BY region) AS regional_median
    FROM stadiums
),
DistanceCalculation AS (
    SELECT
        *,
        ABS(capacity - regional_median) AS distance_to_median
    FROM MedianCalculation
)
SELECT * FROM DistanceCalculation
ORDER BY distance_to_median;
