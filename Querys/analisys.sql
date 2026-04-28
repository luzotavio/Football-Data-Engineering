-- top 5 stadiumns by capacity

select top 5
    stadium,
    capacity
from stadiums
order by capacity desc


-- average capacity of the stadiumns by region
SELECT
    region,
    AVG(capacity) as avg_capacity
FROM
    stadiums
GROUP BY
    region
ORDER BY
    AVG(capacity) DESC

-- count the stadiums in each country
SELECT
    country,
    COUNT(stadium) as number_of_stadiums
FROM
    stadiums
GROUP BY
    country
ORDER BY
    number_of_stadiums

-- stadium ranking with reach region
SELECT * from (
SELECT
    RANK() OVER (PARTITION BY region ORDER BY capacity DESC) as capacity_rank,
    stadium,
    capacity,
    region
FROM stadiums
GROUP BY region, capacity,stadium
) t
WHERE capacity_rank <= 3

-- stadiums with capacity above the average

SELECT * FROM (
    SELECT
        stadium,
        region,
        capacity,
        AVG(capacity) OVER(PARTITION BY region) as avg_region_capacity
    FROM stadiums
) t
WHERE capacity > avg_region_capacity;


-- stadiumns with closes capacity to regional median
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
ORDER BY distance_to_median




