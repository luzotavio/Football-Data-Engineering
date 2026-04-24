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