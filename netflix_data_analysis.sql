-- Remove Duplicates
SELECT show_id, COUNT(*) 
FROM netflix_raw
GROUP BY show_id
HAVING COUNT(*) > 1


SELECT * FROM netflix_raw
WHERE CONCAT(UPPER(title),type) IN (SELECT CONCAT(UPPER(title), type)
FROM netflix_raw
GROUP BY UPPER(title), type
HAVING COUNT(*) > 1)
ORDER BY title

-- This query create the new cleaned data table
WITH cte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY title,type ORDER BY show_id) AS rn
FROM netflix_raw)
SELECT show_id, type, title, CAST(date_added AS date) AS date_added, 
release_year,rating, CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration, description
INTO netflix
FROM cte
WHERE rn = 1

SELECT * FROM netflix


-- New Table for listed in, director, country, cast
-- The CROSS APPLY operator returns only those rows from the left table expression (in its final output) 
-- if it matches with the right table expression.
-- Directors Table
SELECT show_id, TRIM(value) AS director
INTO netflix_directors
FROM netflix_raw
CROSS APPLY STRING_SPLIT(director, ',')

-- Listed In Table
SELECT show_id, TRIM(value) AS genre
INTO netflix_genre
FROM netflix_raw
CROSS APPLY STRING_SPLIT(listed_in, ',')

-- Country Table
SELECT show_id, TRIM(value) AS country
INTO netflix_country
FROM netflix_raw
CROSS APPLY STRING_SPLIT(country, ',')

-- Cast Table
SELECT show_id, TRIM(value) AS cast
INTO netflix_cast
FROM netflix_raw
CROSS APPLY STRING_SPLIT(cast, ',')

-- Populate missing values in country
-- This query maps the countries that directors have in other films to fill the Null value.
INSERT INTO netflix_country
SELECT show_id, m.country 
FROM netflix_raw nr
INNER JOIN(
SELECT director, country
FROM netflix_country nc
INNER JOIN netflix_directors nd
ON nc.show_id = nd.show_id
GROUP BY director, country
) m 
ON nr.director = m.director
WHERE nr.country IS NULL

-- Netflix Data Analysis

/* 1. For each director count the number of movies and tv shows creaed by them in separate columns for directors
who have created Tv shows and movies both*/
SELECT nd.director,
COUNT(DISTINCT CASE WHEN n.type='Movie' THEN n.show_id END) AS no_of_movies,
COUNT(DISTINCT CASE WHEN n.type='Tv Show' THEN n.show_id END) AS no_of_tv_show,
COUNT(DISTINCT(n.type)) AS distinct_type
FROM netflix n
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT(n.type)) = 2

-- 2. Which country has highest number of comedy movies
SELECT TOP 1 nc.country, COUNT(DISTINCT ng.show_id) AS number_of_comedy_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc
ON ng.show_id = nc.show_id
INNER JOIN netflix n
ON ng.show_id = nc.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY number_of_comedy_movies DESC

-- 3. For each year (as per date added to netflix), which director has maximun number of movies released
WITH cte AS(
SELECT nd.director, YEAR(n.date_added) AS date_year, COUNT(DISTINCT n.show_id) AS number_of_movies
FROM netflix n
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id
WHERE n.type = 'Movie'
GROUP BY nd.director, YEAR(n.date_added)
),
cte2 AS(
SELECT *, ROW_NUMBER() OVER(PARTITION BY date_year ORDER BY number_of_movies DESC, director) AS rn
FROM cte
)
SELECT * FROM cte2 WHERE rn=1
ORDER BY number_of_movies DESC

-- 4. What is average duration of movies in each genre
SELECT ng.genre, AVG(CAST(REPLACE(duration, 'min','') AS int)) AS avg_duration
FROM netflix n
INNER JOIN netflix_genre ng
ON n.show_id = ng.show_id
WHERE type = 'Movie'
GROUP BY ng.genre

/* 5. Find the list of directors who have created horror and comedy movies both,
display director names along with number of comedy and horrow movies directed by them*/
SELECT nd.director,
COUNT(DISTINCT CASE WHEN ng.genre = 'Comedies' THEN n.show_id END) AS number_of_comedy_movies,
COUNT(DISTINCT CASE WHEN ng.genre = 'Horror Movies' THEN n.show_id END) AS number_of_horror_movies
FROM netflix n
INNER JOIN netflix_genre ng
ON n.show_id = ng.show_id
INNER JOIN netflix_directors nd
ON n.show_id = nd.show_id
WHERE n.type='Movie' AND ng.genre IN ('Comedies','Horror Movies')
GROUP BY nd.director
HAVING COUNT(DISTINCT ng.genre) = 2