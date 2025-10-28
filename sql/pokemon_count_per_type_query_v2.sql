-- This query handles Pokemon with multiple types (e.g., "Fire/Water").
-- It splits the types into separate rows and computes the count of Pokemon per type.
WITH RECURSIVE split_types(pokemon_id, rest, type) AS (
    SELECT 
        id,
        types || '/',  
        NULL
    FROM pokemon
    UNION ALL
    SELECT
        pokemon_id,
        SUBSTR(rest, INSTR(rest, '/') + 1), 
        LOWER(TRIM(SUBSTR(rest, 1, INSTR(rest, '/') - 1)))
    FROM split_types
    WHERE rest <> ''
)
SELECT 
    type AS type_name,
    COUNT(DISTINCT pokemon_id) AS count_per_type
FROM split_types
WHERE type IS NOT NULL AND type <> ''
GROUP BY type
ORDER BY count_per_type DESC

--Simplified
SELECT 
    LOWER(TRIM(type)) AS type_name,
    COUNT(DISTINCT p.id) AS count_per_type
FROM pokemon p
CROSS JOIN LATERAL unnest(string_to_array(p.types, '/')) AS t(type)
WHERE type IS NOT NULL AND type <> ''
GROUP BY LOWER(TRIM(type))
ORDER BY count_per_type DESC;

--Postgres
WITH RECURSIVE split_types(pokemon_id, rest, type) AS (
    -- Anchor part: start with full "types" string and no type extracted yet
    SELECT 
        id AS pokemon_id,
        types || '/' AS rest,  
        NULL::text AS type
    FROM pokemon

    UNION ALL

    -- Recursive part: extract the first token before '/' and process the remaining string
    SELECT
        pokemon_id,
        SUBSTRING(rest FROM POSITION('/' IN rest) + 1) AS rest,
        LOWER(TRIM(SUBSTRING(rest FROM 1 FOR POSITION('/' IN rest) - 1))) AS type
    FROM split_types
    WHERE rest <> ''
)
SELECT 
    type AS type_name,
    COUNT(DISTINCT pokemon_id) AS count_per_type
FROM split_types
WHERE type IS NOT NULL AND type <> ''
GROUP BY type
ORDER BY count_per_type DESC;
