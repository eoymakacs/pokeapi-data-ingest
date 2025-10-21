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
