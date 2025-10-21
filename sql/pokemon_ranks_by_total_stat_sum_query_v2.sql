-- This query splits Pokemons with multiple types into individual types,
-- computes each Pokemon's total stats, and ranks them within each type.
WITH total_stats AS (
    SELECT 
        p.id, 
        p.name, 
        p.types, 
        SUM(ps.value) AS total_stat_sum
    FROM pokemon p
    JOIN pokemon_stats ps ON p.id = ps.pokemon_id
    GROUP BY p.id, p.types
),
split_types AS (
    SELECT 
        id AS pokemon_id,
        name AS pokemon_name,
        total_stat_sum,
        types || '/' AS rest,
        NULL AS type
    FROM total_stats
    UNION ALL
    SELECT
        pokemon_id,
        pokemon_name,
        total_stat_sum,
        SUBSTR(rest, INSTR(rest, '/') + 1),
        LOWER(TRIM(SUBSTR(rest, 1, INSTR(rest, '/') - 1)))
    FROM split_types
    WHERE rest <> ''
)
SELECT
    pokemon_id,
    pokemon_name,
    type AS type_name,
    total_stat_sum,
    RANK() OVER (PARTITION BY type ORDER BY total_stat_sum DESC) AS rank_within_type
FROM split_types
WHERE type IS NOT NULL AND type <> ''
ORDER BY type_name, rank_within_type
