WITH total_stats AS (
    SELECT p.id, p.name, p.types, SUM(ps.value) AS total_stat_sum
        FROM pokemon p JOIN pokemon_stats ps ON p.id = ps.pokemon_id
        GROUP BY p.id, p.types
    )
    SELECT id, name, types, total_stat_sum, 
        RANK() OVER (PARTITION BY types ORDER BY total_stat_sum DESC) AS rank_within_type
        FROM total_stats
        ORDER BY types, rank_within_type
