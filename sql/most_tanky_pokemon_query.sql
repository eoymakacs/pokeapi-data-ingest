WITH tanky_labels AS (
        SELECT p.id, p.name, p.types,
            CASE 
                WHEN MAX(CASE WHEN ps.stat_name IN ('hp', 'defense') THEN ps.value ELSE 0 END) = MAX(ps.value)
                THEN 'Y'
                ELSE 'N'
            END AS tanky_label
            FROM pokemon p
            JOIN pokemon_stats ps ON p.id = ps.pokemon_id
            GROUP BY p.id, p.name, p.types
    ),
    tanky_counts AS (
        SELECT types, COUNT(*) AS tanky_count
            FROM tanky_labels WHERE tanky_label = 'Y' GROUP BY types
            )
    SELECT * FROM tanky_counts WHERE tanky_count = (SELECT MAX(tanky_count) FROM tanky_counts)