SELECT p.id as pokemon_id, p.name as pokemon_name, p.types,
        CASE 
            WHEN MAX(CASE WHEN ps.stat_name IN ('hp', 'defense') THEN ps.value ELSE 0 END) = MAX(ps.value) 
                THEN 'tanky'
                ELSE 'not_tanky'
        END AS is_tanky
    FROM pokemon p 
    JOIN pokemon_stats ps ON p.id = ps.pokemon_id
    GROUP BY p.id, p.name, p.types