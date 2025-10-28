
pokemon_ids = [1, 2, 3]
stats = ["stat_1", "stat_2"]

new_stats = {(p_id, stat_name) for p_id in pokemon_ids for stat_name in stats}

print(new_stats)