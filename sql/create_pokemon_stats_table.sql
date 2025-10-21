CREATE TABLE IF NOT EXISTS pokemon_stats (
                                    pokemon_id INTEGER,
                                    stat_name TEXT,
                                    value INTEGER,
                                    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id),
                                    PRIMARY KEY (pokemon_id, stat_name)
                                )