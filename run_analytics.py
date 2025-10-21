import sqlite3
import traceback
from datetime import datetime
import utils
import logging

_config = utils.get_config_for_pokeingest()
logger = utils.init_log(_config)

DB_FILE = _config['db']['db_file']

# Analytics Queries
POKEMON_RANKS_BY_TOTAL_STAT_SUM_QUERY = utils.load_query("pokemon_ranks_by_total_stat_sum_query.sql")
POKEMON_COUNT_PER_TYPE_QUERY = utils.load_query("pokemon_count_per_type_query.sql")

POKEMON_TANKY_CLASSIFICATION_QUERY = utils.load_query("pokemon_tanky_classification_query.sql")
MOST_TANKY_POKEMON_QUERY = utils.load_query("most_tanky_pokemon_query.sql") 

# Queries to answer
def run_analytics_queries():
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        logger.info("Running analytics queries...")
        # Query 1
        logger.info("\nQuery 1: Find the count of all distinct counts of Pokemon types (ex: water:14, grass/poison: 5)")
        start_time = datetime.now()
        c.execute(POKEMON_COUNT_PER_TYPE_QUERY)
        rows = c.fetchall()
        logger.info(f"Query 1 completed. Rows: {len(rows)} | Duration: {(datetime.now() - start_time).seconds}s")

        # Query 2
        logger.info("\nQuery 2: Rank Pokemon by total stat sum per type")
        start_time = datetime.now()
        c.execute(POKEMON_RANKS_BY_TOTAL_STAT_SUM_QUERY)
        rows = c.fetchall()
        logger.info(f"Query 2 completed. Rows: {len(rows)} | Duration: {(datetime.now() - start_time).seconds}s")

        # Query 3
        logger.info("\nQuery 3: Label Pokemon as 'tanky' or 'not tanky'")
        start_time = datetime.now()
        c.execute(POKEMON_TANKY_CLASSIFICATION_QUERY)
        rows = c.fetchall()
        logger.info(f"Query 3 completed. Rows: {len(rows)} | Duration: {(datetime.now() - start_time).seconds}s")

        # Query 4
        logger.info("\nQuery 4: Which Pokemon type(s) have the most tanky Pokemon?")
        start_time = datetime.now()
        c.execute(MOST_TANKY_POKEMON_QUERY)
        rows = c.fetchall()
        duration = (datetime.now() - start_time).seconds
        logger.info(f"Query 4 completed. Rows: {len(rows)} | Duration: {duration}s")

        for row in rows:
            logger.info(row)

    except sqlite3.Error as e:
        logger.error(f"SQLite error occurred: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Unexpected error occurred while running analytics queries: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()

run_analytics_queries()