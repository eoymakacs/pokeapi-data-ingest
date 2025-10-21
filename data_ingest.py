import asyncio
import aiohttp
import sqlite3
import utils
from datetime import datetime

_config = utils.get_config_for_pokeingest()
logger = utils.init_log(_config)


DB_FILE = _config['db']['db_file']

BASE_URL = _config['api']['base_url']
GENERATION_URL = f"{BASE_URL}/generation/{{}}"

CREATE_POKEMON_TABLE = utils.load_query("create_pokemon_table.sql")
POKEMON_INSERT = utils.load_query("pokemon_insert.sql")

CREATE_POKEMON_STATS_TABLE = utils.load_query("create_pokemon_stats_table.sql")
POKEMON_STATS_INSERT = utils.load_query("pokemon_stats_insert.sql")
POKEMON_STATS_SELECT = utils.load_query("pokemon_stats_select.sql")
POKEMON_STATS_DELETE = utils.load_query("delete_pokemon_stat.sql")

is_incremental = True

def init_db():
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        if not is_incremental:
            logger.info(f"is_incremental = {is_incremental} - Tables will be dropped and recreated.")
            c.execute(f"drop table if exists pokemon_stats")
            c.execute(f"drop table if exists pokemon")
            conn.commit()
        
        # Pokemon table
        c.execute(CREATE_POKEMON_TABLE)
        
        # Stats table
        c.execute(CREATE_POKEMON_STATS_TABLE)
        
        conn.commit()
        logger.info("Database has been initialized successfully.")
    except Exception as e:
        logger.error(f"Error happened while initializing the database: {e}", exc_info=True)
        if conn:
            conn.rollback()
            conn.close()
    finally:
        return conn

# Send a GET request to given API url to fetch JSON data.
async def get_data_from_api(session, url):
    try:
        async with session.get(url) as resp:
            if not resp.ok:
                logger.info(f"Request failed: {url} ---- ({resp.status})")
                return None
            return await resp.json()
    except Exception as e:
        logger.error(f"Exception occurred while requesting {url}: {e}", exc_info=True)
        return None
    
# Fetch all generations from the API.    
async def get_all_generations(session):
    try:
        base_generation_url = f"{BASE_URL}/generation"
        data = await get_data_from_api(session, base_generation_url)
        if not data or "results" not in data:
            return []
        return [{"name": g.get("name"), "url": g.get("url")} for g in data["results"]]
    except Exception as e:
        logger.error(f"Exception occurred while fetching generations: {base_generation_url}, {e}", exc_info=True)
        return []

# Fetch all Pokemon species URLs for a given generation.
async def get_species_for_generation(session, url):
    try:
        data = await get_data_from_api(session, url)
        if not data:
            return []
        return [s["url"] for s in data["pokemon_species"]]
    except Exception as e:
        logger.error(f"Exception occurred while getting species data for generation {url}: {e}", exc_info=True)
        return []

# Fetch the URL of the default Pokemon for a given species.
async def get_default_pokemon_url(session, species_url):
    try:
        species_data = await get_data_from_api(session, species_url)
        if not species_data:
            return None
        
        for variety in species_data.get("varieties", []):
            if variety.get("is_default"):
                return variety.get("pokemon", {}).get("url")
        return None
    except Exception as e:
        logger.error(f"Exception occurred while getting default Pokemon URL for species {species_url}: {e}", exc_info=True)
        return None

# Get Pokemon details from the given URL.
async def get_pokemon_details(session, url):
    try:
        data = await get_data_from_api(session, url)
        if not data:
            return None

        stats = {s.get("stat", {}).get("name"): s.get("base_stat") for s in data.get("stats", [])}
        types = [t.get("type", {}).get("name") for t in data.get("types", [])]

        # Ensure we have valid data
        if not data.get("id") or not data.get("name"):
            return None

        return {
            "id": data["id"],
            "name": data["name"],
            "types": "/".join(filter(None, types)),
            "stats": {k: v for k, v in stats.items() if k is not None and v is not None}
        }

    except Exception as e:
        logger.error(f"Exception occurred while fetching Pokemon details from {url}: {e}", exc_info=True)
        return None


def sync_pokemon_data(conn, pokemons):
    try:
        c = conn.cursor()
        pokemon_tuples = [(p["id"], p["name"], p["types"]) for p in pokemons]
        c.executemany(POKEMON_INSERT, pokemon_tuples)
        
        
        pokemon_ids = [p["id"] for p in pokemons]
        if not pokemon_ids:
            return
        min_id = min(p["id"] for p in pokemons)
        max_id = max(p["id"] for p in pokemons)

        # Get existing stats for target pokemons
        query = POKEMON_STATS_SELECT.format(','.join('?'*len(pokemon_ids)))
        c.execute(query, pokemon_ids)
        existing_stats = {(row[0], row[1]) for row in c.fetchall()}

        # Build new stat rows
        new_stats = {(p["id"], stat_name) for p in pokemons for stat_name in p["stats"].keys()}

        # Run this block only if we are running an incremental data refresh.
        if is_incremental:
            # Find deleted stat rows
            to_delete = existing_stats - new_stats

            # Delete all missing stats at once
            if to_delete:
                c.executemany(POKEMON_STATS_DELETE, list(to_delete))
                logger.info(f"{len(to_delete)} stats data deleted for pokemon id range: [{min_id}-{max_id}]")
            else:
                logger.info(f"No stats data deleted for pokemon id range: [{min_id}-{max_id}]")

        # Insert or replace new stat rows
        stat_tuples = [
            (p["id"], stat_name, value)
            for p in pokemons
            for stat_name, value in p["stats"].items()
        ]
        c.executemany(POKEMON_STATS_INSERT, stat_tuples)
        conn.commit()
        return len(pokemon_tuples), len(stat_tuples)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error syncing pokemon stats batch: {e}", exc_info=True)
        return 0, 0

async def main():
    conn = None
    try:
        # Initialize database
        conn = init_db()
        if not conn:
            logger.error("DB initialization failed. Process is being stopped.")
            return

        async with aiohttp.ClientSession() as session:
            # 1. Get all generation URLs
            generations = await get_all_generations(session)
            logger.info(f"Found {len(generations)} generations")

            # Only process Generation I for now
            target_gens = [g for g in generations if g.get("name") in ["generation-i"]]

            for gen in target_gens:
                logger.info(f"\n=== Started ingesting data for {gen['name']} ===")
                total_pokemon_rows = 0
                total_stats_rows = 0

                # 2. Get all species URLs for this generation
                species_urls = await get_species_for_generation(session, gen['url'])
                logger.info(f"{len(species_urls)} species found in {gen['name']}")

                # 3. Fetch default Pokemon URLs
                default_tasks = [get_default_pokemon_url(session, url) for url in species_urls]
                default_results = await asyncio.gather(*default_tasks, return_exceptions=True)

                pokemon_urls = []
                for r in default_results:
                    if isinstance(r, Exception):
                        logger.info(f"Error fetching default Pokemon URL: {r}")
                        #traceback.print_exception(type(r), r, r.__traceback__)
                        logger.error("Exception occurred in asyncio task", exc_info=(type(r), r, r.__traceback__))
                    elif r:
                        pokemon_urls.append(r)

                logger.info(f"Fetched {len(pokemon_urls)} default Pokemon URLs successfully.")

                # 4. Fetch Pokemon details
                detail_tasks = [get_pokemon_details(session, url) for url in pokemon_urls]
                detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

                pokemons = []
                for r in detail_results:
                    if isinstance(r, Exception):
                        logger.info(f"Error fetching Pokemon details: {r}")
                        logger.error("Exception occurred in asyncio task", exc_info=(type(r), r, r.__traceback__))
                    elif r:
                        pokemons.append(r)

                logger.info(f"Fetched {len(pokemons)} Pokemon details successfully.")

                # 5: Save data into DB in batches
                for pokemon_rows_batch in utils.batch(pokemons, 250):    
                    pokemon_count, stats_count = sync_pokemon_data(conn, pokemon_rows_batch)
                    total_pokemon_rows += pokemon_count
                    total_stats_rows += stats_count

                logger.info(f"\nData Summary for {gen['name']}:")
                logger.info(f"Total Pokemon rows inserted/updated: {total_pokemon_rows}")
                logger.info(f"Total Pokemon stats rows inserted/updated: {total_stats_rows}")

        logger.info(f"\nData ingestion complete. Database file: {DB_FILE}")

    except Exception as e:
        logger.error(f"Fatal error in main(): {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    asyncio.run(main())
