"""
Crawl HCMC bus data from EBMS API.

Pipeline
--------
1. getallroute
2. getvarsbyroute/{route_id}
3. getpathsbyvar/{route_id}/{var_id}
4. getstopsbyvar/{route_id}/{var_id}

Outputs
-------
data/MCPT/
    routes_info.json
    route_paths.json
    route_stops.json
"""

import asyncio
import aiohttp
import json
import os
import time
import logging

# CONFIG

BASE_URL = "https://apicms.ebms.vn/businfo"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "MCPT")

OUTPUT_INFO = os.path.join(DATA_DIR, "routes_info.json")
OUTPUT_PATHS = os.path.join(DATA_DIR, "route_paths.json")
OUTPUT_STOPS = os.path.join(DATA_DIR, "route_stops.json")

MAX_CONCURRENT = 5
REQUEST_DELAY = 0.3
BATCH_SIZE = 10


# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# HTTP FETCH
async def fetch_json(session, url, semaphore):

    async with semaphore:

        for attempt in range(3):

            try:

                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "Mozilla/5.0",
                    },
                ) as resp:

                    if resp.status == 200:

                        text = await resp.text()

                        if not text.strip():
                            return None

                        return json.loads(text)

                    logger.warning("HTTP %s for %s", resp.status, url)

            except Exception as e:

                logger.warning("Error %s : %s", url, e)

            await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

        logger.error("Failed request: %s", url)

        return None


# FETCH ALL ROUTES
async def fetch_routes(session, semaphore):

    url = f"{BASE_URL}/getallroute"

    routes = await fetch_json(session, url, semaphore)

    if not routes:
        raise RuntimeError("Cannot fetch routes")

    logger.info("Fetched %d routes", len(routes))

    return routes


# CRAWL SINGLE ROUTE
async def crawl_route(session, route, semaphore, all_paths, all_stops):

    route_id = route.get("RouteId")
    route_no = route.get("RouteNo")

    vars_url = f"{BASE_URL}/getvarsbyroute/{route_id}"

    variants = await fetch_json(session, vars_url, semaphore)

    if not variants:

        logger.warning("No variants for route %s", route_no)

        return {**route, "Vars": []}

    variant_info = []

    async def process_variant(var):

        var_id = var.get("RouteVarId")

        if not var_id:
            return

        paths_url = f"{BASE_URL}/getpathsbyvar/{route_id}/{var_id}"
        stops_url = f"{BASE_URL}/getstopsbyvar/{route_id}/{var_id}"

        paths_data, stops_data = await asyncio.gather(
            fetch_json(session, paths_url, semaphore),
            fetch_json(session, stops_url, semaphore),
        )

        # -------- PATHS --------
        if isinstance(paths_data, dict):

            all_paths.append(
                {
                    "RouteId": route_id,
                    "RouteNo": route_no,
                    "RouteVarId": var_id,
                    "RouteVarName": var.get("RouteVarName"),
                    "Outbound": var.get("Outbound"),
                    "lat": paths_data.get("lat", []),
                    "lng": paths_data.get("lng", []),
                }
            )

        # -------- STOPS --------
        if stops_data:

            if not isinstance(stops_data, list):
                stops_data = [stops_data]

            for stop in stops_data:

                all_stops.append(
                    {
                        "RouteId": route_id,
                        "RouteNo": route_no,
                        "RouteVarId": var_id,
                        "RouteVarName": var.get("RouteVarName"),
                        "Outbound": var.get("Outbound"),
                        **stop,
                    }
                )

    await asyncio.gather(*[process_variant(v) for v in variants])

    # metadata for variants
    for var in variants:

        variant_info.append(
            {
                "RouteVarId": var.get("RouteVarId"),
                "RouteVarName": var.get("RouteVarName"),
                "RouteVarShortName": var.get("RouteVarShortName"),
                "Outbound": var.get("Outbound"),
                "Distance": var.get("Distance"),
                "RunningTime": var.get("RunningTime"),
                "StartStop": var.get("StartStop"),
                "EndStop": var.get("EndStop"),
            }
        )

    return {
        **route,
        "Vars": variant_info,
    }


# MAIN
async def main():

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    all_paths = []
    all_stops = []
    routes_info = []

    async with aiohttp.ClientSession() as session:

        routes = await fetch_routes(session, semaphore)

        total = len(routes)

        for i in range(0, total, BATCH_SIZE):

            batch = routes[i : i + BATCH_SIZE]

            results = await asyncio.gather(
                *[
                    crawl_route(session, r, semaphore, all_paths, all_stops)
                    for r in batch
                ]
            )

            routes_info.extend(results)

            done = min(i + BATCH_SIZE, total)

            logger.info("Progress %d/%d routes", done, total)

            await asyncio.sleep(REQUEST_DELAY)

    # -------- SAVE FILES --------

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(OUTPUT_INFO, "w", encoding="utf-8") as f:
        json.dump(routes_info, f, ensure_ascii=False, indent=2)

    with open(OUTPUT_PATHS, "w", encoding="utf-8") as f:
        json.dump(all_paths, f, ensure_ascii=False, indent=2)

    with open(OUTPUT_STOPS, "w", encoding="utf-8") as f:
        json.dump(all_stops, f, ensure_ascii=False, indent=2)

    logger.info("Saved routes_info.json (%d routes)", len(routes_info))
    logger.info("Saved route_paths.json (%d variants)", len(all_paths))
    logger.info("Saved route_stops.json (%d stops)", len(all_stops))


# RUN
if __name__ == "__main__":

    start = time.time()

    asyncio.run(main())

    elapsed = time.time() - start

    logger.info("Total crawl time %.2f seconds", elapsed)