"""
Crawl bus route data (vars, paths, stops) from xe-buyt.com API.

For each route in data/MCPT/routes.json, fetches:
  1. Variants (getvarsbyroute)
  2. Paths per variant (getpathsbyvar)
  3. Stops per variant (getstopsbyvar)

Output files in data/MCPT/:
  - routes_info.json  : route metadata + variant info
  - route_paths.json  : GPS lat/lng paths per variant
  - route_stops.json  : bus stops per variant
"""

import asyncio
import json
import os
import time
import logging

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.xe-buyt.com/businfo"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "MCPT")
ROUTES_FILE = os.path.join(DATA_DIR, "routes.json")
OUTPUT_INFO = os.path.join(DATA_DIR, "routes_info.json")
OUTPUT_PATHS = os.path.join(DATA_DIR, "route_paths.json")
OUTPUT_STOPS = os.path.join(DATA_DIR, "route_stops.json")

# Rate limiting
MAX_CONCURRENT = 5
REQUEST_DELAY = 0.3  # seconds between requests per semaphore slot


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore):
    """Fetch JSON from a URL with rate limiting and retry logic."""
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
                        try:
                            return json.loads(text)
                        except json.JSONDecodeError:
                            logger.warning("JSON decode error for %s, attempt %d", url, attempt + 1)
                    else:
                        logger.warning("HTTP %d for %s, attempt %d", resp.status, url, attempt + 1)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning("Request error for %s: %s, attempt %d", url, e, attempt + 1)

            await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

        logger.error("Failed after 3 attempts: %s", url)
        return None


async def crawl_route(
    session: aiohttp.ClientSession,
    route: dict,
    semaphore: asyncio.Semaphore,
    all_paths: list,
    all_stops: list,
):
    """Crawl vars, paths, stops for a single route. Appends paths/stops to shared lists."""
    route_id = route["RouteId"]
    route_no = route["RouteNo"]

    # 1. Get variants
    vars_url = f"{BASE_URL}/getvarsbyroute/{route_id}_1"
    variants = await fetch_json(session, vars_url, semaphore)

    if not variants or not isinstance(variants, list):
        logger.warning("No variants for route %s (id=%s)", route_no, route_id)
        return {
            **route,
            "Vars": [],
        }

    # 2. For each variant, fetch paths and stops
    var_info_list = []

    async def process_variant(var: dict):
        var_id = var.get("RouteVarId")
        if not var_id:
            return

        paths_url = f"{BASE_URL}/getpathsbyvar/{route_id}_1/{var_id}"
        stops_url = f"{BASE_URL}/getstopsbyvar/{route_id}_1/{var_id}"

        paths_data, stops_data = await asyncio.gather(
            fetch_json(session, paths_url, semaphore),
            fetch_json(session, stops_url, semaphore),
        )

        # Collect paths
        if paths_data:
            all_paths.append({
                "RouteId": route_id,
                "RouteNo": route_no,
                "RouteVarId": var_id,
                "RouteVarName": var.get("RouteVarName", ""),
                "Outbound": var.get("Outbound", ""),
                "lat": paths_data.get("lat", []) if isinstance(paths_data, dict) else [],
                "lng": paths_data.get("lng", []) if isinstance(paths_data, dict) else [],
            })

        # Collect stops
        if stops_data:
            if not isinstance(stops_data, list):
                stops_data = [stops_data]
            for stop in stops_data:
                all_stops.append({
                    "RouteId": route_id,
                    "RouteNo": route_no,
                    "RouteVarId": var_id,
                    "RouteVarName": var.get("RouteVarName", ""),
                    "Outbound": var.get("Outbound", ""),
                    **stop,
                })

    await asyncio.gather(*[process_variant(v) for v in variants])

    # Build variant info (without paths/stops data)
    for var in variants:
        var_info_list.append({
            "RouteVarId": var.get("RouteVarId"),
            "RouteVarName": var.get("RouteVarName"),
            "RouteVarShortName": var.get("RouteVarShortName"),
            "Outbound": var.get("Outbound"),
            "Distance": var.get("Distance"),
            "RunningTime": var.get("RunningTime"),
            "StartStop": var.get("StartStop"),
            "EndStop": var.get("EndStop"),
        })

    return {
        **route,
        "Vars": var_info_list,
    }


async def main():
    with open(ROUTES_FILE, "r", encoding="utf-8") as f:
        routes = json.load(f)

    logger.info("Loaded %d routes from %s", len(routes), ROUTES_FILE)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_paths = []
    all_stops = []
    routes_info = []

    async with aiohttp.ClientSession() as session:
        batch_size = 10
        for i in range(0, len(routes), batch_size):
            batch = routes[i : i + batch_size]
            batch_results = await asyncio.gather(
                *[crawl_route(session, route, semaphore, all_paths, all_stops) for route in batch]
            )
            routes_info.extend(batch_results)

            done = min(i + batch_size, len(routes))
            logger.info("Progress: %d/%d routes crawled", done, len(routes))
            await asyncio.sleep(REQUEST_DELAY)

    # Save routes_info.json
    with open(OUTPUT_INFO, "w", encoding="utf-8") as f:
        json.dump(routes_info, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d routes to %s", len(routes_info), OUTPUT_INFO)

    # Save route_paths.json
    with open(OUTPUT_PATHS, "w", encoding="utf-8") as f:
        json.dump(all_paths, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d variant paths to %s", len(all_paths), OUTPUT_PATHS)

    # Save route_stops.json
    with open(OUTPUT_STOPS, "w", encoding="utf-8") as f:
        json.dump(all_stops, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d stop entries to %s", len(all_stops), OUTPUT_STOPS)


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    elapsed = time.time() - start
    logger.info("Total time: %.1f seconds", elapsed)
