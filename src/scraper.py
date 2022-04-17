import asyncio
import copy
import logging
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import List, Optional

import aiohttp

import config
from helpers.Api import botDetectorApi
from helpers.Scraper import Scraper

logger = logging.getLogger(__name__)


@dataclass()
class Job:
    name: str
    data: Optional[List[dict]] = None


# global variables
jobs = deque([Job("get_players_to_scrape")])
results = []


async def get_proxy_list() -> List:
    """
    returns the proxy list from webshare.io
    output format: ['http://user:pass@ip:port', 'http://user:pass@ip:port', ...]
    """
    logger.info("fetching proxy list from webshare.io")
    async with aiohttp.ClientSession() as session:
        async with session.get(config.PROXY_DOWNLOAD_URL) as response:
            if response.status != 200:
                logger.error(f"response status {response.status}")
                logger.error(f"response body: {await response.text()}")
                raise Exception("error fetching proxy list")

            proxies = str(await response.text())
            proxies = proxies.splitlines()
            proxies = [proxy.split(":") for proxy in proxies]
            proxies = [
                f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}"
                for proxy in proxies
            ]
            logger.info(f"fetched {len(proxies)} proxies")
    return proxies


async def create_worker(proxy):
    """
    This function is responsible for creating a worker that will process jobs.

    The worker will process jobs in the following order:
    1. get_players_to_scrape
    2. process_hiscore
    3. post_scraped_players

    The worker will repeat the process until there are no more jobs to process.
    """
    global results

    api = botDetectorApi(config.ENDPOINT, config.QUERY_SIZE, config.TOKEN)
    scraper = Scraper(proxy)
    while True:
        if len(jobs) == 0:
            await asyncio.sleep(1)
            continue

        # take the first job
        job = jobs.popleft()

        if job.name == "get_players_to_scrape":
            # get_players_to_scrape
            players = await api.get_players_to_scrape()
            # for each player create a job to process the hiscore
            [jobs.append(Job("process_hiscore", [player])) for player in players]
            # add a job to post the scraped data to the api
            jobs.append(Job("post_scraped_players"))
            # add a job midway through the process hiscore jobs to get players to scrape
            if len(jobs) < 2 * int(config.QUERY_SIZE):
                jobs.insert(int(len(jobs) / 2), Job("get_players_to_scrape"))
            logger.debug(f"Length of jobs: {len(jobs)}")
        elif job.name == "post_scraped_players":
            # copy the results
            job.data = copy.deepcopy(results)
            results = []
            # posting data to api
            await api.post_scraped_players(job.data)
            # add a job to get players to scrape
            jobs.append(Job("get_players_to_scrape"))
        elif job.name == "process_hiscore" and job.data:
            player = job.data[0]
            hiscore = await scraper.lookup_hiscores(player)
            # data validation
            if hiscore is None:
                logger.warning(f"Hiscore is empty for {player.get('name')}")
                continue
            # player is not on the hiscores
            if "error" in hiscore:
                # update additional metadata
                player["possible_ban"] = 1
                player["confirmed_player"] = 0

                output = {}
                output["player"] = await scraper.lookup_runemetrics(player)
                output["hiscores"] = None
            else:
                # update additional metadata
                player["possible_ban"] = 0
                player["confirmed_ban"] = 0
                player["label_jagex"] = 0
                player["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

                output = {}
                output["player"] = player
                output["hiscores"] = hiscore
            results.append(output.copy())


async def main():
    """
    This function is the main function of the program.
    It creates a list of proxies and then creates a worker for each proxy.
    """
    proxies = await get_proxy_list()
    workers = [asyncio.create_task(create_worker(proxy)) for proxy in proxies]
    await asyncio.gather(*workers)


if __name__ == "__main__":
    # from https://stackoverflow.com/questions/63347818/aiohttp-client-exceptions-clientconnectorerror-cannot-connect-to-host-stackover
    if (
        sys.platform.startswith("win")
        and sys.version_info[0] == 3
        and sys.version_info[1] >= 8
    ):
        logger.info("Set policy")
        policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
    asyncio.run(main())
