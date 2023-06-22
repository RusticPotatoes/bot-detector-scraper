import asyncio
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import ClientSession
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiohttp.client_exceptions import ClientHttpProxyError
import config.config as config
from config.config import app_config
from modules.scraper import Scraper
from modules.validation.player import Player, PlayerDoesNotExistException
from enum import Enum
from utils.http_exception_handler import InvalidResponse

logger = logging.getLogger(__name__)

class WorkerState(Enum):
    FREE = "free"
    WORKING = "working"
    BROKEN = "broken"


class Worker:
    def __init__(self, proxy: str):
        self.name = str(uuid.uuid4())
        self.state: WorkerState = WorkerState.FREE
        self.proxy: str = proxy

    async def initialize(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=app_config.KAFKA_HOST,  # Kafka broker address
            value_serializer=lambda x: json.dumps(x).encode(),
        )
        await self.producer.start()
        self.scraper = Scraper(self.proxy)
        self.session = aiohttp.ClientSession(timeout=app_config.SESSION_TIMEOUT)
        return self
    
    async def destroy(self):
        await self.session.close()
        await self.producer.stop()

    async def scrape_player(self, player: Player):
        self.state = WorkerState.WORKING
        hiscore = None
        try:
            hiscore = await self.scraper.lookup_hiscores(player, self.session)
            player.possible_ban = 0
            player.confirmed_ban = 0
            player.label_jagex = 0
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        except InvalidResponse:
            logger.warning(f"invalid response")
            self.state = WorkerState.FREE
            return
        except ClientHttpProxyError:
            logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
            self.state = WorkerState.BROKEN
            return
        except PlayerDoesNotExistException:
            # logger.info(f"Hiscore is empty for {player.name}")
            player.possible_ban = 1
            player.confirmed_player = 0

            # this is a bit much indenting
            try:
                player = await self.scraper.lookup_runemetrics(player, self.session)
            except InvalidResponse:
                logger.warning(f"Invalid response")
                self.state = WorkerState.FREE
                return
            except ClientHttpProxyError:
                logger.warning(f"ClientHttpProxyError killing worker name={self.name}")
                self.state = WorkerState.BROKEN
                return

        assert isinstance(
            player, Player
        ), f"expected the variable player to be of class Player, but got {player}"

        output = {"player": player.dict(), "hiscores": hiscore}
        await self.producer.send(topic="scraper", value=output)
        self.state = WorkerState.FREE
        return