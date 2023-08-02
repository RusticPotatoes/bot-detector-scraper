import asyncio
import logging
import time

from aiohttp import ClientResponse, ClientSession

from modules.validation.player import Player
from utils.http_exception_handler import InvalidResponse

logger = logging.getLogger(__name__)


class RuneMetricsApi:
    def __init__(self, proxy: str = None) -> None:
        self.proxy = proxy
        self.base_url = "https://apps.runescape.com/runemetrics/profile/profile"

    async def lookup_runemetrics(self, player: Player, session: ClientSession) -> dict:
        """
        Performs a RuneMetrics lookup on the given player.

        :param player: a dictionary containing the player's name and id
        :return: a dictionary containing the player's RuneMetrics data
        """
        player_name = player.name
        base_url = "https://apps.runescape.com/runemetrics/profile/profile"
        url = f"{base_url}?user={player_name}"

        async with session.get(url, proxy=self.proxy) as response:
            data: dict = await self._handle_response_status(response, player)
            assert data is not None, "data cannot be none"

            logger.info(f"found {player_name} on runemetrics")

            match data.get("error"):
                case "NO_PROFILE":
                    # username is not associated to an account
                    player.label_jagex = 1
                case "NOT_A_MEMBER":
                    # account is perm banned
                    player.label_jagex = 2
                case "PROFILE_PRIVATE":
                    # runemetrics is set to private. either they're too low level or they're banned.
                    player.label_jagex = 3
                case _:
                    # account is active, probably just too low stats for hiscores
                    player.label_jagex = 0
            # API assigns this too, but just being safe
            player.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            return player

    async def _handle_response_status(
        self, response: ClientResponse, player: Player
    ) -> dict:
        status = response.status
        match status:
            # OK
            case 200:
                return await response.json()
            # NOK, but known
            case 429:
                logger.warning(status)
                await asyncio.sleep(15)
            case s if 500 <= s < 600:
                logger.warning(status)
                await asyncio.sleep(5)
            case 403:
                logger.warning(status)
                await asyncio.sleep(5)
            # NOK
            case _:
                body = await response.text()
                logger.error(
                    f"Unhandled status code {status}.\n"
                    f"Header: {response.headers}\n"
                    f"Body: {body}"
                )
        raise InvalidResponse()
