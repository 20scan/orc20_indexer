import os
import sys
import json
import time
import signal
import asyncio
from environs import Env
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.log import setup_logging

setup_logging()

from src.main import handle_event
from src.data.processer.pgsql import Pgsql as DataProcesser
from src.data.redis_helper import RedisHelper
from src.alert import send_alert


class Run:
    """
    Class representing the main execution of the indexer.
    """

    def __init__(self):
        """
        Initialize the Run object.
        """
        self.set_signal()

        self.redis_helper = RedisHelper()
        self.data_processer = DataProcesser()
        self.stop_flag = False
        self.close_flag = False

        env = Env()
        env.read_env()
        self.start_block_height = env.int("CORE_START_BLOCK_HEIGHT")

        self.mempool_block_height = -1
        self.event_default_error = "not processed by indexer"

        self.default_block_confirmations = 6
        self.default_sleep_seconds = 10

    def set_signal(self):
        """
        Set the signal handlers for the Run object.
        """
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGHUP, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signal_num, frame):
        """
        Stop the execution of the Run object.
        """
        if self.stop_flag == True and self.close_flag == True:
            sys.exit(0)
        self.stop_flag = True
        self.close_flag = True

    async def init(self):
        """
        Initialize the data processer.
        """
        await self.data_processer.init()

    async def close(self):
        """
        Close the Redis connection and data processer.
        """
        await self.redis_helper.close()
        await self.data_processer.close()
        time.sleep(3)

    async def load_snapshot(self):
        """
        Load the snapshot data into the database.
        """
        await self.data_processer.clear_all_tables()

        data_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "data/snapshot/")
        )

        # Load tokens from tokens.json
        with open(f"{data_path}/tokens.json", "r") as tokens_file:
            tokens_json = json.load(tokens_file)

        # Load holders from holders.json
        with open(f"{data_path}/holders.json", "r") as holders_file:
            holders_json = json.load(holders_file)

        tokens = []
        for token in tokens_json.values():
            token["circulating"] = token["minted"]
            tokens.append(token)

        logger.info("saving tokens ...")
        await self.data_processer.batch_save_tokens_in_dict(tokens)

        holders = [
            holder
            for token_holders in holders_json.values()
            for holder in token_holders
        ]
        for holder in holders:
            holder["original_balance"] = holder["balance"]

        logger.info("saving balances ...")
        await self.data_processer.batch_save_balances_in_dict(holders)

        return

    async def get_latest_block_height(self):
        """
        Get the latest block height from Redis.
        """
        result = await self.redis_helper.get_current_block()
        if result is None:
            latest_block_height = self.start_block_height - 1
            return latest_block_height

        latest_block_height = int(result)
        return latest_block_height

    async def handle_block(self, block_height, is_pending=False):
        """
        Handle the events in a block.
        """
        try:
            events = await self.data_processer.get_events_by_block_height(block_height)
            if events is None:
                logger.error(f"Failed to get events by block height: {block_height}")
                self.stop_flag = True
                return

            logger.info(f"handling block: {block_height}, got {len(events)} events")

            for event in events:
                if is_pending is True and event.error != self.event_default_error:
                    continue
                await handle_event(event, self.data_processer, is_pending)

        except Exception as e:
            error = f"Failed to handle block: {block_height}, {e}"
            logger.error(error)
            await send_alert(error)
            self.stop_flag = True

        return

    async def reprocess_block(self, block_height):
        """
        Reprocess a block and its previous blocks.
        """
        try:
            for reprocess_block_height in range(
                block_height - self.default_block_confirmations, block_height
            ):
                if await self.redis_helper.is_block_rollback(reprocess_block_height):
                    self.stop_flag = True
                    break

            return True

        except Exception as e:
            error = f"Failed to reprocess block: {block_height}, {e}"
            logger.error(error)
            await send_alert(error)
            self.stop_flag = True

        return

    async def run(self):
        """
        Run the main execution loop.
        """
        logger.info("loading snapshot ...")
        await self.load_snapshot()
        logger.info("done")

        start_block_height = self.start_block_height

        while not self.stop_flag:
            latest_block_height = await self.get_latest_block_height()

            while start_block_height <= latest_block_height and not self.stop_flag:
                if start_block_height == latest_block_height:
                    await self.reprocess_block(latest_block_height)

                await self.handle_block(start_block_height)
                start_block_height += 1

            if self.stop_flag:
                break

            await self.handle_block(self.mempool_block_height, True)
            if self.stop_flag:
                break

            logger.debug(
                f"current block:{start_block_height}, latest blcok: {latest_block_height},  Waiting for new block ..."
            )
            time.sleep(self.default_sleep_seconds)

    async def main(self):
        """
        Main entry point of the indexer.
        """
        await self.init()

        while not self.close_flag:
            await self.run()
            self.stop_flag = False

        await self.close()


if __name__ == "__main__":
    asyncio.run(Run().main())
