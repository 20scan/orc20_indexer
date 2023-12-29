import sys
import asyncio
import aioredis
from loguru import logger
from environs import Env


class RedisHelper:
    def __init__(self):
        """
        Initialize the RedisHelper class.

        This class provides methods to interact with Redis.

        Attributes:
            semaphore (asyncio.Semaphore): A semaphore to limit the number of concurrent Redis operations.
            pool (aioredis.ConnectionPool): A connection pool for Redis.
            redis (aioredis.Redis): A Redis client.
            redis_key_current_block (str): The key for storing the current block value in Redis.
        """
        self.semaphore = asyncio.Semaphore(100)

        env = Env()
        env.read_env()
        redis_url = env.str("REDIS_URL")

        self.pool = aioredis.ConnectionPool.from_url(redis_url, decode_responses=True)
        self.redis = aioredis.Redis(connection_pool=self.pool, health_check_interval=30)

        self.redis_key_current_block = "current_block"
        self.redis_key_block_rollback = "block_rollback"

    async def close(self):
        """
        Close the Redis connection pool.
        """
        await self.pool.disconnect()

    async def get_current_block(self):
        """
        Get the current block value from Redis.

        Returns:
            str: The current block value.

        Raises:
            Exception: If there is an error while getting the current block value from Redis.
        """
        async with self.semaphore:
            try:
                return await self.redis.get(self.redis_key_current_block)
            except Exception as e:
                error = (
                    f"RedisHelper::get_current_block: Failed to get current block {e}"
                )
                logger.error(error)
                raise Exception(error)

    async def is_block_rollback(self, block: int):
        async with self.semaphore:
            try:
                return await self.redis.hexists(self.redis_key_block_rollback, block)
            except Exception as e:
                error = f"RedisHelper::is_block_rollback: Failed to check if block is rollback {e}"
                logger.error(error)
                raise Exception(error)
