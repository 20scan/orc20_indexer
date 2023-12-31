import os
import sys
import asyncio
from environs import Env
from typing import Union, Tuple
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable, CreateIndex
from sqlalchemy.dialects.postgresql import ENUM, JSON, ARRAY
from sqlalchemy.dialects.postgresql import insert
from aiopg.sa import create_engine
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from src.data.processer.interface import Interface
from src.structure import Event, Token, Balance, Pending_Inscriptions, OTC, OTC_Record


class Pgsql(Interface):
    def __init__(self):
        metadata = sa.MetaData()

        self.event = sa.Table(
            "event",
            metadata,
            sa.Column("id", sa.String(255), primary_key=True, unique=True),
            sa.Column("event_type", ENUM("TRANSFER", "INSCRIBE", name="event_type")),
            sa.Column("block_height", sa.BigInteger),
            sa.Column("block_index", sa.BigInteger),
            sa.Column("timestamp", sa.BigInteger),
            sa.Column("inscription_id", sa.String(255)),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("sender", sa.String(255)),
            sa.Column("receiver", sa.String(255)),
            sa.Column("content", JSON),
            sa.Column("operation", sa.String(255)),
            sa.Column("function_id", sa.BigInteger),
            sa.Column("valid", sa.Boolean),
            sa.Column("error", sa.String(255)),
        )

        self.pending_inscriptions = sa.Table(
            "pending_inscriptions",
            metadata,
            # address
            sa.Column("id", sa.String(255), primary_key=True, unique=True),
            sa.Column("inscriptions", ARRAY(sa.String(255)), default=[]),
        )

        self.token = sa.Table(
            "token",
            metadata,
            # tid: inscription number
            sa.Column("id", sa.BigInteger, primary_key=True, unique=True),
            # inscription info
            sa.Column("tick", sa.String(255)),
            sa.Column("max", sa.DECIMAL(38, 18)),
            sa.Column("lim", sa.DECIMAL(38, 18)),
            sa.Column("dec", sa.Integer),
            sa.Column("ug", sa.Boolean),
            sa.Column("mp", sa.Boolean),
            # deploy info
            sa.Column("deployer", sa.String(255)),
            sa.Column("deploy_time", sa.BigInteger),
            sa.Column("inscription_id", sa.String(255)),
            # mint info
            sa.Column("first_number", sa.BigInteger, default=0),
            sa.Column("first_id", sa.String(255), default=""),
            sa.Column("first_time", sa.BigInteger, default=0),
            sa.Column("last_number", sa.BigInteger, default=0),
            sa.Column("last_id", sa.String(255), default=""),
            sa.Column("last_time", sa.BigInteger, default=0),
            sa.Column("minted", sa.DECIMAL(38, 18), default=0),
            sa.Column("burned", sa.DECIMAL(38, 18), default=0),
            sa.Column("circulating", sa.DECIMAL(38, 18), default=0),
            sa.Column("holders", sa.BigInteger, default=0),
            # upgrade info
            sa.Column("last_upgrade_time", sa.BigInteger, default=0),
            sa.Column("upgrade_records", ARRAY(sa.String(255)), default=[]),
        )
        self.token_index_list = [
            sa.Index("deployer", self.token.c.deployer),
            sa.Index("inscription_id", self.token.c.inscription_id),
        ]

        self.balance = sa.Table(
            "balance",
            metadata,
            # {address}-{tid: inscription_number}
            sa.Column("id", sa.String(255), primary_key=True, unique=True),
            # token info
            sa.Column("tick", sa.String(255)),
            sa.Column("tid", sa.BigInteger),
            sa.Column("inscription_id", sa.String(255)),
            # balance info
            sa.Column("address", sa.String(255)),
            sa.Column("balance", sa.DECIMAL(38, 18)),
            sa.Column("available_balance", sa.DECIMAL(38, 18)),
            sa.Column("transferable_balance", sa.DECIMAL(38, 18)),
            sa.Column("original_balance", sa.DECIMAL(38, 18)),
        )
        self.balance_index_list = [
            sa.Index("tid", self.balance.c.tid),
            sa.Index("address", self.balance.c.address),
        ]

        self.otc = sa.Table(
            "otc",
            metadata,
            # oid: inscription number
            sa.Column("id", sa.BigInteger, primary_key=True, unique=True),
            # inscription info
            sa.Column("tick1", sa.String(255)),
            sa.Column("tid1", sa.BigInteger),
            sa.Column("supply", sa.DECIMAL(38, 18)),
            sa.Column("tick2", sa.String(255)),
            sa.Column("tid2", sa.BigInteger),
            sa.Column("er", sa.DECIMAL(38, 18)),
            sa.Column("mba", sa.DECIMAL(38, 18)),
            sa.Column("dl", sa.BigInteger),
            # deploy info
            sa.Column("owner", sa.String(255)),
            sa.Column("deploy_time", sa.BigInteger),
            sa.Column("inscription_id", sa.String(255)),
            # order status
            sa.Column("valid", sa.Boolean),
            sa.Column("success", sa.Boolean),
            sa.Column("received", sa.DECIMAL(38, 18)),
            # execute info
            sa.Column("execute_id", sa.String(255)),
        )
        self.otc_index_list = [
            sa.Index("owner", self.otc.c.owner),
            sa.Index("inscription_id", self.otc.c.inscription_id),
            sa.Index("tid1", self.otc.c.tid1),
            sa.Index("tid2", self.otc.c.tid2),
        ]

        self.otc_record = sa.Table(
            "otc_record",
            metadata,
            # event id
            sa.Column("id", sa.String(255), primary_key=True, unique=True),
            # otc info
            sa.Column("oid", sa.BigInteger),
            # record info
            sa.Column("inscription_id", sa.String(255)),
            # balance info
            sa.Column("address", sa.String(255)),
            sa.Column("amount_out", sa.DECIMAL(38, 18)),
            sa.Column("amount_in", sa.DECIMAL(38, 18)),
        )
        self.otc_record_index_list = [
            sa.Index("oid", self.otc_record.c.oid),
            sa.Index("inscription_id", self.otc_record.c.inscription_id),
            sa.Index("address", self.otc_record.c.address),
        ]

    # ==================== initialize ====================

    async def init(self):
        env = Env()
        env.read_env()
        self.engine = await create_engine(
            user=env.str("PGSQL_USER"),
            password=env.str("PGSQL_PASSWD"),
            database=env.str("PGSQL_DB"),
            host=env.str("PGSQL_HOST"),
        )

    async def close(self):
        try:
            self.engine.close()
            await self.engine.wait_closed()
        except Exception as e:
            error = f"Pgsql::close: Failed close database connection {e}"
            logger.error(error)
            raise Exception(error)

    async def clear_table(self, table, table_name, index_list=[]):
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                await conn.execute(CreateTable(table, if_not_exists=True))
                for index in index_list:
                    await conn.execute(CreateIndex(index, if_not_exists=True))
        except Exception as e:
            error = f"Pgsql::clear_table: Failed to drop and create tables {e}"
            logger.error(error)
            raise Exception(error)
        return

    async def create_table(self, table, index_list=[]):
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(CreateTable(table, if_not_exists=True))
                for index in index_list:
                    await conn.execute(CreateIndex(index, if_not_exists=True))
        except Exception as e:
            error = f"Pgsql::create_table: Failed to create tables {e}"
            logger.error(error)
            raise Exception(error)

    async def clear_all_tables(self):
        await self.clear_table(self.token, "token", self.token_index_list)
        await self.clear_table(self.balance, "balance", self.balance_index_list)
        await self.clear_table(self.pending_inscriptions, "pending_inscriptions")
        await self.clear_table(self.otc, "otc", self.otc_index_list)
        await self.clear_table(
            self.otc_record, "otc_record", self.otc_record_index_list
        )

    # ==================== save ====================

    async def save_token(self, token: Token):
        try:
            insert_stmt = insert(self.token).values(vars(token))
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_token: Failed to save token {e}"
            logger.error(error)
            raise Exception(error)

    async def batch_save_tokens_in_dict(self, token: list[dict]):
        try:
            insert_stmt = insert(self.token).values(token)
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::batch_save_tokens_in_dict: Failed to save token {e}"
            logger.error(error)
            raise Exception(error)

    async def save_event(self, event: Event):
        try:
            insert_stmt = insert(self.event).values(vars(event))
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_event: Failed to save event {e}"
            logger.error(error)
            raise Exception(error)

    async def save_pending_inscription(self, pending_inscription: Pending_Inscriptions):
        try:
            insert_stmt = insert(self.pending_inscriptions).values(
                vars(pending_inscription)
            )
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_pending_inscription: Failed to save pending inscription {e}"
            logger.error(error)
            raise Exception(error)

    async def save_balance(self, balance: Balance):
        try:
            insert_stmt = insert(self.balance).values(vars(balance))
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_balance: Failed to save balance {e}"
            logger.error(error)
            raise Exception(error)

    async def batch_save_balances(self, balances: list[Balance]):
        new_balances = [vars(balance) for balance in balances]

        try:
            insert_stmt = insert(self.balance).values(new_balances)
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::batch_save_balances: Failed to batch save balances {e}"
            logger.error(error)
            raise Exception(error)

    async def batch_save_balances_in_dict(self, balances: list[dict]):
        try:
            insert_stmt = insert(self.balance).values(balances)
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = (
                f"Pgsql::batch_save_balances_in_dict: Failed to batch save balances {e}"
            )
            logger.error(error)
            raise Exception(error)

    async def save_otc(self, otc: OTC):
        try:
            insert_stmt = insert(self.otc).values(vars(otc))
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_otc: Failed to save otc {e}"
            logger.error(error)
            raise Exception(error)

    async def save_otc_record(self, otc_record: OTC_Record):
        try:
            insert_stmt = insert(self.otc_record).values(vars(otc_record))
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"], set_={c.name: c for c in insert_stmt.excluded}
            )
            async with self.engine.acquire() as conn:
                await conn.execute(on_conflict_stmt)

        except Exception as e:
            error = f"Pgsql::save_otc_record: Failed to save otc record {e}"
            logger.error(error)
            raise Exception(error)

    # ==================== get ====================

    async def get_token(self, token_id: int) -> Union[Token, None]:
        try:
            async with self.engine.acquire() as conn:
                query = self.token.select().where(self.token.c.id == token_id)
                result = await conn.execute(query)
                token = await result.fetchone()
                if token is None:
                    return None
                return Token(**token)
        except Exception as e:
            error = f"Pgsql::get_token: Failed to get token {e}"
            logger.error(error)
            raise Exception(error)

    async def get_pending_inscription(
        self, address: str
    ) -> Union[Pending_Inscriptions, None]:
        try:
            async with self.engine.acquire() as conn:
                query = self.pending_inscriptions.select().where(
                    self.pending_inscriptions.c.id == address
                )
                result = await conn.execute(query)
                pending_inscriptions = await result.fetchone()
                if pending_inscriptions is None:
                    return None
                return Pending_Inscriptions(**pending_inscriptions)
        except Exception as e:
            error = f"Pgsql::get_pending_inscription: Failed to get pending inscriptions {e}"
            logger.error(error)
            raise Exception(error)

    async def get_balance(self, address: str, token_id: int) -> Union[Balance, None]:
        balance_id = f"{address}-{token_id}"
        try:
            async with self.engine.acquire() as conn:
                query = self.balance.select().where(self.balance.c.id == balance_id)
                result = await conn.execute(query)
                balance = await result.fetchone()
                if balance is None:
                    return None
                return Balance(**balance)
        except Exception as e:
            error = f"Pgsql::get_balance: Failed to get balance {e}"
            logger.error(error)
            raise Exception(error)

    async def get_otc(self, otc_id: int) -> Union[OTC, None]:
        try:
            async with self.engine.acquire() as conn:
                query = self.otc.select().where(self.otc.c.id == otc_id)
                result = await conn.execute(query)
                otc = await result.fetchone()
                if otc is None:
                    return None
                return OTC(**otc)
        except Exception as e:
            error = f"Pgsql::get_otc: Failed to get otc {e}"
            logger.error(error)
            raise Exception(error)

    async def get_otc_records(self, oid: int) -> Union[list[OTC_Record], None]:
        try:
            async with self.engine.acquire() as conn:
                query = self.otc_record.select().where(self.otc_record.c.oid == oid)
                result = await conn.execute(query)
                otc_records = await result.fetchall()
                if otc_records is None:
                    return None

                ret_otc_records = [
                    OTC_Record(**otc_record) for otc_record in otc_records
                ]

                return ret_otc_records
        except Exception as e:
            error = f"Pgsql::get_otc_records: Failed to get otc records {e}"
            logger.error(error)
            raise Exception(error)

    async def get_events_by_block_height(
        self, block_height
    ) -> Union[list[Event], None]:
        try:
            async with self.engine.acquire() as conn:
                query = self.event.select().where(
                    self.event.c.block_height == block_height
                )
                result = await conn.execute(query)
                events = await result.fetchall()
                if events is None:
                    return None

                ret_events = [Event(**event) for event in events]
                sorted_ret_events = sorted(ret_events, key=lambda x: x.block_index)
                return sorted_ret_events
        except Exception as e:
            error = f"Pgsql::get_events_by_block_height: Failed to get events {e}"
            logger.error(error)
            raise Exception(error)
