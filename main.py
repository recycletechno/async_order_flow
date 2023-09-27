import asyncio
import logging
import time
import numpy as np
import random
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(
    level=logging.INFO,
    format=f"--%(asctime)s [%(levelname)s] [async_loop] [%(funcName)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

log = logging

MODE_AWAIT = 0
MODE_FUTURE = 1


class TradingClass:

    def __init__(self, num_strategies, strat_chunk_size, mode):
        self.loop = asyncio.get_event_loop()
        self.strat_chunk_size = strat_chunk_size
        self.mode = mode
        self.strats = self.load_strategies(num_strategies)
        self.positions = self.init_positions_from_strats()
        self.strategy_locks: dict[str, bool] = {}
        self.last_chunk_loop_time = 0
        self.strats_processed = 0

    def locking_strat(func):  # noqa
        """
        Decorator to lock strategy from running if it is already running
        :param func: Function to decorate
        :type func: function
        :return: Decorated function
        :rtype: function
        """

        # This nice line raises warning in PyCharm, but it works
        # https://stackoverflow.com/questions/69362223/parameter-self-unfilled-when-using-decorators-even-after-instantiating-obje
        # async def wrapper(*args): self, strat = args

        # Decorator inside class
        # https://stackoverflow.com/questions/1263451/python-decorators-in-classes

        async def wrapper(self, strat):
            self.lock_strat(strat)
            try:
                result = await func(self, strat)  # noqa
            finally:
                self.unlock_strat(strat)
            return result

        return wrapper

    @property
    def positions_len(self):
        """
        Get number of positions
        :return: Number of positions
        :rtype: int
        """

        return len([x for x in self.positions.values() if x > 0])

    async def stats(self, sleep_time):
        """
        Checkup asyncio congestion. It should printout roughly every sleep_time seconds
        :param sleep_time: Time to sleep between printouts
        :type sleep_time: int
        :return: None
        :rtype: None
        """

        while True:
            start_time = time.time()
            await asyncio.sleep(sleep_time)
            actual_time = round(time.time() - start_time, 2)

            log.info(f"strats processed:[{'{:,}'.format(self.strats_processed)}], max chunk time:[{self.last_chunk_loop_time}]"
                     f" pos:[{self.positions_len}] stats time:[{actual_time}]")

    @staticmethod
    def get_chunk(lst: list, n: int) -> list:
        """
        Yield successive n-sized chunks from lst
        :param lst: List to split
        :type lst: list
        :param n: Size of chunk
        :type n: int
        :return: Yielded chunk
        :rtype: list
        """

        for i in range(0, len(lst), n):
            yield lst[i: i + n]

    @staticmethod
    def load_strategies(num_strategies: int) -> dict:
        """
        # Imitate loading strategies from DB or file
        :param num_strategies: Number of strategies to load
        :type num_strategies: int
        :return: Dictionary with strategies
        :rtype: dict
        """

        strats_dict = {}
        for strat_id in range(num_strategies):
            strats_dict[strat_id] = {}
            strats_dict[strat_id]["strat"] = str(strat_id)
            strats_dict[strat_id]["enter_limit"] = False
            strats_dict[strat_id]["exit_exp"] = False

        return strats_dict

    def init_positions_from_strats(self):
        """
        Init positions from strats
        :return: Positions dictionary
        :rtype: dict
        """

        positions = {}
        for strat in self.strats:
            positions[strat] = 0
        return positions

    async def run_trading_cycle(self):
        """
        Run trading cycle in asyncio tasks
        :return: None
        :rtype: None
        """

        # Usual case with chunks
        for chunk in self.get_chunk(list(self.strats), self.strat_chunk_size):
            loop.create_task(self.start_main_loop(chunk))

        log.info(f"Run [{len(self.strats)}] strategies in [{self.strat_chunk_size}] chunks")

    def is_strat_locked(self, strat):
        """
        Check if strategy is locked
        :param strat: Strategy to check
        :type strat: str
        :return: True if locked, False if not
        :rtype: bool
        """

        return strat in self.strategy_locks and self.strategy_locks[strat]

    def lock_strat(self, strat):
        """
        Lock strategy
        :param strat: Strategy to lock
        :type strat: str
        :return: None
        :rtype: None
        """

        self.strategy_locks[strat] = True

    def unlock_strat(self, strat):
        """
        Unlock strategy
        :param strat: Strategy to unlock
        :type strat: str
        :return: None
        :rtype: None
        """

        if strat in self.strategy_locks:
            self.strategy_locks[strat] = False

    @locking_strat  # noqa
    async def enter_limit_task(self, strat):
        """
        Enter order imitation task
        :param strat: Strategy to enter
        :type strat: str
        :return: None
        :rtype: None
        """

        log.debug(f"Enter limit [{strat}] started")

        # Imitate sending order to exchange
        await asyncio.sleep(random.uniform(0, 0.5))

        # Imitate receiving execution info from exchange websocket
        self.positions[strat] += 1
        self.strats[strat]["enter_limit"] = False

        log.debug(f"Enter limit [{strat}] finished")

    @locking_strat  # noqa
    async def exit_on_exp_task(self, strat):
        """
        Exit order imitation task
        :param strat: Strategy to exit
        :type strat: str
        :return: None
        :rtype: None
        """

        log.debug(f"Exit on exp [{strat}] started")

        log.debug(f"Exit on exp [{strat}] - cancel all orders")
        # Imitate canceling all orders
        await asyncio.sleep(random.uniform(0, 0.5))

        # Imitate evaluating min notional filter
        if np.random.random() > 0.5:
            log.debug(f"Exit on exp [{strat}] - eval min notional filter")
            await asyncio.sleep(random.uniform(1, 3))

        # Imitate sending market exit order
        log.debug(f"Exit on exp [{strat}] - sending market exit order")
        await asyncio.sleep(random.uniform(0, 0.5))

        # Imitate receiving execution info from exchange websocket
        self.positions[strat] -= 1
        self.strats[strat]["exit_exp"] = False

        log.debug(f"Exit on exp [{strat}] finished")

    async def start_main_loop(self, strats):
        """
        Start main strategy loop
        :param strats: Strategies to run
        :type strats: list
        :return: None
        :rtype: None
        """

        log.debug(f"Loop started [{len(strats)}]")

        while True:

            start_time = time.time()

            # Shuffle strategies to randomize order
            np.random.shuffle(strats)

            for strat in strats:

                # Strategy params
                sp = self.strats[strat]

                # Send enter order
                if sp["enter_limit"] and self.positions[strat] == 0:
                    # Check if strategy is not locked
                    if not self.is_strat_locked(strat):
                        # Send enter task
                        if self.mode == MODE_FUTURE:
                            self.loop.create_task(self.enter_limit_task(strat))
                        else:
                            await self.enter_limit_task(strat)
                # Send emergency exit
                elif sp["exit_exp"] and self.positions[strat] > 0:
                    # Check if strategy is not locked
                    if not self.is_strat_locked(strat):
                        # Send exit task
                        if self.mode == MODE_FUTURE:
                            self.loop.create_task(self.exit_on_exp_task(strat))
                        else:
                            await self.exit_on_exp_task(strat)
                # Pause after each strategy
                await asyncio.sleep(0.0)

                # Posion check (0 or 1)
                assert self.positions[strat] in (0, 1), f"Position [{self.positions[strat]}] violation on [{strat}]"

            # Pause after each strat chunk
            await asyncio.sleep(0.0)

            actual_time = round(time.time() - start_time, 2)

            # Stats
            if actual_time > self.last_chunk_loop_time:
                self.last_chunk_loop_time = actual_time
            self.strats_processed += 1 * self.strat_chunk_size

            log.debug(f"Loop finished [{len(strats)}] - {actual_time}")

    async def recalc_signals(self):
        """
        Imitate calculation of signals with technical analysis, machine learning, etc.
        :return: None
        """

        log.debug("Recalc signals started")

        for strat in self.strats:
            if self.positions[strat] == 0:
                if np.random.random() > 0.9:
                    self.strats[strat]["enter_limit"] = True
                    log.debug(f"Recalc signals - enter limit [{strat}]")

            if not self.strats[strat]["enter_limit"] and self.positions[strat] > 0:
                if np.random.random() > 0.85:
                    self.strats[strat]["exit_exp"] = True
                    log.debug(f"Recalc signals - exit exp [{strat}]")

        log.debug("Recalc signals finished")

    @staticmethod
    def handle_exception(outer_loop, context):
        """
        Handle exception in event loop
        :param outer_loop: Outer loop
        :type outer_loop: asyncio.AbstractEventLoop
        :param context: Exception context
        :type context: dict
        :return: None
        :rtype: None
        """

        log.error(f"Exception in outer loop: {context}")
        outer_loop.stop()


async def main():
    # Checkup asyncio congestion
    loop.create_task(tcl.stats(sleep_time=3))

    # Run trading cycle
    await tcl.run_trading_cycle()

    # Init scheduler
    scheduler.configure(timezone="utc")
    scheduler.add_job(tcl.recalc_signals, trigger="cron", second="*/10")
    log.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
    scheduler.start()

    log.debug("Loop done")


if __name__ == "__main__":

    # Set main class
    tcl = TradingClass(
        num_strategies=100_000,
        strat_chunk_size=500,
        mode=MODE_FUTURE
    )

    # Init asyncio loop and exception handler
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(tcl.handle_exception)

    # Scheduler config and run
    scheduler: AsyncIOScheduler = AsyncIOScheduler()

    try:
        loop.create_task(main())
        loop.run_forever()
    finally:
        scheduler.shutdown()
        log.debug("Successfully shutdown")
