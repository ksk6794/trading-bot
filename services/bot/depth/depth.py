import asyncio
import copy
import inspect
import logging
from decimal import Decimal
from queue import Queue
from typing import Dict, List, Callable, Set, Iterable

from modules.models import DepthModel, DepthUpdateModel


class Depth:
    def __init__(self, limit: int):
        self._limit = limit

        self._bids: Dict[str, Decimal] = {}
        self._asks: Dict[str, Decimal] = {}

        self._last_update_id = 0
        self._is_snapshot_set = False
        self._is_first_update_processed = False
        self._gap_callbacks: Set[Callable] = set()

        self._queue = Queue()
        self._loop = asyncio.get_event_loop()

    def add_gap_callback(self, cb: Callable):
        self._gap_callbacks.add(cb)

    def set_snapshot(self, model: DepthModel):
        self._bids.clear()
        self._asks.clear()

        self._update(model.bids, self._bids)
        self._update(model.asks, self._asks)

        self._last_update_id = model.last_update_id
        self._is_snapshot_set = True

        logging.info('Depth: snapshot set')

        # Apply deferred updates
        while not self._queue.empty():
            item = self._queue.get()
            self.update(item)

    def update(self, model: DepthUpdateModel):
        if not self._is_snapshot_set:
            self._queue.put(model)

        else:
            if self._is_first_update_processed:
                if model.first_update_id == self._last_update_id + 1:
                    self._update(model.bids, self._bids)
                    self._update(model.asks, self._asks)
                    self._last_update_id = model.last_update_id

                else:
                    missing = model.first_update_id - self._last_update_id + 1
                    logging.error('Missing %d depth updates!', missing)
                    self._last_update_id = 0
                    self._is_snapshot_set = False
                    self._is_first_update_processed = False
                    self._asks.clear()
                    self._asks.clear()

                    self._trigger_callbacks(self._gap_callbacks)

            else:
                # Skip outdated depth updates
                if model.last_update_id <= self._last_update_id:
                    return

                if self._last_update_id == 0 or model.first_update_id <= self._last_update_id + 1 <= model.last_update_id:
                    self._update(model.bids, self._bids)
                    self._update(model.asks, self._asks)
                    self._last_update_id = model.last_update_id
                    self._is_first_update_processed = True

    def _update(self, items: List[List[Decimal]], container: Dict[str, Decimal]):
        for [price, quantity] in items:
            if quantity == 0:
                container.pop(price, None)

            else:
                is_sort_required = price not in container
                container[price] = quantity

                if is_sort_required:
                    sorted_keys = sorted(container, key=container.get)[:self._limit]
                    container_cp = copy.copy(container)
                    container.clear()
                    container.update({k: container_cp[k] for k in sorted_keys})

    def _trigger_callbacks(self, callbacks: Iterable[Callable], *args, **kwargs):
        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                self._loop.create_task(result)
