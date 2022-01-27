from typing import Type

from .base import BaseStrategy
from .dump import DumpStrategy
from .scalp import ScalpStrategy
from .test import TestStrategy


STRATEGIES = {DumpStrategy, ScalpStrategy, TestStrategy}
STRATEGIES_BY_NAME = {strategy.name: strategy for strategy in STRATEGIES}


def get_strategy(name: str) -> Type[BaseStrategy]:
    assert name in STRATEGIES_BY_NAME
    return STRATEGIES_BY_NAME[name]
