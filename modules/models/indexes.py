from pymongo import IndexModel, ASCENDING
from modules.models import OrderModel, PositionModel, UpdateLogModel


INDEXES = {
    UpdateLogModel: [
        IndexModel(
            keys=[
                ('s', ASCENDING),
                ('t', ASCENDING),

            ],
            name='symbol__timestamp__index',
        ),
    ],
    OrderModel: [
        IndexModel(
            keys=[
                ('id', ASCENDING)
            ],
            name='id__index',
            unique=True
        ),
        IndexModel(
            keys=[
                ('symbol', ASCENDING),
                ('timestamp', ASCENDING)
            ],
            name='symbol__timestamp__index'
        ),
        IndexModel(
            keys=[
                ('symbol', ASCENDING),
                ('side', ASCENDING),
                ('timestamp', ASCENDING)
            ],
            name='symbol__side__timestamp__index'
        ),
    ],
    PositionModel: [
        IndexModel(
            keys=[
                ('id', ASCENDING)
            ],
            name='id__index',
            unique=True
        ),
        IndexModel(
            keys=[
                ('symbol', ASCENDING),
                ('strategy_id', ASCENDING),
                ('status', ASCENDING),
                ('timestamp', ASCENDING)
            ],
            name='symbol__strategy_id__status__timestamp__index'
        ),
    ],
}
