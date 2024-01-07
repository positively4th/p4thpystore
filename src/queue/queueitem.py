from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T


class QueueItem(Leaf):
    prototypes = []

    columnSpecs = {
        '__id': {
            'transformer': T.constantNotEmpty(),
        },
        'result': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else []),
        },

    }
