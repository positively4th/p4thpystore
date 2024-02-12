import unittest
from asyncio import sleep
from math import pow

from contrib.pyas.src.pyas_v3 import As

from src.pipetransformstore import PipeTransformStore
from src.store import Store
from test.storetesttools import StoreTestTools
from test.dictstore import DictStore


class TestPipeTransformStore(unittest.IsolatedAsyncioTestCase):

    async def test_CRUD(self):

        async def squareNum(item: dict, inverse=False) -> dict:
            await sleep(0.1)
            res = {**item}
            if 'num' in item:
                res['num'] = int(pow(res['num'], 0.5 if inverse else 2))
            return res

        listener, flusher = StoreTestTools.createEventListenerAndFlusher()
        implodedStore = As(PipeTransformStore, DictStore, Store,
                           *Store.prototypes)({
                               'pipeTransforms': [squareNum, PipeTransformStore.createExplodeTransformer('scalar', ['a', 'b'])]
                           })
        implodedStore.registerEventCallback(listener)

        # Test missing
        expItem = None
        act = await implodedStore.get('A')
        self.assertEqual(expItem, act)

        # Test create
        oldExpItem = expItem
        expItem = {
            'id': '_A',
            'name': 'A',
            'num': 4,
            'a': '1',
            'b': '2',
        }
        expRow = {
            'id': '_A',
            'name': 'A',
            'num': 2,
            'scalar': {
                'a': '1',
                'b': '2',
            }
        }
        expEvent = {
            'type': Store.EventType.SAVE,
            'id': expItem['id'],
            'new': expItem,
            'old': oldExpItem,
        }
        act = await implodedStore.save(expItem)
        self.assertEqual(expItem, act)
        act = await implodedStore.get(expItem['id'])
        self.assertEqual(expItem, act)
        act = implodedStore['dictDB'].query(whereIns={
            'id': [expItem['id']]
        })
        act = None if len(act) != 1 else act[0]
        self.assertEqual(expRow, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(implodedStore, actStoree)

        # Test update
        oldExpItem = expItem
        expItem = {
            'id': '_A',
            'name': 'A',
            'num': 9,
            'a': '11',
            'b': '2',
        }
        expRow = {
            'id': '_A',
            'name': 'A',
            'num': 3,
            'scalar': {
                'a': '11',
                'b': '2',
            }
        }
        expEvent = {
            'type': Store.EventType.SAVE,
            'id': expItem['id'],
            'new': expItem,
            'old': oldExpItem,
        }
        act = await implodedStore.save([expItem])
        act = None if len(act) != 1 else act[0]
        self.assertEqual(expItem, act)
        act = await implodedStore.get(expItem['id'])
        self.assertEqual(expItem, act)
        act = implodedStore['dictDB'].query(whereIns={
            'id': [expItem['id']]
        })
        act = None if len(act) != 1 else act[0]
        self.assertEqual(expRow, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(implodedStore, actStoree)

        # Test delete
        expEvent = {
            'type': Store.EventType.DELETE,
            'id': expItem['id'],
            'new': None,
            'old': expItem,
        }

        act = await implodedStore.delete({'key': expItem['id']})
        act = act['key'] if 'key' in act else None
        self.assertEqual(expItem, act)
        act = await implodedStore.get(expItem['id'])
        expItem = None
        self.assertEqual(expItem, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(implodedStore, actStoree)


if __name__ == '__main__':

    unittest.main()
