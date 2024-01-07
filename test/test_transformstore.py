import unittest
from json import dumps, loads

from contrib.pyas.src.pyas_v3 import As

from src.transformstore import TransformStore
from src.store import Store
from test.teststorebase import TestStoreBase


class TestTransformStore(TestStoreBase):

    async def jsonTransformer(value: any, item: any, storee: Store, inverse: bool = False) -> any:
        return dumps(value) if inverse else loads(value)

    async def test_CRUD(self):
        listener, flusher = TestStoreBase.createEventListenerAndFlusher()
        jsonStore = As(TransformStore, TestStoreBase.DictStore, Store,
                       *Store.prototypes)({
                           'name': 'ChildA',
                           'keyTransformerMap': {
                               'num': TestTransformStore.jsonTransformer
                           }
                       })
        jsonStore.registerEventCallback(listener)

        # Test missing
        exp = None
        act = await jsonStore.get('A')
        self.assertEqual(exp, act)

        # Test create
        exp = {
            'id': '_A',
            'name': 'A',
            'num': [1],
        }
        expEvent = {
            'type': Store.EventType.SAVE,
            'id': exp['id'],
            'item': exp
        }
        act = await jsonStore.save(exp)
        self.assertEqual(exp, act)
        act = await jsonStore.get(exp['id'])
        self.assertEqual(exp, act)
        expDB = {**exp, 'num': dumps(exp['num'])}
        act = jsonStore.selectFromDB(whereIns={
            'id': [exp['id']]
        })
        act = None if len(act) != 1 else act[0]
        self.assertEqual(expDB, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(jsonStore, actStoree)

        # Test update
        exp = {
            'id': '_A',
            'name': 'A',
            'num': [2],
        }
        expEvent = {'type': Store.EventType.SAVE, 'id': exp['id'], 'item': exp}
        act = await jsonStore.save([exp])
        act = None if len(act) != 1 else act[0]
        self.assertEqual(exp, act)
        act = act[0] if len(exp) == 1 else None
        act = await jsonStore.get(exp['id'])
        self.assertEqual(exp, act)
        expDB = {**exp, 'num': dumps(exp['num'])}
        act = jsonStore.selectFromDB(whereIns={
            'id': [exp['id']]
        })
        act = None if len(act) != 1 else act[0]
        self.assertEqual(expDB, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(jsonStore, actStoree)

        # Test delete
        expEvent = {'type': Store.EventType.DELETE,
                    'id': exp['id'],
                    'item': exp}

        act = await jsonStore.delete({'key': exp['id']})
        act = act['key'] if 'key' in act else None
        self.assertEqual(exp, act)
        act = await jsonStore.get(exp['id'])
        exp = None
        self.assertEqual(exp, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(jsonStore, actStoree)


if __name__ == '__main__':

    unittest.main()
