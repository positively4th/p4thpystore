import unittest
from time import sleep

from contrib.pyas.src.pyas_v3 import As

from src.store import Store
from src.mixins.contextlogger import ContextLogger
from test.teststorebase import TestStoreBase


class TestStore(TestStoreBase):

    def setUp(self):
        self.contextLogger = ContextLogger.setContextLogger(As(ContextLogger)({
            'resultHandler': ContextLogger.countResultHandler,
            'isActive': True,
        }))

    def tearDown(self) -> None:
        while self.contextLogger.isPending:
            sleep(1)
        return super().tearDown()

    async def test_CRUD(self):
        listener, flusher = TestStore.createEventListenerAndFlusher()
        storeA = As(TestStore.DictStore, Store,
                    *Store.prototypes)({'name': 'ChildA'})
        storeA.registerEventCallback(listener)

        # Test missing
        exp = None
        act = await storeA.get('A')
        self.assertEqual(exp, act)

        # Test create
        exp = {
            'id': '_A',
            'name': 'A',
            'num': 1,
        }
        expEvent = {
            'type': Store.EventType.SAVE,
            'id': exp['id'],
            'item': exp
        }
        await storeA.save(exp)
        act = await storeA.get(exp['id'])
        self.assertEqual(exp, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(storeA, actStoree)

        # Test update
        exp = {
            'id': '_A',
            'name': 'A',
            'num': 2,
        }
        expEvent = {'type': Store.EventType.SAVE, 'id': exp['id'], 'item': exp}
        await storeA.save([exp])
        act = act[0] if len(exp) == 1 else None
        act = await storeA.get(exp['id'])
        self.assertEqual(exp, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(storeA, actStoree)

        # Test delete
        expEvent = {'type': Store.EventType.DELETE,
                    'id': exp['id'],
                    'item': exp}

        act = await storeA.delete({'key': exp['id']})
        act = act['key'] if 'key' in act else None
        self.assertEqual(exp, act)
        act = await storeA.get(exp['id'])
        exp = None
        self.assertEqual(exp, act)

        eventStoreePair = flusher()
        self.assertEqual(1, len(eventStoreePair))
        eventStoreePair = eventStoreePair[0]
        actEvent = eventStoreePair[0]
        actStoree = eventStoreePair[1]
        self.assertEqual(expEvent, actEvent)
        self.assertEqual(storeA, actStoree)


if __name__ == '__main__':

    unittest.main()
