import unittest
from time import sleep

from contrib.pyas.src.pyas_v3 import As

from src.store import Store
from src.mixins.contextlogger import ContextLogger
from test.storetesttools import StoreTestTools
from test.dictstore import DictStore


class TestStore(unittest.IsolatedAsyncioTestCase):

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

        async def wrapper(shapeShifter: callable = lambda x: x, msg: str = None):

            ss = shapeShifter
            msg = shapeShifter.__name__ if msg is None else msg
            listener, flusher = StoreTestTools.createEventListenerAndFlusher()
            storeA = As(DictStore)({'name': 'ChildA'})
            storeA.registerEventCallback(listener)

            # Test missing
            exp = None
            act = await storeA.get(ss('A'))
            self.assertEqual(ss(exp), act)

            # Test create
            oldExp = None
            exp = {
                'id': '_A',
                'name': 'A',
                'num': 1,
            }
            expEvent = {
                'type': Store.EventType.SAVE,
                'id': exp['id'],
                'new': exp,
                'old': oldExp,
            }
            act = await storeA.save(ss(exp))
            self.assertEqual(ss(exp), act)
            act = await storeA.get(ss(exp['id']))
            self.assertEqual(ss(exp), act)

            eventStoreePair = flusher()
            self.assertEqual(1, len(eventStoreePair))
            eventStoreePair = eventStoreePair[0]
            actEvent = eventStoreePair[0]
            actStoree = eventStoreePair[1]
            self.assertEqual(expEvent, actEvent)
            self.assertEqual(storeA, actStoree)

            # Test update
            oldExp = {**exp}
            exp = {
                'id': '_A',
                'name': 'A',
                'num': 2,
            }
            expEvent = {'type': Store.EventType.SAVE,
                        'id': exp['id'], 'new': exp, 'old': oldExp}
            act = await storeA.save(ss(exp))
            self.assertEqual(ss(exp), act)
            act = await storeA.get(ss(exp['id']))
            self.assertEqual(ss(exp), act)
            eventStoreePair = flusher()
            self.assertEqual(1, len(eventStoreePair))
            eventStoreePair = eventStoreePair[0]
            actEvent = eventStoreePair[0]
            actStoree = eventStoreePair[1]
            self.assertEqual(expEvent, actEvent)
            self.assertEqual(storeA, actStoree)

            # Test delete scalar
            oldExp = {**exp}
            expEvent = {'type': Store.EventType.DELETE,
                        'id': exp['id'],
                        'new': None,
                        'old': oldExp
                        }

            act = await storeA.delete(ss(exp['id']))
            self.assertEqual(ss(exp), act)
            act = await storeA.get(ss(exp['id']))
            exp = None
            self.assertEqual(ss(exp), act)

            eventStoreePair = flusher()
            self.assertEqual(1, len(eventStoreePair))
            eventStoreePair = eventStoreePair[0]
            actEvent = eventStoreePair[0]
            actStoree = eventStoreePair[1]
            self.assertEqual(expEvent, actEvent)
            self.assertEqual(storeA, actStoree)

        await wrapper(lambda x: x, 'ssScalar')
        await wrapper(lambda x: [x], 'ssList')
        await wrapper(lambda x: tuple([x]), 'ssTuple')
        # await wrapper(lambda x: set([x]), 'ssSet')
        await wrapper(lambda x: {'key': x}, 'ssDict')


if __name__ == '__main__':

    unittest.main()
