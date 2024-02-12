import unittest

from contrib.pyas.src.pyas_v3 import As

from src.nested.linksonestore import LinksOneStore
from src.store import Store
from test.storetesttools import StoreTestTools
from test.dictstore import DictStore


class TestLinksOneStore(unittest.IsolatedAsyncioTestCase):

    async def test_CRUD(self):

        listenerChildA, flusherChildA = StoreTestTools.createEventListenerAndFlusher()
        childAStoree = As(DictStore, Store,
                          *Store.prototypes)({'name': 'ChildA'})
        childAStoree.registerEventCallback(listenerChildA)

        listenerChildB, flusherChildB = StoreTestTools.createEventListenerAndFlusher()
        childBStoree = As(DictStore, Store,
                          *Store.prototypes)({'name': 'ChildB'})
        childBStoree.registerEventCallback(listenerChildB)

        linksOneStoree = As(LinksOneStore, DictStore, Store, *Store.prototypes)({
            'linksOneKeyStoreeMap': {
                'childA': childAStoree,
                'childB': childBStoree
            }
        })

        # Test missing
        expItem = None
        actItem = await linksOneStoree.get('P')
        self.assertEqual(expItem, actItem)

        # Test create
        expChildA = {'id': '_cA1', 'name': 'Child A 1', 'num': 10}
        expChildB = {'id': '_cB1', 'name': 'Child B 1', 'num': 20}
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 1,
            'childA': expChildA,
            'childB': expChildB,
        }
        await linksOneStoree.save(expItem)
        actItem = await linksOneStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test update parent
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 2,
            'childA': expChildA,
            'childB': expChildB,
        }
        await linksOneStoree.save({'ep': expItem})
        actItem = await linksOneStoree.get({'sep': expItem['id']})
        actItem = actItem['sep'] if 'sep' in actItem else None
        self.assertEqual(expItem, actItem)

        # Test update childA
        expChildA = {'id': '_cA1', 'name': 'Child A 1', 'num': 11}
        expItem['childA'] = expChildA

        await linksOneStoree.save([expItem])
        actItem = await linksOneStoree.get([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)

        # Test update childB in child store
        expChildB = {'id': '_cB1', 'name': 'Child B 1', 'num': 21}
        expItem['childB'] = expChildB

        await childBStoree.save(expChildB)
        actChild = await childBStoree.get([expChildB['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expChildB, actChild)

        actItem = await linksOneStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test delete
        actItem = await linksOneStoree.delete([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)
        actItem = await linksOneStoree.get([expItem['id']])
        expItem = [None]
        self.assertEqual(expItem, actItem)


if __name__ == '__main__':

    unittest.main()
