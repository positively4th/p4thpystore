import unittest
import ramda as R

from contrib.pyas.src.pyas_v3 import As

from src.nested.linksmanystore import LinksManyStore
from src.store import Store
from test.teststorebase import TestStoreBase


class TestLinksManyStore(TestStoreBase):

    async def test_CRUD(self):

        listenerChildA, flusherChildA = TestLinksManyStore.createEventListenerAndFlusher()
        childStoree = As(TestLinksManyStore.DictStore, Store,
                         *Store.prototypes)({'name': 'ChildA'})
        childStoree.registerEventCallback(listenerChildA)

        linksManyStoree = As(LinksManyStore, TestLinksManyStore.DictStore, Store, *Store.prototypes)({
            'linksManyKeyStoreeMap': {
                'children': childStoree,
            },
            'linksManyKeyIdGetterMap': {
                'children': lambda item, *args, **kwargs: item['parentId']
            }
        })

        # Test missing
        expItem = None
        actItem = await linksManyStoree.get('P')
        self.assertEqual(expItem, actItem)

        # Test create
        expChildA1 = {'id': '_cA1', 'parentId': '_pA',
                      'name': 'Child A 1', 'num': 10}
        expChildA2 = {'id': '_cA2', 'parentId': '_pA',
                      'name': 'Child A 2', 'num': 20}
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 1,
            'children': [expChildA1, expChildA2],
        }
        actItem = await linksManyStoree.save(expItem)
        self.assertEqual(expItem, actItem)
        actItem = await linksManyStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test update collection
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 2,
            'children': [expChildA1, expChildA2],
        }
        await linksManyStoree.save({'ep': expItem})
        actItem = await linksManyStoree.get({'sep': expItem['id']})
        actItem = actItem['sep'] if 'sep' in actItem else None
        self.assertEqual(expItem, actItem)

        # Test update childA1 from collection
        expChildA1 = {**expChildA1, 'num': 11}
        expItem['children'] = [expChildA1, expChildA2]

        await linksManyStoree.save([expItem])
        actItem = await linksManyStoree.get([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)

        # Test update child in children store
        expChildA2 = {**expChildA2, 'num': 21}
        expItem['children'] = [expChildA1, expChildA2]

        await childStoree.save(expChildA2)
        actChild = await childStoree.get([expChildA2['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expChildA2, actChild)

        actItem = await linksManyStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test add child in children store
        expChildA3 = {'id': '_cA3', 'parentId': '_pA',
                      'name': 'Child A 3', 'num': 30}
        expItem['children'] = [expChildA1, expChildA2]
        await childStoree.save(expChildA3)
        actChild = await childStoree.get([expChildA3['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expChildA3, actChild)

        actItem = await linksManyStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test delete
        actItem = await linksManyStoree.delete([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)
        actItem = await linksManyStoree.get([expItem['id']])
        expItem = [None]
        self.assertEqual(expItem, actItem)


if __name__ == '__main__':

    unittest.main()
