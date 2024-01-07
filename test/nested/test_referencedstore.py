import unittest

from contrib.pyas.src.pyas_v3 import As

from src.nested.referencedstore import ReferencedStore
from src.store import Store
from test.teststorebase import TestStoreBase


class TestReferencedStore(TestStoreBase):

    async def test_CRUD(self):

        listenerChildren, flusherChildA = TestReferencedStore.createEventListenerAndFlusher()
        childrenStoree = As(TestReferencedStore.DictStore, Store,
                            *Store.prototypes)({'name': 'ChildA'})
        childrenStoree.registerEventCallback(listenerChildren)

        referencedStoree = As(ReferencedStore, TestStoreBase.DictStore, Store, *Store.prototypes)({
            'referencedKeyStoreeMap': {
                'children': childrenStoree,
            },
            'referencedKeyIdGetterMap': {
                'children': lambda item, *args, **kwargs: item['parentId']
            },
            'referencedIdsGetterMap': {
                'children': lambda item, *args, **kwargs: [
                    item['id'] for item in childrenStoree.selectFromDB(whereIns={'parentId': [item['id']]})
                ]
            },
        })

        # Test missing
        expItem = None
        actItem = await referencedStoree.get('P')
        self.assertEqual(expItem, actItem)

        # Test create
        expChildA1 = {'id': '_cA1', 'parentId': '_pA',
                      'name': 'Child A 1', 'num': 10}
        await childrenStoree.save(expChildA1)
        expChildA2 = {'id': '_cA2', 'parentId': '_pA',
                      'name': 'Child A 2', 'num': 20}
        actItem = await childrenStoree.save(expChildA2)
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 1,
            'children': [expChildA1, expChildA2],
        }
        await referencedStoree.save(expItem)
        actItem = await referencedStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test update collection
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 2,
            'children': [expChildA1, expChildA2],
        }
        await referencedStoree.save({'ep': expItem})
        actItem = await referencedStoree.get({'sep': expItem['id']})
        actItem = actItem['sep'] if 'sep' in actItem else None
        self.assertEqual(expItem, actItem)

        # Test update childA1 from collection
        expChildA1 = {**expChildA1, 'num': 11}
        expItem['children'] = [expChildA1, expChildA2]

        await referencedStoree.save([expItem])
        actItem = await referencedStoree.get([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)

        # Test update child in children store
        expChildA2 = {**expChildA2, 'num': 21}
        expItem['children'] = [expChildA1, expChildA2]

        await childrenStoree.save(expChildA2)
        actChild = await childrenStoree.get([expChildA2['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expChildA2, actChild)

        actItem = await referencedStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test add child in children store
        expChildA3 = {'id': '_cA3', 'parentId': '_pA',
                      'name': 'Child A 3', 'num': 30}
        expItem['children'] = [expChildA1, expChildA2, expChildA3]
        await childrenStoree.save(expChildA3)
        actChild = await childrenStoree.get([expChildA3['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expChildA3, actChild)

        actItem = await referencedStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test remove child in children store
        expItem['children'] = [expChildA2, expChildA3]
        await childrenStoree.delete(expChildA1['id'])
        actChild = await childrenStoree.get([expChildA1['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(None, actChild)

        actItem = await referencedStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test delete
        actItem = await referencedStoree.delete([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)
        actItem = await referencedStoree.get([expItem['id']])
        expItem = [None]
        self.assertEqual(expItem, actItem)


if __name__ == '__main__':

    unittest.main()
