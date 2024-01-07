import unittest
import ramda as R

from contrib.pyas.src.pyas_v3 import As

from src.nested.collectstore import CollectStore
from src.store import Store
from test.teststorebase import TestStoreBase


class TestCollectStore(TestStoreBase):

    @classmethod
    def sortById(cls, items: list | tuple | None) -> list | tuple | None:
        if items is None:
            return None

        return R.sort_by(lambda i: i['id'], items)

    async def test_CRUD(self):

        def storeeIdGetter(vehicle: any) -> Store:
            if vehicle['type'] == 'Bike':
                return bikeStoree.classId()

            if vehicle['type'] == 'Car':
                return carStoree.classId()

            return None

        listenerBikes, flusherBikes = TestCollectStore.createEventListenerAndFlusher()
        bikeStoree = As(TestCollectStore.DictStore, Store,
                        *Store.prototypes)({'name': 'Bikes'})
        bikeStoree.registerEventCallback(listenerBikes)

        listenerCars, flusherCars = TestCollectStore.createEventListenerAndFlusher()
        carStoree = As(TestCollectStore.DictStore, Store,
                       *Store.prototypes)({'name': 'Cars'})
        carStoree.registerEventCallback(listenerCars)

        collectStoree = As(CollectStore, TestStoreBase.DictStore, Store, *Store.prototypes)({
            'collectKeyStoreesMap': {
                'vehicles': [bikeStoree, carStoree],
            },
            'collectKeyStoreeIdGetterMap': {
                'vehicles': storeeIdGetter
            },
            'collectIdsGetterMap': {
                'vehicles': lambda item, *args, **kwargs: [
                    item['id'] for item in carStoree.selectFromDB(whereIns={'parentId': [item['id']]})
                ] + [
                    item['id'] for item in bikeStoree.selectFromDB(whereIns={'parentId': [item['id']]})
                ]
            },
        })

        # Test missing
        expItem = None
        actItem = await collectStoree.get('P')
        self.assertEqual(expItem, actItem)

        # Test create
        expBike1 = {'id': '_b1', 'parentId': '_pA',
                    'name': 'Bike 1', 'type': 'Bike'}
        expCar1 = {'id': '_c1', 'parentId': '_pA',
                   'name': 'Car 1', 'type': 'Car'}
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 1,
            'vehicles': [expBike1, expCar1],
        }
        await collectStoree.save(expItem)
        actItem = await collectStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test update collection
        expItem = {
            'id': '_pA',
            'name': 'A',
            'num': 2,
            'vehicles': [expBike1, expCar1],
        }
        actItem = await collectStoree.save({'ep': expItem})
        actItem = actItem['ep'] if 'ep' in actItem else None
        self.assertEqual(expItem, actItem)
        actItem = await collectStoree.get({'sep': expItem['id']})
        actItem = actItem['sep'] if 'sep' in actItem else None
        self.assertEqual(expItem, actItem)

        # Test update childA1 from collection
        expCar1 = {**expCar1, 'name': 'Car 1a'}
        expItem['vehicles'] = [expBike1, expCar1]

        actItem = await collectStoree.save([expItem])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)
        actItem = await collectStoree.get([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        self.assertEqual(expItem, actItem)

        # Test update child in children store
        expBike1 = {**expBike1, 'name': 'Bike 1b'}
        expItem['vehicles'] = [expBike1, expCar1]

        await bikeStoree.save(expBike1)
        actChild = await bikeStoree.get([expBike1['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(expBike1, actChild)

        actItem = await collectStoree.get(expItem['id'])
        self.assertEqual(expItem, actItem)

        # Test add child in children store
        expBike2 = {'id': '_b2', 'parentId': '_pA',
                    'name': 'Bike 2', 'type': 'Bike'}
        expItem['vehicles'] = self.sortById([expBike1, expBike2, expCar1])
        await bikeStoree.save(expBike2)
        actChild = await bikeStoree.get(expBike2['id'])
        self.assertEqual(expBike2, actChild)
        actItem = await collectStoree.get(expItem['id'])
        actItem['vehicles'] = self.sortById(actItem['vehicles'])

        # Test remove child in children store
        expItem['vehicles'] = self.sortById([expBike2, expCar1])
        actChild = await bikeStoree.delete(expBike1['id'])
        self.assertEqual(expBike1, actChild)
        actChild = await bikeStoree.get([expBike1['id']])
        actChild = actChild[0] if len(actChild) == 1 else None
        self.assertEqual(None, actChild)

        actItem = await collectStoree.get(expItem['id'])
        actItem['vehicles'] = self.sortById(actItem['vehicles'])
        self.assertEqual(expItem, actItem)

        # Test delete
        actItem = await collectStoree.delete([expItem['id']])
        actItem = actItem[0] if len(actItem) == 1 else None
        actItem['vehicles'] = self.sortById(actItem['vehicles'])
        self.assertEqual(expItem, actItem)
        actItem = await collectStoree.get([expItem['id']])
        expItem = [None]
        self.assertEqual(expItem, actItem)


if __name__ == '__main__':

    unittest.main()
