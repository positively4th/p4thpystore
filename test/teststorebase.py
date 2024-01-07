import unittest
import ramda as R
from dill import loads, dumps
from collections.abc import Hashable

from contrib.pyas.src.pyas_v3 import As
from contrib.p4thpymap.src.async_map import map as p4thmap

from src.store import Store


class TestStoreBase(unittest.IsolatedAsyncioTestCase):

    def createEventListenerAndFlusher():

        orderedEvents = []

        def listener(event: Store.Event, store):
            orderedEvents.append((event, store))

        def flusher():
            res = []
            while len(orderedEvents) > 0:
                res.append(orderedEvents.pop(0))
            return res

        return listener, flusher

    class DictStore:

        prototypes = [
        ]

        columnSpecs = {}

        @staticmethod
        def onNew(cls, self):
            self.db = {}

        @staticmethod
        def idGetter(item: any) -> any:
            return item['id']

        async def select(self, ids: list | tuple) -> list:
            idItemsMap = R.pipe(
                R.map(lambda id: (id, [loads(dumps(self.db[id]))]
                      if id in self.db else None)),
                R.filter(lambda pair: pair[1] is not None),
                R.from_pairs
            )(ids)
            return idItemsMap

        async def _saveOne(self, item: dict):
            with self.pendingMapLock:
                self.db[self.itemId(item)] = loads(dumps(item))
                self.forgetItem(self.itemId(item))
            return item

        async def _deleteOne(self, itemId: any):
            res = await self.get(itemId)
            with self.pendingMapLock:
                if itemId in self.db:
                    self.db.pop(itemId)
                    self.forgetItem(itemId)

            return res

        def selectFromDB(self, whereIns: dict = None):
            res = R.values(self.db)
            if whereIns is not None:
                for key, whereIn in whereIns.items():
                    res = R.filter(lambda item: item[key] in whereIn)(res)

            return res
