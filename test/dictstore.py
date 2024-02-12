from __future__ import annotations

from contrib.pyas.src.pyas_v3 import T
from contrib.pyas.src.pyas_v3 import As

from src.store import Store
from .testdb import TestDB


class DictStore:

    prototypes = [
        Store, *Store.prototypes
    ]

    columnSpecs = {
        'dictDB': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else As(TestDB)({})),
        }
    }

    @property
    def dictDB(self):
        return self['dictDB']

    @staticmethod
    def idGetter(item: any) -> any:
        return item['id']

    async def select(self, ids: list | tuple) -> list:
        res = self.dictDB.query(
            predicate=lambda item: self['idGetter'](item) in ids)
        # res = [item for item in self['db']
        #        if self['idGetter'](item) in ids]
        return res

    async def _saveOne(self, newItem: dict):
        with self.pendingMapLock:
            self.dictDB.upsert(newItem, self['idGetter'])
            # self.setItem({id: newItem})

        return newItem

    async def _deleteOne(self, itemId: any):
        res = await self.get(itemId)
        self.forgetItem(itemId)
        self.dictDB.delete(itemId, self['idGetter'])

        return res
