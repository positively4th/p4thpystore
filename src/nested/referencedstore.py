from __future__ import annotations
from json import dumps
import ramda as R
from inspect import iscoroutine

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from ..store import Store
from .relationstore import RelationStore


class ReferencedStore(Leaf):

    class ReferencedStoreError(Store.StoreError):
        pass

    prototypes = [RelationStore]

    columnSpecs = {
        'referencedKeyStoreeMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getReferencedKeyStoreeMap()),
        },
        'referencedKeyIdGetterMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getReferencedKeyIdGetterMap()),
        },
        'referencedIdsGetterMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getReferencedIdsGetterMap()),
        }

    }

    @classmethod
    def getReferencedKeyStoreeMap(cls, relatedItem: any, store: Store) -> any:
        raise ReferencedStore.ReferencedStoreError(
            'getReferencedKeyStoreeMap not implemented.')

    @classmethod
    def getReferencedKeyIdGetterMap(cls) -> any:
        raise ReferencedStore.ReferencedStoreError(
            'getReferencedKeyIdGetterMap not implemented.')

    @classmethod
    def getReferencedIdsGetterMap(cls) -> any:
        raise ReferencedStore.ReferencedStoreError(
            'getReferencedIdsGetterMap not implemented.')

    @staticmethod
    def onNew(cls, self):
        for relatedKey, relatedStoree in self['referencedKeyStoreeMap'].items():
            relatedStoree.registerEventCallback(
                lambda *args, **kwargs: self.referncedOnStoreEvent(relatedKey, *args, **kwargs))

    def referncedOnStoreEvent(self, relatedKey, event: Store.Event, storee: Store):
        if event['type'] == Store.EventType.SAVE:
            itemId = self.itemIdFromChild(relatedKey, event['new'])
            self.forgetItem(itemId)

        if event['type'] == Store.EventType.DELETE:
            itemId = self.itemIdFromChild(relatedKey, event['old'])
            self.forgetItem(itemId)

    @property
    def referencedKeyIdGetterMap(self):
        return self['referencedKeyIdGetterMap']

    def itemIdFromChild(self, relatedKey, relatedItem):
        return self.referencedKeyIdGetterMap[relatedKey](relatedItem)

    @property
    def referencedIdsGetterMap(self):
        return self['referencedIdsGetterMap']

    async def referencedIds(self, relatedKey, item):
        referencedIdsGetter = self.referencedIdsGetterMap[relatedKey]
        ids = referencedIdsGetter(item, relatedKey)
        if iscoroutine(ids):
            ids = await ids
        return ids

    @classmethod
    def createReferencedGetter(cls, storee, id) -> callable:

        async def helper():
            child = await storee.get(id)
            return child

        return helper

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyReferencedGetter(self, item):
        if item is None:
            return None
        res = {
            **item,
            **{
                key: await item[key]()
                for key in self['referencedKeyStoreeMap']
            }
        }
        return res

    async def process(self, queueItems, T=None):

        async def transform(item):
            item = item if T is None else await T(item)
            for relatedKey, relatedStoree in self['referencedKeyStoreeMap'].items():
                referencedIds = await self.referencedIds(relatedKey, item)
                item[relatedKey] = self.createReferencedGetter(
                    relatedStoree, referencedIds)

            return item

        queueItems = await super().process(queueItems, T=transform)
        return queueItems

    async def get(self, *args, **kwargs) -> list[dict]:
        items = await super().get(*args, **kwargs)
        return await self.applyReferencedGetter(items)

    async def _saveOne(self, item: any):
        referencedKeyStoreeMap = self['referencedKeyStoreeMap']

        savedItem = self.cloneItem(
            R.omit(referencedKeyStoreeMap.keys(), item))
        savedItem = await super()._saveOne(savedItem)

        for relatedKey, relatedStoree in referencedKeyStoreeMap.items():
            if relatedKey in item:
                savedItem[relatedKey] = await relatedStoree.save(item[relatedKey])

        self.forgetItem(self.itemId(item))
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
