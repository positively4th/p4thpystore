from __future__ import annotations
from json import dumps
import ramda as R
from inspect import iscoroutine

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from src.store import Store


class ReferencedStore(Leaf):

    class ReferencedStoreError(Store.StoreError):
        pass

    prototypes = []

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
                lambda *args, **kwargs: self.onStoreEvent(relatedKey, *args, **kwargs))

    def onStoreEvent(self, relatedKey, event: Store.Event, storee: Store):
        itemId = self.itemIdFromChild(relatedKey, event['item'])
        self.forgetItem(itemId)

    @property
    def referencedKeyIdGetterMap(self):
        return self['referencedKeyIdGetterMap']

    def itemIdFromChild(self, relatedKey, relatedItem):
        return self.referencedKeyIdGetterMap[relatedKey](relatedItem)

    @property
    def referencedIdsGetterMap(self):
        return self['referencedIdsGetterMap']

    async def relatedIds(self, relatedKey, item):
        relatedIdsGetter = self.referencedIdsGetterMap[relatedKey]
        ids = relatedIdsGetter(item, relatedKey)
        if iscoroutine(ids):
            ids = await ids
        return ids

    @classmethod
    def createRelatedGetter(cls, storee, id) -> callable:

        async def helper():
            child = await storee.get(id)
            return child

        return helper

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyRelatedGetter(self, item):
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
                relatedIds = await self.relatedIds(relatedKey, item)
                item[relatedKey] = self.createRelatedGetter(
                    relatedStoree, relatedIds)

            return item

        queueItems = await super().process(queueItems, T=transform)
        return queueItems

    async def get(self, *args, **kwargs) -> list[dict]:
        items = await super().get(*args, **kwargs)
        return await self.applyRelatedGetter(items)

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
