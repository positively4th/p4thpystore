from __future__ import annotations
from json import dumps
import ramda as R
from inspect import iscoroutine

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from ..store import Store
from .relationstore import RelationStore


class CollectStore(Leaf):

    class CollectStoreError(Store.StoreError):
        pass

    prototypes = [RelationStore]

    columnSpecs = {
        'collectKeyStoreesMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getCollectKeyStoreesMap()),
        },
        'collectKeyStoreeIdGetterMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getCollectKeyStoreeIdGetterMap()),
        },
        'collectIdsGetterMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getCollectIdsGetterMap()),
        }
    }

    @classmethod
    def getCollectKeyStoreesMap(cls, childItem: any, store: Store) -> any:
        raise CollectStore.CollectStoreError(
            'getCollectKeyStoreesMap not implemented.')

    @classmethod
    def getCollectKeyStoreeIdGetterMap(cls, childItem: any, store: Store) -> any:
        raise CollectStore.CollectStoreError(
            'getCollectKeyStoreeIdGetterMap not implemented.')

    @classmethod
    def getCollectIdsGetterMap(cls) -> any:
        raise CollectStore.CollectStoreError(
            'getCollectIdsGetterMap not implemented.')

    @staticmethod
    def onNew(cls, self):
        for childrenKey, childStorees in self.collectKeyStoreesMap.items():
            for childStoree in childStorees:
                childStoree.registerEventCallback(
                    lambda *args, **kwargs: self.relationOnStoreEvent(childStoree, *args, **kwargs))

    def itemIdFromCollectedChild(self, relatedKey, relatedItem):
        return self.getCollectKeyStoreeIdGetterMap()[relatedKey](relatedItem)

    @property
    def collectKeyStoreesMap(self):
        return self['collectKeyStoreesMap']

    @property
    def collectKeyStoreeIdGetterMap(self):
        return self['collectKeyStoreeIdGetterMap']

    @property
    def collectIdsGetterMap(self):
        return self['collectIdsGetterMap']

    async def collectChildIds(self, collectKey, item):
        childIdsGetter = self.collectIdsGetterMap[collectKey]
        ids = childIdsGetter(item, collectKey)
        if iscoroutine(ids):
            ids = await ids
        return ids

    def storeeFromChildItem(self, collectKey, childItem):
        storees = self.collectKeyStoreesMap[collectKey]
        storeeIdGetter = self.collectKeyStoreeIdGetterMap[collectKey]
        storeeId = storeeIdGetter(childItem)
        storee = R.find(lambda collectStoree: collectStoree.classId() == storeeId)(
            storees
        )
        return storee

    @classmethod
    def createChildrenGetter(cls, storees, id) -> callable:

        async def helper():
            res = []
            for storee in storees:
                res.append(await storee.get(id))
            return R.pipe(
                R.unnest,
                R.filter(lambda item: item is not None)
            )(res)

        return helper

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyChildrenGetter(self, item):
        if item is None:
            return None
        res = {
            **item,
            **{
                key: await item[key]()
                for key in self.collectKeyStoreesMap
            }
        }
        return res

    async def process(self, ids, T=None):

        async def transform(item):
            for collectKey, collectStorees in self.collectKeyStoreesMap.items():
                childrenIds = self.collectChildIds(collectKey, item)
                if iscoroutine(childrenIds):
                    childrenIds = await childrenIds
                self.updateForeignIdIdMap(self.itemId(item), childrenIds)
                item[collectKey] = self.createChildrenGetter(
                    collectStorees, childrenIds
                )
            return item if T is None else await T(item)

        ids = await super().process(ids, T=transform)
        return ids

    async def get(self, *args, **kwargs) -> list[dict]:
        items = await super().get(*args, **kwargs)
        return await self.applyChildrenGetter(items)

    async def _saveOne(self, item: any):
        collectKeyStoreesMap = self.collectKeyStoreesMap
        saveItem = dict(item)
        savedChildrenMap = {}
        for childrenKey in collectKeyStoreesMap.keys():
            children = saveItem.pop(childrenKey, [])
            savedChildren = []
            for child in children:
                childStoree = self.storeeFromChildItem(childrenKey, child)
                if not childStoree:
                    continue
                savedChild = await childStoree.save(child)
                savedChildren.append(savedChild)

            savedChildrenMap[childrenKey] = savedChildren
        savedItem = {
            **(
                await super()._saveOne(saveItem)
            ),
            **savedChildrenMap
        }
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
