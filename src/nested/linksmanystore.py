from __future__ import annotations
from json import dumps

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from ..store import Store
from .relationstore import RelationStore


class LinksManyStore(Leaf):

    class LinksManyStoreError(Store.StoreError):
        pass

    prototypes = [RelationStore]

    columnSpecs = {
        'linksManyKeyStoreeMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinksManyKeyStoreeMap()),
        },
        'linksManyKeyIdGetterMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinksManyKeyIdGetterMap()),
        },
    }

    @classmethod
    def getLinksManyKeyStoreeMap(cls, childItem: any, store: Store) -> any:
        raise LinksManyStore.LinksManyStoreError(
            'getLinksManyKeyStoreeMap not implemented.')

    @classmethod
    def getLinksManyKeyIdGetterMap(cls) -> any:
        raise LinksManyStore.LinksManyStoreError(
            'getLinksManyKeyIdGetterMap not implemented.')

    @staticmethod
    def onNew(cls, self):
        for _, childStoree in self['linksManyKeyStoreeMap'].items():
            childStoree.registerEventCallback(
                lambda *args, **kwargs: self.relationOnStoreEvent(childStoree, *args, **kwargs))

    @property
    def linksManyKeyIdGetterMap(self):
        return self['linksManyKeyIdGetterMap']

    def itemIdFromChild(self, childrenKey, childItem):
        return self.linksManyKeyIdGetterMap[childrenKey](childItem)

    @classmethod
    def createChildrenGetter(cls, storee, id) -> callable:

        async def helper():
            child = await storee.get(id)
            return child

        return helper

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyChildrenGetter(self, item):
        if item is None:
            return None
        res = {
            **item,
            **{
                key: await item[key]()
                for key in self['linksManyKeyStoreeMap']
            }
        }
        return res

    async def process(self, queueItems, T=None):

        async def transform(item):
            for key, childStoree in self['linksManyKeyStoreeMap'].items():
                childrenIds = item[key]
                self.updateForeignIdIdMap(self.itemId(item), childrenIds)
                item[key] = self.createChildrenGetter(childStoree, childrenIds)

            return item if T is None else await T(item)

        queueItems = await super().process(queueItems, T=transform)
        return queueItems

    async def get(self, *args, **kwargs) -> list[dict]:
        items = await super().get(*args, **kwargs)
        return await self.applyChildrenGetter(items)

    async def _saveOne(self, item: any):
        linksManyKeyStoreeMap = self['linksManyKeyStoreeMap']
        saveItem = self.cloneItem(item)
        savedItem = {}
        for childrenKey, childrenStoree in linksManyKeyStoreeMap.items():
            savedChildren = (await childrenStoree.save(item[childrenKey]))
            savedItem[childrenKey] = savedChildren
            saveItem[childrenKey] = [
                childrenStoree.itemId(savedChild) for savedChild in savedChildren
            ]
        savedItem = {
            **(
                await super()._saveOne(saveItem)
            ),
            **savedItem
        }
        self.forgetItem(self.itemId(item))
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
