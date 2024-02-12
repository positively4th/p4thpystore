from __future__ import annotations

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as async_p4thmap

from ..store import Store


class RelationStore(Leaf):

    class RelationStoreError(Store.StoreError):
        pass

    prototypes = []

    columnSpecs = {
        'relationForeignIdIdMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else {}),
        }
    }

    @async_p4thmap(isScalar=(0, 'isItem'), inputArgNr=1)
    @Store.makeEventEmitter(Store.EventType.SAVE)
    async def save(self, item, *args, **kwargs):
        oldItem = self['idGetter'](item)
        oldItem = await self.get(oldItem)
        newItem = await self._saveOne(item, *args, **kwargs)
        if oldItem is not None:
            self.forgetItem(self['idGetter'](oldItem))
        self.forgetItem(self['idGetter'](newItem))
        return newItem, oldItem

    def relationOnStoreEvent(self, childStoree, event: Store.Event, *args, **kwargs):

        if event['type'] == Store.EventType.SAVE:
            if event['new'] is None:
                return
            childId = childStoree.itemId(event['new'])

            childIdItemIdMap = self['relationForeignIdIdMap']

            itemIds = childIdItemIdMap[childId] if childId in childIdItemIdMap else None
            if itemIds is None:
                return
            self.forgetItem(itemIds)
            childIdItemIdMap.pop(childId, None)

        if event['type'] == Store.EventType.DELETE:
            childId = childStoree.itemId(event['old'])

            childIdItemIdMap = self['relationForeignIdIdMap']

            itemIds = childIdItemIdMap[childId] if childId in childIdItemIdMap else None
            if itemIds is None:
                return
            self.forgetItem(itemIds)
            childIdItemIdMap.pop(childId, None)

    def updateForeignIdIdMap(self, itemId, childrenIds):
        childIdItemIds = self['relationForeignIdIdMap']
        for childId in childrenIds:
            itemIds = childIdItemIds[childId] \
                if childId in childIdItemIds else set()
            itemIds = itemIds.union(set((itemId,)))
            childIdItemIds[childId] = itemIds
