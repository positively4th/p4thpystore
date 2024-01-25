from __future__ import annotations

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T

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

    def relationOnStoreEvent(self, childStoree, event: Store.Event, *args, **kwargs):

        childId = childStoree.itemId(event['item'])
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
