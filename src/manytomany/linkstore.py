from __future__ import annotations
import ramda as R
from enum import Enum
from typing import *

from contrib.pyas.src.pyas_v3 import T
from contrib.pyas.src.pyas_v3 import As

from ..store import Store

# a-facetItem -N-> linkItem <-N- another-facetItem


class LinkStore:

    @staticmethod
    def updateSet(res, key, vals):
        res[key] = res[key] if key in res else set()
        res[key] = res[key].union(vals)
        return res

    class Many2ManyLinkStoreError(Store.StoreError):
        pass

    class FacetStore():

        columnSpecs = {
            'facetIdIdsMap': {
                'transformer': T.constantNotEmpty(),
            },
            'linkStoree': {
                'transformer': T.constantNotEmpty(),
            },
            'select': {
                'transformer': T.constantNotEmpty(),
            },
        }

        async def select(self, ids: list | tuple) -> list:
            return await self['select'](ids)

        async def process(self, ids, T=None):
            linkStoree = self['linkStoree']

            idNoneMap = {
                id: None for id in ids
            }

            rows = await self.select(ids)
            items = await linkStoree.transform(rows, T)
            idItemsMap = R.group_by(
                lambda item: linkStoree['idGetter'](item))(items)
            linkStoree.setItem(idItemsMap)
            linkStoree.deliveItems(idItemsMap)

            facetIdItemsMap = R.group_by(
                lambda item: self['idGetter'](item))(items)
            self['facetIdIdsMap'].update(facetIdItemsMap)

            return {**idNoneMap, **facetIdItemsMap}

        @Store.makeShapeAgnostic
        async def get(self, facetIds: list | tuple, *args, **kwArgs) -> list:

            linkStoree = self['linkStoree']

            existingIds = R.intersection(
                facetIds, self['facetIdIdsMap'].keys()
            )
            missingIds = R.difference(facetIds, existingIds)

            missingIdItemsMap = {}
            if len(missingIds) > 0:
                missingIdItemsMap = await super().getList(missingIds, *args, **kwArgs)

            ids = R.pipe(
                R.map(lambda facetId: tuple(
                    self['facetIdIdsMap'][facetId])
                ),
                R.unnest
            )(existingIds)

            idItemsMap = await linkStoree.getList(ids, *args, **kwArgs)
            facetIdItemsMap = R.pipe(
                R.values,
                R.group_by(lambda item: self['idGetter'](item))
            )(idItemsMap)
            facetIdItemsMap.update(missingIdItemsMap)
            return R.map(lambda items: [] if items is None else items)(facetIdItemsMap)

        def handleLinkStoreEvent(self, event: Store.Event, *args, **kwargs):
            # print(f'{self.informativeClassId()}: {event}')

            oldfacetId = self['idGetter'](event['old']) \
                if event['old'] is not None else None

            newfacetId = self['idGetter'](event['new']) \
                if event['new'] is not None else None

            if oldfacetId is not None:
                self['facetIdIdsMap'].pop(oldfacetId, None)

            if event['type'] == Store.EventType.SAVE:
                self['facetIdIdsMap'][newfacetId] = self['facetIdIdsMap'][newfacetId] \
                    if newfacetId in self['facetIdIdsMap'] else set()
                self['facetIdIdsMap'][newfacetId].add(event['id'])

            if event['type'] == Store.EventType.DELETE:
                if newfacetId in self['facetIdIdsMap']:
                    self['facetIdIdsMap'][newfacetId].remove(oldfacetId)

        async def save(self, *args, **kwargs):

            linkStoree = self['linkStoree']
            return await linkStoree.save(*args, **kwargs)

        async def _deleteOne(self, facetId: any):
            linkStoree = self['linkStoree']

            oldFacets = await self.get(facetId)

            if facetId in self['facetIdIdsMap']:
                # sets are not hashable
                await linkStoree.delete(tuple(self['facetIdIdsMap'][facetId]))

            return oldFacets

    prototypes = []

    columnSpecs = {}

    def createFacetStoree(self, idGetter: callable, select: callable) -> dict:

        with self.loadedMapLock:
            facetIdIdsMap = {}
            for id, item in self['loadedMap'].items():
                if item is None:
                    continue
                LinkStore.updateSet(
                    facetIdIdsMap, facetStoree['idGetter'](item), set((id,)))
        facetStoree = As(LinkStore.FacetStore, *self.prototypes)({
            'idGetter': idGetter,
            'select': lambda *args, **kwargs: select(*args, **kwargs),
            'linkStoree': self,
            'facetIdIdsMap': facetIdIdsMap,
        })

        self.registerEventCallback(
            lambda *args, **kwargs:
            facetStoree.handleLinkStoreEvent(*args, **kwargs)
        )

        return facetStoree
