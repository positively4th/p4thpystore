from json import dumps
import ramda as R

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from .store import Store

# linkedItem -N-> linkItem <-M- linkedItem


class LinkerStore(Leaf):

    class LinkerStoreError(Store.StoreError):
        pass

    prototypes = []

    columnSpecs = {
        'linkerKeyLinkLinkeeStoreePairMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinkerKeyLinkLinkeeStoreePairMap()),
        },
        'linkerLinkLinkeeIdMapper': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinkerLinkLinkeeIdMapper()),
        },
    }

    @classmethod
    def getLinkerKeyLinkLinkeeStoreePairMap(cls):
        raise LinkerStore.LinkerStoreError(
            'getLinkerKeyLinkLinkeeStoreePairMap not implemented.')

    @classmethod
    def getLinkerLinkLinkeeIdMapper(cls):
        raise LinkerStore.LinkerStoreError(
            'getLinkerLinkLinkeeIdMapper not implemented.')

    @classmethod
    def createLinkerGetter(cls, linkStoree, linkLinkeeIdsMapper: callable, linkeeStoree, item) -> callable:

        async def linkerGetter():
            links = R.unnest(linkStoree.get([item]))
            linkeeIds = await linkLinkeeIdsMapper(links)
            linkees = await linkeeStoree.get(linkeeIds)
            return linkees

        return linkerGetter

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyLinkerGetter(self, item):
        if item is None:
            return None
        res = {
            **item,
            **{
                key: await item[key]()
                for key in self['linkerKeyLinkLinkeeStoreePairMap']
            }
        }
        return res

    async def process(self, queueItems, T=None):

        async def transform(item):
            item = item if T is None else await T(item)
            for key, storeePair in self['linkerKeyLinkLinkeeStoreePairMap'].items():
                linkStoree, linkeeStoree = storeePair
                item[key] = self.createLinkerGetter(
                    linkStoree, self['linkerLinkLinkeeIdsMapper'], linkeeStoree, item)

            return item

        queueItems = await super().process(queueItems, T=transform)
        return queueItems

    async def get(self, *args, **kwargs) -> list:
        items = await super().get(*args, **kwargs)
        return await self.applyLinkerGetter(items)

    async def _saveOne(self, item: any):

        savedItem = (await super()._saveOne(item))
        self.forgetItem(self.itemId(item))
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
