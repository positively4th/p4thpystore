from json import dumps

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from src.store import Store


class LinksOneStore(Leaf):

    class LinksOneStoreError(Exception):
        pass

    prototypes = []

    columnSpecs = {
        'linksOneKeyStoreeMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinksOneKeyStoreeMap()),
        },

    }

    @classmethod
    def getLinksOneKeyStoreeMap():
        raise LinksOneStore('getLinksOneKeyStoreeMap not implemented')

    @classmethod
    def createLinkeeGetter(cls, storee, id) -> callable:

        async def helper():
            linkee = await storee.get(id)
            return linkee

        return helper

    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=1, jsonFallbackMap={dumps(None): None})
    async def applyLinkeeGetter(self, item):
        if item is None:
            return None
        res = {
            **item,
            **{
                key: await item[key]()
                for key in self['linksOneKeyStoreeMap']
            }
        }
        return res

    async def process(self, queueItems, T=None):

        async def transform(item):
            item = item if T is None else await T(item)
            for key, linkeeStoree in self['linksOneKeyStoreeMap'].items():
                linkeeId = item[key]
                item[key] = self.createLinkeeGetter(linkeeStoree, linkeeId)

            return item

        queueItems = await super().process(queueItems, T=transform)
        return queueItems

    async def get(self, *args, **kwargs) -> list[dict]:
        items = await super().get(*args, **kwargs)
        return await self.applyLinkeeGetter(items)

    async def _saveOne(self, item: any):
        linksOneKeyStoreeMap = self['linksOneKeyStoreeMap']
        saveItem = self.cloneItem(item)
        savedItem = {}
        for linkeeKey, linkeeStoree in linksOneKeyStoreeMap.items():
            savedLinkee = (await linkeeStoree.save([item[linkeeKey]]))[0]
            savedItem[linkeeKey] = savedLinkee
            saveItem[linkeeKey] = linkeeStoree.itemId(savedLinkee)
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
