from json import dumps

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymap.src.async_map import map as p4thmap

from .relationstore import RelationStore


class LinksOneStore(Leaf):

    class LinksOneStoreError(Exception):
        pass

    prototypes = [RelationStore]

    columnSpecs = {
        'linksOneKeyStoreeMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getLinksOneKeyStoreeMap()),
        },

    }

    @staticmethod
    def onNew(cls, self):
        for _, childStoree in self['linksOneKeyStoreeMap'].items():
            childStoree.registerEventCallback(
                lambda *args, **kwargs: self.relationOnStoreEvent(childStoree, *args, **kwargs))

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

    async def process(self, ids, T=None):

        async def transform(item):
            for key, linkeeStoree in self['linksOneKeyStoreeMap'].items():
                linkeeId = item[key]
                self.updateForeignIdIdMap(self.itemId(item), [linkeeId])
                item[key] = self.createLinkeeGetter(linkeeStoree, linkeeId)

            return item if T is None else await T(item)

        items = await super().process(ids, T=transform)
        return items

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
        return savedItem
