from inspect import iscoroutine

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T

from .store import Store


class PipeTransformStore(Leaf):

    class LinksManyStoreError(Store.StoreError):
        pass

    prototypes = []

    columnSpecs = {
        'pipeTransforms': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getPipeTransforms()),
        },

    }

    @classmethod
    def createExplodeTransformer(cls, implodedKey: str, explodedKeys: list | tuple) -> callable:

        def T(item, inverse: bool):

            if inverse:
                res = {**item}
                imploded = {
                    implodedKey: {
                        key: res.pop(key, None)
                        for key in explodedKeys if key in item
                    }
                }
                return {
                    **res,
                    **imploded
                }

            res = {
                **item
            }
            source = res.pop(implodedKey, None)
            if source is None:
                return res

            for key in explodedKeys:
                if not key in source:
                    continue
                res[key] = source[key]

            return res

        return T

    @classmethod
    def getPipeTransforms(cls) -> []:
        return []

    @property
    def pipeTransforms(self):
        return self['pipeTransforms']

    async def pipeTransform(self, item, inverse=False):
        for T in self.pipeTransforms:
            item = T(item, inverse=inverse)
            if iscoroutine(item):
                item = await item
        return item

    async def process(self, ids, T=None):

        async def transform(item):
            item = await self.pipeTransform(item)
            return item if T is None else await T(item)

        return await super().process(ids, T=transform)

    async def _saveOne(self, item: any):
        saveItem = await self.pipeTransform(item, inverse=True)
        savedItem = (await super()._saveOne(saveItem))
        savedItem = await self.pipeTransform(savedItem)
        self.forgetItem(self['idGetter'](item))
        self.setItem(item)
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
