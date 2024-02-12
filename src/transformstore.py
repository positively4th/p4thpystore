from inspect import iscoroutine
from json import loads, dumps

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T

from .store import Store


class TransformStore(Leaf):

    class TransformStoreError(Store.StoreError):
        pass

    prototypes = []

    columnSpecs = {
        'keyTransformerMap': {
            'transformer': T.fallback(lambda val, key, classee: val if key in classee.row else classee.getKeyTransformerMap()),
        },

    }

    @classmethod
    def createJSONTransformer(cls):

        def T(value, item, self, inverse: bool):
            if value is None:
                return None
            return dumps(value) if inverse else loads(value)

        return T

    @classmethod
    def getTransformKeys(cls) -> list:
        return []

    @classmethod
    def getKeyTransformerMap(cls) -> callable:

        raise TransformStore.LinksManyStoreError(
            'getKeyTransformerMap not implemented.'
        )

    def transformApply(self, key, item, inverse=False):
        transformer = self['keyTransformerMap'][key]
        return transformer(item[key], item, self, inverse=inverse)

    async def process(self, ids, T=None):

        async def transform(item):
            for key in self['keyTransformerMap']:
                if key not in item:
                    continue
                value = self.transformApply(key, item)
                if iscoroutine(value):
                    value = await value
                item[key] = value
            return item if T is None else await T(item)

        return await super().process(ids, T=transform)

    async def _saveOne(self, item: any):
        keyTransformerMap = self['keyTransformerMap']
        saveItem = self.cloneItem(item)
        savedItem = {}
        for key, _ in keyTransformerMap.items():
            if key not in saveItem:
                continue
            saveItem[key] = self.transformApply(key, item, inverse=True)
            if iscoroutine(saveItem[key]):
                saveItem[key] = await saveItem[key]
        savedItem = (await super()._saveOne(saveItem))
        for key, _ in keyTransformerMap.items():
            if key not in savedItem:
                continue
            savedItem[key] = self.transformApply(key, savedItem, inverse=False)
            if iscoroutine(savedItem[key]):
                savedItem[key] = await savedItem[key]
        return savedItem

    async def _deleteOne(self, itemId: any):
        return await super()._deleteOne(itemId)
