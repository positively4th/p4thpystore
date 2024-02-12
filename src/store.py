from __future__ import annotations
import ramda as R
import asyncio
import threading
import logging
from typing import *
from enum import Enum

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.pyas.src.pyas_v3 import As
from contrib.p4thpymap.src.map import map as p4thmap
from contrib.p4thpymap.src.async_map import map as async_p4thmap
from contrib.p4thpymisc.src.misc import clone

from .queue.queueitem import QueueItem
from src.mixins.log import Log
from src.mixins.contextlogger import ContextLogger


class Store():

    class StoreError(Exception):
        pass

    class EventType(Enum):
        SAVE = 1
        DELETE = 2

    Event: Dict = frozenset({
        'type': EventType,
        'id': str,
        'new': any,
        'old': any
    })

    type Events = List[Event]

    type EventListener = Callable[[List[Event], Leaf], None]

    @staticmethod
    def makeShapeAgnostic(method):

        async def helper(self, ids: str | int | float | tuple | list | set | dict, *args, **kwargs):

            if isinstance(ids, str):
                item = await method(self, [ids], *args, **kwargs)
                return item[ids]

            if isinstance(ids, dict):
                uniqIds = R.pipe(
                    R.values,
                    R.uniq
                )(ids)
                items = await method(self, uniqIds, *args, **kwargs)

                res = {
                    key: items[id] if id in items else None
                    for key, id in ids.items()
                }
                return res

            items = await method(self, ids, *args, **kwargs)
            return ids.__class__([items[id] if id in items else None for id in ids])

        return helper

    @staticmethod
    def makeEventEmitter(eventType: Store.EventType):

        def argHelper(method: callable) -> callable:

            async def methodHelper(self, *args, **kwargs):
                new, old = await method(self, *args, **kwargs)
                self.runEventCallbacks(eventType, new, old)
                return old if new is None else new

            return methodHelper

        return argHelper

    async def select(self, ids: list) -> list:
        raise Store.StoreError('select Not implemented')

    async def _saveOne(self, item: any):
        raise Store.StoreError('_saveOne Not implemented')

    async def _deleteOne(self, item: any):
        raise Store.StoreError('_deleteOne Not implemented')

    @async_p4thmap(isScalar=(0, 'isItem'), inputArgNr=1)
    async def transform(self, item, T=None) -> dict:
        return await T(item) if T is not None else item

    @staticmethod
    def ensureUnique(items: list) -> any:
        if len(items) != 1:
            raise Store.StoreError(f'Multiple items with same id.')
        return items[0]

    async def process(self, ids, T=None):

        idItemMap = {
            id: None for id in ids
        }

        items = [
            await self.transform(row, T)
            for row in await self.select(ids)
        ]
        idItemMap.update(R.pipe(
            R.group_by(
                lambda item: self['idGetter'](item)
            ),
            R.map(self.ensureUnique)
        )(items))

        self.setItem(idItemMap)

        return idItemMap

    @staticmethod
    def createIsDictWithId(self: Store, noneResult: bool | None = None) -> bool:

        def isDictWithId(item: any) -> bool:

            if noneResult is not None:
                if item is None:
                    return noneResult

            try:
                self.itemId(item)
            except (TypeError, KeyError):
                return False
            except Exception as e:
                print(e)
                return False
            return True

        return isDictWithId

    @classmethod
    def createKeyIdGetter(cls, key: str = None) -> callable:

        _key = key
        _key = cls.idKey if _key is None and hasattr(cls, 'idKey') else _key
        _key = 'id' if _key is None else _key

        def keyGetter(item: dict) -> any:
            return item[_key]

        return keyGetter

    idCtr = 0

    prototypes = [
        Log, *Log.prototypes
    ]

    batchSize = 100
    batchDelay = 0.01

    columnSpecs = {
        'loadedMap': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else {}),
        },
        'pendingMap': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else {}),
        },
        'batchSize': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else classee.batchSize),
        },
        'batchDelay': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else classee.batchDelay),
        },
        'eventCallbacks': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else {}),
        },
        'idGetter': {
            'transformer': T.writableEmpty(lambda val, key, classee: classee.createKeyIdGetter()),
        },
        'isItemTester': {
            'transformer': T.writableEmpty(lambda val, key, classee: classee.createIsDictWithId(classee, noneResult=True))
        },
    }

    def itemId(self, item):
        return self['idGetter'](item)

    def isItem(self, item) -> bool:
        return self['isItemTester'](item)

    def isItemId(self, itemId) -> bool:
        return isinstance(itemId, (str, int, float))

    def setItem(self, item):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot set, locked!')
        #     pass

        if isinstance(item, (list, tuple)):
            item = R.pipe(
                R.group_by(lambda item: self['idGetter'](item)),
                R.map(lambda items: self.ensureUnique(items))
            )(item)
        if not isinstance(item, dict):
            item = {self['idGetter'](item): item}
        with self.loadedMapLock:
            self['loadedMap'].update(item)
        return item

    def getItem(self, itemId):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot get, locked!')
        #     pass

        with self.loadedMapLock:
            res = self.cloneItem(self['loadedMap'][itemId]) \
                if itemId in self['loadedMap'] else None
            return self.cloneItem(res)

    def itemExists(self, itemId):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot get, locked!')
        #     pass
        with self.loadedMapLock:
            return itemId in self['loadedMap']

    def forgetItem(self, itemIds):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot forget, locked!')
        #     pass

        _itemIds = itemIds if isinstance(
            itemIds, (list | tuple | set)) else [itemIds]
        with self.loadedMapLock:
            for itemId in _itemIds:
                self['loadedMap'].pop(itemId, None)
        self.log('info', f'{self.informativeClassId()} dropped item {
            ', '.join(_itemIds)}')

    @classmethod
    def debug(cls):
        logging.basicConfig(
            level=logging.DEBUG,  # <-- update to DEBUG
            format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )

    @classmethod
    def onNew(cls, self):
        self.queueTask = None
        self.producer = asyncio.Queue()
        self.pendingMapLock = threading.Lock()
        self.loadedMapLock = threading.Lock()

    @classmethod
    def cloneItem(cls, item: any, sortComparator: callable | None = None) -> any:

        return clone(item)

    def deliveItems(self, idItemsMap):
        for id, item in idItemsMap.items():
            self.log('info',
                     'item {} added to loaded map'.format(id))
            with self.pendingMapLock:
                if not id in self['pendingMap']:
                    continue
                qs = self['pendingMap'].pop(id)
            for q in qs:
                q.put_nowait({
                    '_sid': id,
                    'result': item
                })
                self.log('info', 'item {} delivered'.format(id))

    def start(self):

        async def helper(name):

            async def retryGet(delay=0, retries=1):
                try:
                    if self.__class__.__name__.find('EventStore_'):
                        pass

                    return self.producer.get_nowait()
                except asyncio.QueueEmpty as e:
                    if retries > 0:
                        await asyncio.sleep(delay)
                        return await retryGet(retries=retries-1)
                    # self.log('error', 'Empty queue: {}'.format(e))
                return None

            async def waitForSeveral():

                items = []
                while len(items) < self['batchSize']:
                    # try:
                    item = await retryGet(self['batchDelay'], retries=1)
                    if item is None:
                        if len(items) > 0:
                            break
                    else:
                        items.append(item)

                if self.__class__.__name__.find('EventStore_'):
                    pass
                return items

            while True:
                items = await waitForSeveral()

                self.log('info', '{} is processing'.format(name))
                try:
                    idItemMap = await ContextLogger.asLogged(
                        self.process,
                        tag=f"{self.process.__name__}:{
                            self.informativeClassId()}",
                        resultHandler=ContextLogger.countResultHandler)([As(QueueItem)(item)['_sid'] for item in items])
                except Exception as e:
                    idItemMap = {item['_sid']: e for item in items}
                self.deliveItems(idItemMap)

        assert self.queueTask is None

        name = self.__class__.__name__
        self.queueTask = asyncio.create_task(
            helper(name), name=name)
        self.log('info', 'Started store', name)

    def ensureOpen(self):
        if self.queueTask is not None:
            return
        self.start()

    async def waitFor(self, _sid: str) -> list[dict]:

        with self.pendingMapLock:
            qs = self['pendingMap'][_sid] if _sid in self['pendingMap'] else []
            q = asyncio.Queue()
            qs.append(q)
            self['pendingMap'][_sid] = qs

        self.log('info', 'adding {} to queue.'.format(_sid))
        if len(qs) == 1:
            await self.producer.put({
                '_sid': _sid,
            })
        return await q.get()

    async def getList(self, ids: list, **kwArgs) -> list[dict]:

        self.ensureOpen()

        pendingIds = []
        res = {}
        for id in ids:
            if self.itemExists(id):
                res[id] = self.getItem(id)
            else:
                pendingIds.append(id)

        res.update(await ContextLogger.asLogged(self.loadNewTG, resultHandler=ContextLogger.countResultHandler)(pendingIds))
        return res

    @makeShapeAgnostic
    async def get(self, *args, **kwargs):
        return await self.getList(*args, **kwargs)

    @async_p4thmap(isScalar=(0, 'isItem'), inputArgNr=1)
    @makeEventEmitter(EventType.SAVE)
    async def save(self, item, *args, **kwargs):
        oldItem = self['idGetter'](item)
        oldItem = await self.get(oldItem)
        newItem = await self._saveOne(item, *args, **kwargs)
        if oldItem is not None:
            self.forgetItem(self['idGetter'](oldItem))
        self.setItem([newItem])
        return newItem, oldItem

    @async_p4thmap(isScalar=(0, 'isItemId'), inputArgNr=1)
    @makeEventEmitter(EventType.DELETE)
    async def delete(self, itemId: str | float | int, *args, **kwargs):
        return None, await self._deleteOne(itemId, *args, **kwargs)

    def registerEventCallback(self, f: Store.EventListener) -> str:
        self.idCtr += 1
        res = f'''ecb_{str(self.idCtr)}'''
        self['eventCallbacks'][res] = f
        return res

    # inputArgNr is used as argument for isItem.
    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=2)
    def runEventCallbacks(self, eventType: Store.EventType, new: any | None, old: any) -> None:
        id = self['idGetter'](old if new is None else new)
        event = {
            'type': eventType,
            'id': id,
            'new': new,
            'old': old
        }
        for id, f in self['eventCallbacks'].items():
            try:
                f(event, self)
            except Exception as e:
                print(f'runEventCallbacks: {f}', e)

    async def loadNewTG(self, pendingIds):
        res = {}
        tasks = []
        async with asyncio.TaskGroup() as taskGroup:
            for _sid in pendingIds:
                tasks.append(taskGroup.create_task(self.waitFor(_sid)))
        es = []
        for qi in tasks:
            qi = qi.result()
            r = qi['result']
            if isinstance(r, Exception):
                es.append(r)
            else:
                res[qi['_sid']] = r
        for e in es:
            raise e
        return res
