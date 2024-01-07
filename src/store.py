from __future__ import annotations
from dill import loads, dumps
import ramda as R
import asyncio
import threading
import logging
import random
from typing import *
from enum import Enum

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T
from contrib.pyas.src.pyas_v3 import As
from contrib.p4thpymap.src.map import map as p4thmap
from contrib.p4thpymap.src.async_map import map as async_p4thmap
from src.queue.queueitem import QueueItem
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
        'item': any
    })

    type Events = List[Event]

    type EventListener = Callable[[List[Event], Leaf], None]

    @staticmethod
    def makeShapeAgnostic(method):

        def ensureUniq(items: list | tuple, id: str) -> any:
            if len(items) == 0:
                return None
            if len(items) > 1:
                raise Store.StoreError(f'''Id {id} is not unique.''')
            return items[0]

        async def helper(self, ids: str | int | float | tuple | list | dict, *args, **kwargs):

            if isinstance(ids, str):
                items = await method(self, [ids], *args, **kwargs)
                items = items[ids] if ids in items else []
                return ensureUniq(items, ids)

            if isinstance(ids, dict):
                uniqIds = R.pipe(
                    R.values,
                    R.uniq
                )(ids)
                items = await method(self, uniqIds, *args, **kwargs)

                res = {
                    key: ensureUniq(items[id], id)
                    for key, id in ids.items()
                }
                return res

            items = await method(self, ids, *args, **kwargs)
            return [ensureUniq(items[id] if id in items else [], id) for id in ids]

        return helper

    @staticmethod
    def makeEventEmitter(eventType: Store.EventType):

        def argHelper(method: callable) -> callable:

            async def methodHelper(self, *args, **kwargs):
                res = await method(self, *args, **kwargs)
                self.runEventCallbacks(eventType, res)
                return res

            return methodHelper

        return argHelper

    async def select(self, ids: list | tuple) -> list:
        raise Store.StoreError('select Not implemented')

    async def _saveOne(self, item: any):
        raise Store.StoreError('_saveOne Not implemented')

    async def _deleteOne(self, item: any):
        raise Store.StoreError('_deleteOne Not implemented')

    async def process(self, queueItems, T=None):

        async def _T(row):
            return row

        if T is not None:
            _T = T

        __ids = [As(QueueItem)(queueItem)['__id']
                 for queueItem in queueItems]
        rows = await self.select(__ids)

        for queueItem in queueItems:
            qi__id = queueItem['__id']
            queueItemee = As(QueueItem)(queueItem)
            queueItemee['result'] = [
                await _T(self.cloneItem(row)) for row in rows[qi__id]
            ] if qi__id in rows else []
            self.log('info', 'item {} ready to be delivered'.format(qi__id))
        return queueItems

    @staticmethod
    def createIsDictWithId(self: Store) -> bool:

        def isDictWithId(item: any) -> bool:
            try:
                self.idGetter(item)
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
            'transformer': T.writableEmpty(lambda val, key, classee: classee.createIsDictWithId(classee))
        },
    }

    def itemId(self, item):
        return self['idGetter'](item)

    def isItem(self, item) -> bool:
        return self['isItemTester'](item)

    def isItemId(self, itemId) -> bool:
        return not self.isItem(itemId) and isinstance(itemId, Hashable)

    def setItem(self, id, item):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot set, locked!')
        #     pass
        with self.loadedMapLock:
            res = self['loadedMap'].pop(id, None)
            self['loadedMap'][id] = item
        return res

    def getItem(self, itemId):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot get, locked!')
        #     pass
        with self.loadedMapLock:
            return self['loadedMap'].pop(itemId, None)

    def itemExists(self, itemId):
        # if self.loadedMapLock.locked():
        #     raise Exception('cannot get, locked!')
        #     pass
        with self.loadedMapLock:
            return itemId in self['loadedMap']

    def forgetItem(self, itemId):
        if self.loadedMapLock.locked():
            raise Exception('cannot forget, locked!')
            pass
        with self.loadedMapLock:
            return self['loadedMap'].pop(itemId, None)

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
    def cloneItem(cls, item: any) -> any:
        return loads(dumps(item))

    def start(self):

        def deliverItems(queueItems):
            for queueItem in queueItems:
                queueItemee = As(QueueItem)(queueItem)
                __id = queueItemee['__id']
                self.setItem(__id, queueItem['result'])
                # self['loadedMap'][__id] = queueItem['result']
                self.log('info',
                         'item {} added to loaded map'.format(__id))
                with self.pendingMapLock:
                    if not __id in self['pendingMap']:
                        continue
                    qs = self['pendingMap'].pop(__id)
                for q in qs:
                    q.put_nowait(queueItem)
                    self.log('info', 'item {} delivered'.format(__id))

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
                if self.__class__.__name__.find('EventStore_'):
                    pass
                while len(items) < self['batchSize']:
                    # try:
                    if self.__class__.__name__.find('EventStore_'):
                        pass
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
                    queueItems = await ContextLogger.asLogged(
                        self.process,
                        tag=f"{self.process.__name__}:{
                            self.informativeClassId()}",
                        resultHandler=ContextLogger.countResultHandler)(items)
                except Exception as e:
                    queueItems = [{**item, **{'result': e}} for item in items]
                deliverItems(queueItems)

        assert self.queueTask is None

        name = self.__class__.__name__
        self.queueTask = asyncio.create_task(
            helper(name), name=name)
        self.log('info', 'Started store', name)

    def ensureOpen(self):
        if self.queueTask is not None:
            return
        self.start()

    async def waitFor(self, __id: str) -> list[dict]:

        with self.pendingMapLock:
            qs = self['pendingMap'][__id] if __id in self['pendingMap'] else []
            q = asyncio.Queue()
            qs.append(q)
            self['pendingMap'][__id] = qs

        self.log('info', 'adding {} to queue.'.format(__id))
        if len(qs) == 1:
            await self.producer.put({
                '__id': __id,
            })
        return await q.get()

    @makeShapeAgnostic
    async def get(self, ids: tuple | list, forceLoad=False, **kwArgs) -> list[dict]:

        self.ensureOpen()

        pendingIds = []
        res = {}
        for id in ids:
            if self.itemExists(id) and not forceLoad:
                res[id] = self.getItem(id)
            else:
                pendingIds.append(id)

        res.update(await ContextLogger.asLogged(self.loadNewTG, resultHandler=ContextLogger.countResultHandler)(pendingIds))
        return res
        r = random.randint(0, 3)
        if r < 1:
            res.update(await ContextLogger.asLogged(loadNewTG, resultHandler=ContextLogger.countResultHandler)(pendingIds))
        elif r < 2:
            res.update(await ContextLogger.asLogged(loadNewGather, resultHandler=ContextLogger.countResultHandler)(pendingIds))
        else:
            res.update(await ContextLogger.asLogged(loadNewAsCompleted, resultHandler=ContextLogger.countResultHandler)(pendingIds))

        return res

    @makeEventEmitter(EventType.SAVE)
    @async_p4thmap(isScalar=(0, 'isItem'), inputArgNr=1)
    async def save(self, *args, **kwargs):
        return await self._saveOne(*args, **kwargs)

    @makeEventEmitter(EventType.DELETE)
    @async_p4thmap(isScalar=(0, 'isItemId'), inputArgNr=1)
    async def delete(self, *args, **kwargs):
        return await self._deleteOne(*args, **kwargs)

    def registerEventCallback(self, f: Store.EventListener) -> str:
        self.idCtr += 1
        res = f'''ecb_{str(self.idCtr)}'''
        self['eventCallbacks'][res] = f
        return res

    # inputArgNr is used as argument for isItem.
    @p4thmap(isScalar=(0, 'isItem'), inputArgNr=2)
    def runEventCallbacks(self, eventType: Store.EventType, item: dict) -> None:
        event = {
            'type': eventType,
            'id': self.itemId(item),
            'item': item
        }
        for id, f in self['eventCallbacks'].items():
            try:
                f(event, self)
            except Exception as e:
                print(e)

    async def loadNewTG(self, pendingIds):
        res = {}
        tasks = []
        async with asyncio.TaskGroup() as taskGroup:
            for __id in pendingIds:
                tasks.append(taskGroup.create_task(self.waitFor(__id)))
        es = []
        for qi in tasks:
            qi = qi.result()
            r = qi['result']
            if isinstance(r, Exception):
                es.append(r)
            else:
                res[qi['__id']] = r
        for e in es:
            raise e
        return res

    async def loadNewGather(self, pendingIds):
        res = {}
        try:
            coros = (self.waitFor(__id) for __id in pendingIds)
            cores = await asyncio.gather(
                *coros, return_exceptions=True)
        except Exception as e:
            self.log('error', e)
            raise e
        es = []
        for qi in cores:
            r = qi['result']
            if isinstance(r, Exception):
                es.append(r)
            else:
                res[qi['__id']] = r
        for e in es:
            raise e
        return res

    async def loadNewAsCompleted(self, pendingIds):
        res = {}
        tasks = [asyncio.create_task(self.waitFor(
            __id), name=__id) for __id in pendingIds]
        try:
            for task in asyncio.as_completed(tasks):
                qi = await task
                r = qi['result']
                res[qi['__id']] = r
            return res
        except Exception as e:
            print(e)
            raise (e)
