from os import linesep
import threading
import datetime
from os import times
from numbers import Number
from time import sleep
from queue import Queue, Empty
from jsonpickle import loads, dumps

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T

from src.mixins.log import Log


class TimeLogFile(Leaf):

    prototypes = [
        Log, *Log.prototypes
    ]

    columnSpecs = {
        'db': {
            'transformer': T.fallback(lambda val, key, classee: '/tmp/timelog.txt'),
        },
        'getDBId': {
            'transformer': T.fallback(lambda val, key, classee: classee.getDBId),
        },
        'isActive': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else None),
        },
        'maxInsertRows': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else classee.maxInsertRows),
        },
        'nextRowWaitTime': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else classee.nextRowWaitTime),
        },
        'threadStarter': {
            'transformer': T.fallback(lambda val, key, classee: classee.threadStarter),
        },
        'inserter': {
            'transformer': T.fallback(lambda val, key, classee: classee.inserter),
        },
        'dbDeepCloner': {
            'transformer': T.fallback(lambda val, key, classee: classee.jsonDeepCloner),
        },
    }

    maxInsertRows = 250
    nextRowWaitTime = 1.0
    idMetaMap = {}

    def __destroy__(self):
        if 'db' not in self.row:
            return

        self['db'].close()

    @staticmethod
    def inserter(db, data):

        with open(db, "a+") as f:
            f.write(linesep)
            f.write(linesep + dumps(data, unpicklable=False))
            f.write(linesep)

    @staticmethod
    def jsonDeepCloner(o):
        return loads(dumps(o))

    @staticmethod
    def getDBId(db):
        res = None
        res = db if isinstance(db, (str, float, int)) else res
        res = db[res] if res is None and 'id' in db else res
        res = db.id if res is None and hasattr(
            db, 'id') and db.id is not callable else res
        res = db.id(res) if res is None and db.id is callable else res
        res = id(db) if res is None else res
        return res

    @staticmethod
    def threadStarter(self, db):
        res = {}

        res['db'] = db
        res['queue'] = Queue()
        res['dbOpQueue'] = Queue()

        thread = threading.Thread(
            target=TimeLogFile._startQueue,
            name=f"{TimeLogFile.__name__}: dbId",
            args=(
                res['queue'], res['dbOpQueue'], lambda *args, **kwargs: self['inserter'](
                    db, *args, **kwargs),
                self['maxInsertRows'],
                self['nextRowWaitTime'],
                lambda *args, **kwargs: self.log(*args, **kwargs)
            ), daemon=True)
        thread.start()
        res['thread'] = self['threadStarter']
        return res

    @property
    def isPending(self):
        return self.pendingDBOps > 0 or self.pendingRows > 0

    @property
    def pendingRows(self):
        res = 0
        for _, meta in self.idMetaMap.items():
            res += meta['queue'].qsize()
        return res

    @property
    def pendingDBOps(self):
        res = 0
        for _, meta in self.idMetaMap.items():
            res += meta['dbOpQueue'].qsize()
        return res

    @staticmethod
    def _startQueue(queue, dbOpQueue: Queue, inserter: callable,
                    maxInsertRows: int,
                    nextRowWaitTime: float,
                    logLike: callable = None):

        _logLike = (lambda level, msg, p: print(
            msg)) if logLike is None else logLike

        successes = 0
        fails = 0

        while True:
            rows = [queue.get()]
            dbOpQueue.put('insert')
            try:

                while len(rows) < maxInsertRows:
                    try:
                        rows.append(
                            queue.get(True, nextRowWaitTime))
                    except Empty:
                        break

                try:
                    inserter(rows)
                except Exception as e:
                    fails += len(rows)
                    _logLike(
                        'warning', f"Timelog fail: {str(e)}")
                    for row in rows:
                        queue.put(row)
                    sleep(1.0)
                successes += len(rows)
                _logLike('info',
                         f"Pending inserts: {queue.qsize()}. Successes/fails = {successes}/{fails}", p=0.1
                         )
            finally:
                dbOpQueue.get()

    @property
    def isActive(self):
        return self.row and (
            self['timeLogDB'] if self['isActive'] is None else self['isActive']
        )

    def getTimeLogQueue(self, db):
        cls = self.__class__
        dbId = self['getDBId'](db)
        if not dbId in cls.idMetaMap:
            cls.idMetaMap[dbId] = self['threadStarter'](self, db)

        return cls.idMetaMap[dbId]['queue']

    @staticmethod
    def onNewClass(cls):
        pass

    def _open(self, staticData: dict = {}, dataAdder: callable = lambda meta, result: {**meta, 'result': result}):

        return {
            'dataAdder': dataAdder,
            **staticData,
            't0': times()
        }

    def _close(self, data, res, error, dataAdder: callable = lambda meta, result: {**meta, 'result': result}):
        if not self.isActive:
            return

        ts1 = times()
        ts0 = data.pop('t0')
        for k in dir(ts1):
            if not isinstance(getattr(ts1, k), Number):
                continue
            data[k] = getattr(ts1, k) - getattr(ts0, k)

        data['jiff'] = datetime.datetime.now()
        data['error'] = error
        data = data.pop('dataAdder')(data, res)
        self._logDuration(data)

    def _logDuration(self, data: dict):

        if not self.isActive:
            return
        row = {
            'identifier': self.informativeClassId(),
            **data,
        }
        queue = self.getTimeLogQueue(self['db'])
        queue.put(row)
