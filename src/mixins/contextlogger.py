import asyncio
from json import dumps
from json import loads
from functools import wraps
from inspect import iscoroutinefunction
from contextvars import ContextVar
from contextvars import copy_context

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import As
from contrib.pyas.src.pyas_v3 import T

from src.mixins.timelogfile import TimeLogFile


class ContextLogger(Leaf):

    prototypes = [
        TimeLogFile, *TimeLogFile.prototypes
    ]

    columnSpecs = {
        'resultHandler': {
            'transformer': T.constant(lambda val, key, classee: val if key in classee.row else classee.noResultHandler)
        },
        'staticData': {
            'transformer': T.constant(lambda val, key, classee: val if key in classee.row else {}),
        },
        'tag': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else ''),
        },
        'tagPath': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else ''),
        },
        'errors': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else []),
        },
    }

    _cloneNr = 0

    def _deepClone(self, tag, resultHandler=None, staticData={}):
        res = {
            **{
                k: self[k]
                for k in self.columnSpecs.keys()
                if k != 'resultHandler'
            },
            'staticData': {**self['staticData'], **staticData},
        }
        res['isActive'] = self['isActive']
        res['tag'] = tag
        res['tagPath'] = self['tag']
        if self['tagPath'] != '':
            res['tagPath'] = f"{self['tagPath']}/{res['tagPath']}"
        res = loads(dumps(res))
        res['db'] = self['dbDeepCloner'](self['db'])
        res['resultHandler'] = self['resultHandler'] if resultHandler is None else resultHandler
        res['cloneNr'] = self.__class__._cloneNr
        self.__class__._cloneNr += 1
        return As(self.__class__)(res)

    @staticmethod
    def noLog(f, *args, **kwargs):
        return lambda *args, **kwargs: f(*args, **kwargs)

    @staticmethod
    def noResultHandler(data, result):
        return data

    @staticmethod
    def countResultHandler(data, result):
        count = 1
        try:
            count = 1 if isinstance(result, str) else len(result)
        except Exception as e:
            print(str(e))
        return {**data, **{'count': count}}

    @classmethod
    def onNew(cls, self):
        self['errors'] = []

    @classmethod
    def setContextLogger(cls, contextLogger):
        currentContextLogger.set(contextLogger)
        return contextLogger

    @classmethod
    def run(cls, contextLogger, f, *args, **kwargs):

        @wraps(f)
        def helper():
            ContextLogger.setContextLogger(contextLogger)
            return f(*args, **kwargs)

        ctx = copy_context()

        return ctx.run(helper)

    @classmethod
    def getContextLogger(cls):

        if currentContextLogger is not None:
            contextLogger = currentContextLogger.get()

        return cls._noContextLogger() if contextLogger is None else contextLogger

    @classmethod
    def _noContextLogger(cls):
        return As(ContextLogger)({
            'isActive': False
        })

    def addError(self, error: str):
        self['errors'].append(error)

    @classmethod
    def asLogged(cls, f,
                 tag=None,
                 doAwait=None,
                 resultHandler=None,
                 staticData=None):

        _staticData = {} if staticData is None else staticData
        _self = cls.getContextLogger()

        isAsync = doAwait if doAwait is not None else \
            iscoroutinefunction(f)

        if not _self.isActive:

            def noLog(*args0, **kwargs0):
                return f(*args0, **kwargs0)

            return noLog

        ctx = copy_context()

        if not isAsync:

            @wraps(f)
            def logSync(*args, **kwArgs):

                return ctx.run(cls._makeLog(_self, False, _staticData),
                               f,
                               f.__name__ if tag is None else tag,
                               _self.noResultHandler if resultHandler is None else resultHandler,
                               *args,
                               **kwArgs)

            return logSync

        @wraps(f)
        async def logASync(*args, **kwArgs):

            task = cls._makeLog(_self, True, _staticData)(
                f,
                f.__name__ if tag is None else tag,
                _self.noResultHandler if resultHandler is None else resultHandler,
                *args,
                **kwArgs)
            return await asyncio.create_task(task, name=f.__name__, context=ctx)

        return logASync

    @classmethod
    def _makeLog(cls, parentLogger, isAsync: bool, staticData):

        if not isAsync:
            def _logSync(f: callable,
                         tag: str,
                         resultHandler,
                         *args,
                         **kwArgs):

                cls.setContextLogger(parentLogger._deepClone(
                    tag=tag, resultHandler=resultHandler, staticData=staticData))
                logger = currentContextLogger.get()
                data = logger._before()
                try:
                    res = f(*args, **kwArgs)
                    data = resultHandler(data, res)
                    data = logger._after(data, None)
                    return res
                except Exception as e:
                    data = logger._after(data, str(e))
                    raise e

            return _logSync

        async def _logASync(f: callable,
                            tag: str,
                            resultHandler,
                            *args,
                            **kwArgs):

            cls.setContextLogger(parentLogger._deepClone(
                tag=tag, resultHandler=resultHandler, staticData=staticData))
            logger = currentContextLogger.get()
            data = logger._before()
            try:
                res = f(*args, **kwArgs)
                res = await res
                data = resultHandler(data, res)
                data = logger._after(data, None)
                return res
            except Exception as e:
                data = logger._after(data, str(e))
                raise e

        return _logASync

    def _before(self):
        return self._open({})

    def _after(self, data, error):
        self._close({
            **self['staticData'],
            'tag': self['tag'],
            'tagPath': self['tagPath'],
            **data
        }, None, error)


currentContextLogger = ContextVar(
    'CurrentContextLogger', default=ContextLogger._noContextLogger())
