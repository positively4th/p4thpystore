from __future__ import annotations
from json import dumps
import ramda as R

from contrib.p4thpymisc.src.misc import createCaster

from src.store import Store


class StoreTestTools:

    def createEventListenerAndFlusher():

        orderedEvents = []

        def listener(event: Store.Event, store):
            orderedEvents.append((event, store))

        def flusher():
            res = []
            while len(orderedEvents) > 0:
                res.append(orderedEvents.pop(0))
            return res

        return listener, flusher

    @staticmethod
    def _srt(thing):
        return StoreTestTools.keySort(thing)

    @staticmethod
    def keySort(iterable: any) -> any:

        if isinstance(iterable, str):
            return iterable

        caster = createCaster(iterable)

        keys = None
        items = iterable
        def T(x): return x
        try:
            if keys == None:
                keys = iterable.keys()
        except Exception as e:
            pass

        try:
            if keys == None:
                items = {
                    dumps(i, sort_keys=True): i for i in iterable
                }
                keys = items.keys()
                def T(x): return list(x.values())
        except TypeError as e:
            pass
        except Exception as e:
            print(e)

        if keys == None:
            return iterable

        try:
            res = R.pipe(
                R.sort_by(lambda k: k),
                R.map(lambda k: (k, StoreTestTools.keySort(items[k]))),
                R.from_pairs,
            )(keys)
            return caster(T(res))
        except Exception as e:
            print(e)

        return iterable
