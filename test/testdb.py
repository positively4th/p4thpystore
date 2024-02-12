from __future__ import annotations
import ramda as R

from contrib.pyas.src.pyas_v3 import T
from contrib.p4thpymisc.src.misc import clone


class TestDB:

    prototypes = [
    ]

    columnSpecs = {
        'rows': {
            'transformer': T.fallback(lambda val, key, classee: []),
        }
    }

    def query(self, whereIns: dict = None, predicate: callable = None) -> list:
        res = self['rows']
        if whereIns is not None:
            for key, whereIn in whereIns.items():
                res = R.filter(lambda item: item[key] in whereIn)(res)
        if predicate is not None:
            res = R.filter(predicate)(res)
        return R.map(clone)(res)

    def upsert(self, newItem: any, idGetter: callable):
        id = idGetter(newItem)
        for i, dbItem in enumerate(self['rows']):
            if idGetter(dbItem) == id:
                self['rows'].pop(i)
                break
        self['rows'].append(clone(newItem))

    def delete(self, itemId, idGetter: callable):
        for i, item in enumerate(self['rows']):
            if idGetter(item) == itemId:
                self['rows'].pop(i)
                break
