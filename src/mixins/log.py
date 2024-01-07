import logging
from random import random

from contrib.pyas.src.pyas_v3 import Leaf
from contrib.pyas.src.pyas_v3 import T


class Log(Leaf):

    prototypes = []

    columnSpecs = {
        'logPrefix': {
            'transformer': T.writableEmpty(lambda val, key, classee: val if key in classee.row else f"[{classee.informativeClassId()}] "),
        },
    }

    logging = logging

    def log(self, level, msg, *args, p=None, **kwargs):

        if not hasattr(self.logging, level):
            self.logging.error('Invalid log level {}:'.format(level))
            self.logging.error(*args, **kwargs)
            return

        if p is not None and random() > p:
            return

        l = getattr(self.logging, level)
        l(self.logPrefix+msg, *args, **kwargs)

    @property
    def logPrefix(self) -> str:
        return self['logPrefix']
