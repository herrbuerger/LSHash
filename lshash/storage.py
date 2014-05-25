# lshash/storage.py
# Copyright 2012 Kay Zhu (a.k.a He Zhu) and contributors (see CONTRIBUTORS.txt)
#
# This module is part of lshash and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import json
import zlib
import os

try:
    import redis
except ImportError:
    redis = None
     
try:
    import lmdb
except ImportError:
    lmdb = None

__all__ = ['storage']


def storage(storage_config, index):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        storage_config['redis']['db'] = index
        return RedisStorage(storage_config['redis'])
    elif 'lmdb' in storage_config:
        storage_config['lmdb']['db'] = str(index)
        return LMDBStorage(storage_config['lmdb'])
    else:
        raise ValueError("Only in-memory dictionary and Redis are supported.")


class BaseStorage(object):
    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def set_val(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def get_val(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def append_val(self, key, val):
        """ Append `val` to the list stored at `key`.

        If the key is not yet present in storage, create a list with `val` at
        `key`.
        """
        raise NotImplementedError

    def get_list(self, key):
        """ Returns a list stored in storage at `key`.

        This method should return a list of values stored at `key`. `[]` should
        be returned if the list is empty or if `key` is not present in storage.
        """
        raise NotImplementedError


class InMemoryStorage(BaseStorage):
    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()

    def keys(self):
        return self.storage.keys()

    def set_val(self, key, val):
        self.storage[key] = val

    def get_val(self, key):
        return self.storage[key]

    def append_val(self, key, val):
        self.storage.setdefault(key, []).append(val)

    def get_list(self, key):
        return self.storage.get(key, [])


class RedisStorage(BaseStorage):
    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = redis.StrictRedis(**config)
        seed = "[4.0, 36.0, 18.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0, 18.0, 75.0, 84.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 70.0, 144.0, 14.0, 15.0, 12.0, 1.0, 0.0, 0.0, 9.0, 24.0, 3.0, 3.0, 10.0, 2.0, 6.0, 81.0, 122.0, 2.0, 0.0, 0.0, 0.0, 0.0, 144.0, 144.0, 144.0, 50.0, 1.0, 4.0, 14.0, 17.0, 52.0, 9.0, 15.0, 49.0, 14.0, 81.0, 144.0, 40.0, 0.0, 0.0, 1.0, 6.0, 3.0, 15.0, 97.0, 55.0, 11.0, 16.0, 13.0, 0.0, 0.0, 0.0, 0.0, 3.0, 144.0, 100.0, 18.0, 0.0, 0.0, 0.0, 2.0, 98.0, 144.0, 12.0, 0.0, 0.0, 0.0, 11.0, 60.0, 50.0, 0.0, 0.0, 0.0, 0.0, 6.0, 9.0, 35.0, 20.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 93.0, 24.0, 0.0, 0.0, 0.0, 0.0, 0.0, 24.0, 71.0, 36.0, 0.0, 0.0, 0.0, 0.0, 1.0, 6.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 1.0, 0.0]"
        self.c = Compressor(seed)

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def set_val(self, key, val):
        self.storage.set(key, val)

    def get_val(self, key):
        return self.storage.get(key)

    def append_val(self, key, val):
        cval = self.c.compress(json.dumps(val))
        self.storage.rpush(key, cval)

    def get_list(self, key):
        result = [self.c.decompress(r) for r in self.storage.lrange(key, 0, -1)]
        return result


class LMDBStorage(BaseStorage):
    def __init__(self, config):
        if not lmdb:
            raise ImportError("lmdb is required to use lmdb as storage.")
        self.name = 'lmdb'
        self.env = lmdb.open(config['path'], map_size=1048576 * 1024 * 1024, max_dbs=4)
        self.storage = self.env.open_db(config['db'], dupsort=True)
        seed = "[4.0, 36.0, 18.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0, 18.0, 75.0, 84.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 70.0, 144.0, 14.0, 15.0, 12.0, 1.0, 0.0, 0.0, 9.0, 24.0, 3.0, 3.0, 10.0, 2.0, 6.0, 81.0, 122.0, 2.0, 0.0, 0.0, 0.0, 0.0, 144.0, 144.0, 144.0, 50.0, 1.0, 4.0, 14.0, 17.0, 52.0, 9.0, 15.0, 49.0, 14.0, 81.0, 144.0, 40.0, 0.0, 0.0, 1.0, 6.0, 3.0, 15.0, 97.0, 55.0, 11.0, 16.0, 13.0, 0.0, 0.0, 0.0, 0.0, 3.0, 144.0, 100.0, 18.0, 0.0, 0.0, 0.0, 2.0, 98.0, 144.0, 12.0, 0.0, 0.0, 0.0, 11.0, 60.0, 50.0, 0.0, 0.0, 0.0, 0.0, 6.0, 9.0, 35.0, 20.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 93.0, 24.0, 0.0, 0.0, 0.0, 0.0, 0.0, 24.0, 71.0, 36.0, 0.0, 0.0, 0.0, 0.0, 1.0, 6.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 1.0, 0.0]"
        self.c = Compressor(seed)

    def keys(self, pattern="*"):
        raise NotImplementedError

    def set_val(self, key, val):
        raise NotImplementedError

    def get_val(self, key):
        raise NotImplementedError

    def append_val(self, key, val):
        cval = self.c.compress(json.dumps(val))
        with self.env.begin(write=True, db=self.storage) as txn:
            txn.put(key, cval)

    def get_list(self, key):
        result = []
        with self.env.begin(db=self.storage) as txn:
            cursor = txn.cursor()
            if cursor.set_key(key):
                for data in cursor.iternext_dup():
                    result.append(self.c.decompress(data))
        return result

class Compressor(object):
    def __init__(self, seed):
        c = zlib.compressobj(9)
        d_seed = c.compress(seed)
        d_seed += c.flush(zlib.Z_SYNC_FLUSH)
        self.c_context = c.copy()

        d = zlib.decompressobj()
        d.decompress(d_seed)
        while d.unconsumed_tail:
            d.decompress(d.unconsumed_tail)
        self.d_context = d.copy()

    def compress(self, text):
        c = self.c_context.copy()
        t = c.compress(text)
        t2 = c.flush(zlib.Z_FINISH)
        return t + t2

    def decompress(self, ctext):
        d = self.d_context.copy()
        t = d.decompress(ctext)
        while d.unconsumed_tail:
            t += d.decompress(d.unconsumed_tail)
        return t