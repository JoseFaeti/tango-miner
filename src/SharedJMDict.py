"""Read-only, memory-mapped JMdict data for spawned worker processes."""
import hashlib
import json
import mmap
import os
from pathlib import Path
import struct
import sys
import tempfile
import zlib

from src.JMDict import JMDict

_MAGIC = b'TMJDX001'
_PREFIX = struct.Struct('<8sIIQ32s')
_HEADER = struct.Struct('<8sIIQ32s32s')
_SLOT = struct.Struct('<IQ')
_LENGTHS = struct.Struct('<II')
_CACHE_LIMIT = 8192


def _identity(dictionary_hash):
    digest = bytes.fromhex(dictionary_hash)
    if len(digest) != 32:
        raise ValueError('Invalid JMdict hash')
    source = Path(sys.modules[JMDict.__module__].__file__).read_bytes() + Path(__file__).read_bytes()
    signature = hashlib.sha256(digest + sys.version.encode('utf-8') + source + _MAGIC).hexdigest()
    return digest, signature


def _valid(path, dictionary_digest):
    try:
        with path.open('rb') as stream:
            raw = stream.read(_HEADER.size)
            if len(raw) != _HEADER.size:
                return False
            magic, slots, count, length, expected, checksum = _HEADER.unpack(raw)
            if (magic != _MAGIC or expected != dictionary_digest or slots < 2
                    or slots & (slots - 1) or count > slots // 2
                    or length != path.stat().st_size
                    or length < _HEADER.size + slots * _SLOT.size):
                return False
            digest = hashlib.sha256(raw[:_PREFIX.size])
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
            return digest.digest() == checksum
    except (OSError, ValueError, struct.error):
        return False


def prepare_shared_index(dictionary_path, dictionary_hash, cache_dir):
    """Return a verified immutable index, creating it atomically when absent."""
    dictionary_digest, signature = _identity(dictionary_hash)
    directory = Path(cache_dir) / 'shared-jmdict-v1'
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (signature + '.bin')
    if _valid(path, dictionary_digest):
        return path
    from src.steps.MergeTokensStep.index import get_jmdict

    dictionary = get_jmdict(dictionary_path)
    count = len(dictionary.index)
    slots = 2
    while slots < max(2, count * 2):
        slots *= 2
    table = bytearray(slots * _SLOT.size)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w+b', dir=directory, delete=False) as stream:
            temporary = Path(stream.name)
            stream.seek(_HEADER.size + len(table))
            for form, entries in dictionary.index.items():
                key = form.encode('utf-8')
                payload = json.dumps(entries, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                hashed = zlib.crc32(key)
                index = hashed & (slots - 1)
                while _SLOT.unpack_from(table, index * _SLOT.size)[1]:
                    index = (index + 1) & (slots - 1)
                offset = stream.tell()
                _SLOT.pack_into(table, index * _SLOT.size, hashed, offset)
                stream.write(_LENGTHS.pack(len(key), len(payload)))
                stream.write(key)
                stream.write(payload)
            length = stream.tell()
            stream.seek(_HEADER.size)
            stream.write(table)
            prefix = _PREFIX.pack(_MAGIC, slots, count, length, dictionary_digest)
            digest = hashlib.sha256(prefix)
            stream.flush()
            stream.seek(_HEADER.size)
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
            stream.seek(0)
            stream.write(_HEADER.pack(_MAGIC, slots, count, length, dictionary_digest, digest.digest()))
        temporary.replace(path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return path


class SharedIndex:
    """Small private hash directory and shared, read-only mapped entry data."""
    def __init__(self, path, dictionary_hash):
        digest, _ = _identity(dictionary_hash)
        self.file = Path(path).open('rb')
        try:
            self.mapping = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
            magic, slots, count, length, expected, _ = _HEADER.unpack_from(self.mapping)
            if (magic != _MAGIC or expected != digest or length != len(self.mapping)
                    or slots < 2 or slots & (slots - 1) or count > slots // 2
                    or _HEADER.size + slots * _SLOT.size > length):
                raise ValueError('Invalid shared JMdict header')
            self.positions = {}
            view = memoryview(self.mapping)[_HEADER.size:_HEADER.size + slots * _SLOT.size]
            try:
                for hashed, offset in _SLOT.iter_unpack(view):
                    if not offset:
                        continue
                    previous = self.positions.get(hashed)
                    if previous is None:
                        self.positions[hashed] = offset
                    elif isinstance(previous, tuple):
                        self.positions[hashed] = previous + (offset,)
                    else:
                        self.positions[hashed] = (previous, offset)
            finally:
                view.release()
            self.cache = {}
        except BaseException:
            if hasattr(self, 'mapping'):
                self.mapping.close()
            self.file.close()
            raise

    def get(self, word, default=None):
        cached = self.cache.get(word)
        if cached is not None:
            return cached
        try:
            key = word.encode('utf-8')
        except UnicodeEncodeError:
            return default
        offsets = self.positions.get(zlib.crc32(key))
        if offsets is None:
            return default
        for offset in offsets if isinstance(offsets, tuple) else (offsets,):
            key_length, data_length = _LENGTHS.unpack_from(self.mapping, offset)
            begin = offset + _LENGTHS.size
            if self.mapping[begin:begin + key_length] == key:
                result = json.loads(self.mapping[begin + key_length:begin + key_length + data_length])
                if len(self.cache) >= _CACHE_LIMIT:
                    self.cache.clear()
                self.cache[word] = result
                return result
        return default

    def close(self):
        self.mapping.close()
        self.file.close()


def install_worker_index(index_path, dictionary_path, dictionary_hash):
    """Install the verified index only for the corresponding dictionary path."""
    import src.steps.MergeTokensStep.index as merger

    dictionary = JMDict.__new__(JMDict)
    dictionary.index = SharedIndex(index_path, dictionary_hash)
    dictionary.xml_path = Path(dictionary_path).expanduser().resolve()
    merger._jmdict, merger._jmdict_path = dictionary, dictionary.xml_path
