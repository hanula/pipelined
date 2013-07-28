
import unittest
from nose.tools import assert_equal


class TestChunk(unittest.TestCase):

    def call_FUT(self, size, iterable):
        from pipelined import chunked
        stream = iter(iterable)
        return chunked(size)(stream)

    def test_it_chunks_the_stream(self):
        chunks = self.call_FUT(2, range(5))
        assert_equal(list(chunks),
                     [[0, 1], [2, 3], [4]])

    def test_it_works_with_empty_stream(self):
        chunks = self.call_FUT(2, [])
        assert_equal(list(chunks), [])

    def test_it_chunks_smaller_stream_than_size(self):
        chunks = self.call_FUT(10, range(3))
        assert_equal(list(chunks), [[0, 1, 2]])


class TestTee(unittest.TestCase):

    def call_FUT(self, pipelines, iterable):
        from pipelined import tee
        return tee(*pipelines)(iterable)

    def _stream_logger(self, log_store):
        def logger(stream):
            for item in stream:
                log_store.append(item)
                yield item
        return logger

    def test_it_clones_the_stream(self):
        logged = []
        pipelines = [
            [self._stream_logger(logged)],
            [self._stream_logger(logged)],
        ]
        stream = self.call_FUT(pipelines, range(3))
        list(stream)
        assert_equal(logged, [0, 0, 1, 1, 2, 2])


class TestContext(unittest.TestCase):

    def call_FUT(self, stream):
        from pipelined import context
        return context(stream)

    def test_it_return_empty_dict(self):
        assert_equal(self.call_FUT(None), {})
        assert_equal(self.call_FUT(range(3)), {})
