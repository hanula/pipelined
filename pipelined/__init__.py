import types
import inspect
import itertools


class Pipeline:

    def __init__(self):
        pass

    def feed(self, sources):
        self


class Stream:

    def __init__(self, iter, context=None):
        self.iter = iter
        self.context = context

    def __iter__(self):
        return self.iter


class Context(dict):

    def __init__(self, pipeline, data=None):
        self.pipeline = pipeline
        super(Context, self).__init__()
        self.update(data)


def context(stream):
    return getattr(stream, 'context', {})


def chunked(size):

    def fn(stream):
        while True:
            chunk = list(itertools.islice(stream, size))
            if not chunk:
                break
            yield chunk
    return fn


def run(pipeline, data):

    def make_consumer(fn):
        if not inspect.isgeneratorfunction(fn):

            def gen(input):

                for i in input:

                    ret = fn(i)
                    assert not isinstance(ret, types.GeneratorType)
                    yield ret

            return gen
        return fn

    #pipeline, reducer = pipeline[:-1], pipeline[-1]
    #pipeline = [make_consumer(fn) for fn in pipeline] + [reducer]

    #pipeline = (fn if isinstance(types.GeneratorType, fn) else [fn]
    #            for fn in pipeline)
    #pipeline = itertools.chain.from_iterable(pipeline)

    def consume(dat):
        import time
        it = iter(dat)
        while True:
            try:
                d = next(it)
                yield d
            except StopIteration:
                return
            except Exception as exc:
                print("EXC", exc, type(exc))

        return

    pipeline = ([fn] if not isinstance(fn, (list, tuple)) else fn
                for fn in pipeline)
    pipeline = list(itertools.chain.from_iterable(pipeline))
    #print("NEW PIPE", pipeline)

    for fn in pipeline:
        data = consume(data)
        data = fn(Stream(data))

    return data


def parallel(*fns):

    import threading

    def consumer(input):
        for it in input:

            workers = [threading.Thread(target=fn, args=()) for fn in fns]

            for w in workers:
                yield it

    return consumer


def tee(*pipelines):

    def consumer(stream):
        for item in stream:
            for pipeline in pipelines:
                yield from run(pipeline, [item])

    return consumer
#
