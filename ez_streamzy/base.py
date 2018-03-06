"""
Code for the base entities that manage stream operators
that can act on a record of data.

The base classes are Extractor, Processor, Mapper, Reducer,
and Loader.
"""


class GetAllRetriever(object):
    def __init__(self, source, streamer):
        self.source = source
        self.started = False
        self.streamer = streamer

    def __iter__(self):
        if not self.started:
            self.started = True
            for record in self.source:
                self.streamer.process(record)
        return self.streamer.extract()


class StreamChain(object):
    def __init__(self, generator=None):
        self.generator = generator

    def chain(self, streamer):
        if not streamer.get_all:
            self.generator = (streamer.process(x) for x in self.generator)
        else:
            self.generator = (x for x in GetAllRetriever(self.generator, streamer))

    def __iter__(self):
        return self.generator

class Streamer(object):
    """
    Streamer is a base that all things inherit from
    so that chaining is easily done.
    """

    get_all = False

    def __init__(self):
        self._chain = StreamChain()

    def __iter__(self):
        return (x for x in self._chain)

    def __rshift__(self, other):
        self._chain.chain(other)
        return self

class Processor(Streamer):
    """
    Processor object which acts as a base for handling
    records in a streaming manner to process them using
    an arbitrary function.
    """
    def process(self, record):
        raise NotImplementedError


class Extractor(Streamer):
    """
    Extractor object which acts as a base for extracting data
    from a source.

    Attributes:
        source (iterable): An iterable source of data
    """
    def __init__(self, source):
        self._chain = StreamChain(source)


class Mapper(Streamer):
    """
    Mapper object that acts as a mapping base for taking
    records and mapping them out based on a single field
    value
    """

    get_all = True

    def process(self, record):
        self.map(record)

    def extract(self):
        pass
