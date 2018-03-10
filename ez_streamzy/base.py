"""
Code for the base entities that manage stream operators
that can act on a record of data.

The base classes are Extractor, Processor, and Output.
"""


class Processor(object):
    """
    Processor object which acts as a base for handling
    records in a streaming manner to process them using
    an arbitrary function.
    """
    def process(self, record):
        raise NotImplementedError

    def __rshift__(self, other):
        obj = other
        if not hasattr(other, "processors"):
            obj = StreamChain()
        obj.processors.append(self)
        return obj


class Output(Processor):
    """
    Output object which acts as a base for setting up
    outputs in a stream chain. It has a method `out` that is implicitly
    called by the `process` method.

    The `out` method is to be overriden
    """
    def out(self, record):
        raise NotImplementedError

    def process(self, record):
        self.out(record)
        return record


class Extractor(object):
    """
    Extractor object which acts as a base for extracting data
    from a source.

    Attributes:
        source (iterable): An iterable source of data
    """
    def __init__(self, source):
        self.source = source

    def __rshift__(self, other):
        obj = other
        if not hasattr(other, "extractors"):
            obj = StreamChain()
            obj.processors.append(other)
        obj.extractors.append(self)
        return obj

    def __iter__(self):
        return (x for x in self.source)


class StreamChain(Processor):
    """
    This is the main chain representation that
    dictates the path that a record follows in streamed processing.
    """
    def __init__(self):
        self.extractors = []
        self.processors = []

    def run(self):
        for extractor in self.extractors:
            for record in extractor:
                self.process(record)

    def process(self, record):
        out = record
        for processor in self.processors:
            out = processor.process(out)
        return out

    @property
    def _generator(self):
        for extractor in self.extractors:
            for record in extractor:
                out = self.process(record)
                yield out

    def __iter__(self):
        return (x for x in self._generator)

    def __rshift__(self, other):
        if isinstance(other, Processor):
            self.processors.append(other)
        elif isinstance(other, StreamChain):
            new_chain = StreamChain()
            new_chain.processors = self.processors
            new_chain.processors.append(other)
            return new_chain
        else:
            raise TypeError("You can only add Processors or Outputs to a StreamChain")
        return self
