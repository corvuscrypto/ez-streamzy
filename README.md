[![Build Status](https://travis-ci.org/corvuscrypto/ez-streamzy.svg?branch=master)](https://travis-ci.org/corvuscrypto/ez-streamzy)
# Ez-streamzy

Ez-streamzy is meant to be an easy to use library that
allows you to set up your processing pipelines to handle streaming data.

## Why?

Because $#%! you that's why! and also because why not?

## Examples

There are a few examples if you prefer to just get your
hands dirty without knowing much about how something works
beneath the surface

## Quickstart

The whole idea behind ez-streamzy is that everything you
do using it should be relatively easy. There are three
major base classes to know about:

* Extractor
* Processor
* StreamChain

### Extractor

The extractor represents something that extracts data
from a source and streams the data, one record at a time,
into the rest of your pipeline.

Args:

* source - an iterable source of data

### Processor

The processor represents something that performs a
transformation on the record it is handling before
passing the record to the next step in the pipeline.

Overridable Methods:

* process(self, record) - This is the method to override to program calculations into the Processor. __YOU MUST RETURN THE TRANSFORMED RECORD TO PASS IT ALONG__

### StreamChain

The stream chain represents a full pipeline.
By chaining extractors and processors together, stream
chains are automagically created.

One can run the pipeline by calling the `run()` method on
a StreamChain. However, to get the results of the chain you must iterate over it using a for-loop or cast to a `list`

Methods:

* run(self) - run the pipeline without returning results

### Stream operator (>>)

The library overrides the right-shift operator to emulate
stream operations found in other languages. As a quick
example:

```python
extractor = Extractor([1, 2, 3])
processor = Processor()

chain = extractor >> processor

# get the results
list(chain)
```

That's it! Ez-peazy lemon squeezy, and that's Ez-streamzy
