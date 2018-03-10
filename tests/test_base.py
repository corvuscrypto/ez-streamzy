"""
Tests the classes in base.py
"""
from ez_streamzy.base import Extractor, Processor, Output
from pytest import raises


class Add2Processor(Processor):
    def process(self, record):
        return record + 2


class Multiply2Processor(Processor):
    def process(self, record):
        return record * 2


class CountProcessor(Processor):
    def process(self, record):
        return len(record)

class ReferencedArrayOutput(Output):
    def __init__(self, obj):
        self.out_obj = obj

    def out(self, record):
        self.out_obj.append(record)


def test_chained_iteration_works():
    """
    Ensure that iteration follows the chain
    tightly and records get processed appropriately
    """
    source = Extractor([2, 3])
    A = Add2Processor()
    B = Multiply2Processor()

    # add 2 then multiply 2
    chain = source >> A >> B >> A >> A

    expected_result = [12, 14]
    assert list(chain) == expected_result


def test_output_works_as_expected():
    """
    Ensure that outputs are able to handle records
    and that certain conditions are met
    """
    out_array = []
    output = ReferencedArrayOutput(out_array)
    source = Extractor([2, 3])

    chain = source >> output
    chain.run()

    assert out_array == [2, 3]

    # now create a new output and chain it
    new_out_array = []
    new_output = ReferencedArrayOutput(new_out_array)
    chain = chain >> new_output
    chain.run()

    assert new_out_array == [2, 3]
    # old one should be doubled now
    assert out_array == [2, 3, 2, 3]


def test_sub_chains():
    data = [2, 4, 1, 2]
    extractor = Extractor(data)
    # first create a single chain
    # this one only adds 2
    chain_1 = Add2Processor()

    # create a chain that
    # multiplies by two
    chain_2 = Multiply2Processor()

    # create a chain that
    # multiplies by two and then adds by two
    chain_3 = chain_2 >> chain_1

    result_1 = list(extractor >> chain_1)
    result_2 = list(extractor >> chain_2)
    result_3 = list(extractor >> chain_2 >> chain_1)

    assert result_1 == [4, 6, 3, 4]
    assert result_2 == [4, 8, 2, 4]
    assert result_3 == [6, 10, 4, 6]


