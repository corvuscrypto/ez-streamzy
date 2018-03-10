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

# def test_simple_mapping_works():
#     """
#     Ensure that Mapping maps all the records
#     before actually releasing the records
#     """

#     fake_data = [
#         {"id": 1, "value": 100.0},
#         {"id": 1, "value": -10.0},
#         {"id": 2, "value": 300.0},
#         {"id": 2, "value": 200.0},
#         {"id": 2, "value": 200.0},
#         {"id": 1, "value": 1000.0},
#         {"id": 4, "value": 10.0},
#         {"id": 1, "value": 1.0},
#         {"id": 5, "value": 10.0},
#         {"id": 5, "value": -100.0},
#     ]

#     source = Extractor(fake_data)
#     mapper = InMemMapper()
#     reducer = ValueReducer()

#     chain = source >> mapper >> reducer

#     expected_result = [(1, 1091.0), (2, 700.0), (4, 10.0), (5, -90.0)]
#     assert list(chain) == expected_result


