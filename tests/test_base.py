"""
Tests the classes in base.py
"""
from ez_streamzy.base import StreamChain, Extractor, Processor, Mapper


class Add2Processor(Processor):
    def process(self, record):
        return record + 2


class Multiply2Processor(Processor):
    def process(self, record):
        return record * 2


class CountProcessor(Processor):
    def process(self, record):
        return len(record)


class InMemMapper(Mapper):
    def __init__(self):
        super(InMemMapper, self).__init__()
        self.mem_map = {}

    def map(self, record):
        id = record['id']
        if id not in self.mem_map:
            self.mem_map[id] = []
        self.mem_map[id].append(record)

    def extract(self):
        return ((x, y) for x, y in self.mem_map.items())


class ValueReducer(Processor):
    def process(self, record):
        amount = 0.0
        for r in record[1]:
            amount += r['value']
        return (record[0], amount)


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


def test_simple_mapping_works():
    """
    Ensure that Mapping maps all the records
    before actually releasing the records
    """

    fake_data = [
        {"id": 1, "value": 100.0},
        {"id": 1, "value": -10.0},
        {"id": 2, "value": 300.0},
        {"id": 2, "value": 200.0},
        {"id": 2, "value": 200.0},
        {"id": 1, "value": 1000.0},
        {"id": 4, "value": 10.0},
        {"id": 1, "value": 1.0},
        {"id": 5, "value": 10.0},
        {"id": 5, "value": -100.0},
    ]

    source = Extractor(fake_data)
    mapper = InMemMapper()
    reducer = ValueReducer()

    chain = source >> mapper >> reducer

    expected_result = [(1, 1091.0), (2, 700.0), (4, 10.0), (5, -90.0)]
    assert list(chain) == expected_result


