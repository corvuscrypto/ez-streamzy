"""
This example shows how ez_streamzy can be used in performing
calculations that require combining chains

In this example, we want to add a running balance to some transaction
records. The caveat is that the amounts are stored as strings (sound familiar?).
The necessary steps to process data:
    - transform string to integer expressing dollars and cents
    - calculate the current running balance
    - divide amount to return it as a float

This kind of pipeline can be useful for processing transaction records
where it is useful to see per record what the full balance is.
"""
from ez_streamzy.base import Processor, Extractor


example_data = [
    {"date": "2018-03-01", "amount": "$200.00"},
    {"date": "2018-03-01", "amount": "$-10.50"},
    {"date": "2018-03-02", "amount": "$400.03"},
    {"date": "2018-03-02", "amount": "$-22.00"},
    {"date": "2018-03-02", "amount": "$-100.00"},
    {"date": "2018-03-03", "amount": "$54.50"},
    {"date": "2018-03-04", "amount": "$45.20"},
    {"date": "2018-03-05", "amount": "$-120.00"},
    {"date": "2018-03-08", "amount": "$200.00"},
    {"date": "2018-03-10", "amount": "$30.04"},
    {"date": "2018-03-11", "amount": "$-730.00"},
]


class StringToInt(Processor):
    def process(self, record):
        # get only the numbers
        amount_string = ''.join([
            x for x in record['amount']
            if x in '0123456789-'
        ])
        # cast to integer and set on record
        record['amount'] = int(amount_string)
        return record


class CalculateRunningBalance(Processor):
    running_total = 0

    def process(self, record):
        self.running_total += record['amount']
        record['balance'] = self.running_total
        return record


class AmountsToFloat(Processor):
    def process(self, record):
        record['amount'] = float(record['amount']) / 100.0
        record['balance'] = float(record['balance']) / 100.0
        return record


if __name__ == "__main__":
    # setup the processing chain
    chain = (
        Extractor(example_data) >> StringToInt()
        >> CalculateRunningBalance() >> AmountsToFloat()
    )

    result = list(chain)
    print(result)
