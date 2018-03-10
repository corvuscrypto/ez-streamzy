"""
This example shows how ez_streamzy can be used in normalization
pipelines.

In this trivial setup, we want to normalize strings by performing
the following operations:
    - lower case transformation
    - strip extra whitespace
    - string exchange based on original value

This kind of pipeline can be useful for processing records where
you need to take a string that may have many variations and
transforming it to a type the rest of your data system understands.

The example we will use is a list of animal names. We will generalize
the type of animal based on a static map we create.
"""
from ez_streamzy.base import Extractor, Processor

example_data = [
    "Eagle",
    "UNICORN",
    "beaR",
    "doVe",
    "Husky",
    "husky",
    "dRaGoN"
]

class ToLowerCase(Processor):
    def process(self, data):
        return data.lower()


class StripWhitespace(Processor):
    def process(self, data):
        return data.strip()


class GetAnimalType(Processor):
    mappings = {
        "bird": ["falcon", "eagle", "dove", "raven"],
        "dog": ["husky", "chihuahua", "poodle"],
        "mythical": ["unicorn", "dragon", "gryphon"]
    }

    def process(self, data):
        # search through the types we have and try to find
        # the first that matches
        for _type, _list in self.mappings.items():
            if data in _list:
                return _type

        # if not any other type, just return empty string
        return ""


if __name__ == "__main__":

    # setting up the process chain is easy
    chain = (
        Extractor(example_data) >> ToLowerCase()
        >> StripWhitespace() >> GetAnimalType()
    )

    result = list(chain)

    # ['bird', 'mythical', '', 'bird', 'dog', 'dog', 'mythical']
    print(result)
