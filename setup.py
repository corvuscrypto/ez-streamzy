from setuptools import find_packages, setup

setup(
    name='ez_streamzy',
    version='0.1.0',
    packages=find_packages(exclude=['tests*', 'examples']),
    description='Easy stream processing',
    author='Clifford Richardson',
    author_email='cmrallen@gmail.com',
    install_requires=[],
    tests_require=["pytest"]
)
