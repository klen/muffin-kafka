# Muffin-Kafka

**Muffin-Kafka** – [Apache Kafka](https://kafka.apache.org) integration for [Muffin](https://klen.github.io/muffin) framework

[![Tests Status](https://github.com/klen/muffin-kafka/workflows/tests/badge.svg)](https://github.com/klen/muffin-kafka/actions)
[![PYPI Version](https://img.shields.io/pypi/v/muffin-kafka)](https://pypi.org/project/muffin-kafka/)
[![Python Versions](https://img.shields.io/pypi/pyversions/muffin-kafka)](https://pypi.org/project/muffin-kafka/)

## Requirements

- python >= 3.10

## Installation

**muffin-kafka** should be installed using pip:

```shell
$ pip install muffin-kafka
```

## Usage

```python
    from muffin import Application
    import muffin_kafka

    # Create Muffin Application
    app = Application('example')

    # Initialize the plugin
    kafka = muffin_kafka.Plugin(app, **options)

    # As alternative:
    # kafka = muffin_kafka.Plugin()
    # ...
    # kafka.setup(app, dsn="DSN_URL")

```

### Setup the plugin

TODO

## Bug tracker

If you have any suggestions, bug reports or annoyances please report them to
the issue tracker at https://github.com/klen/muffin-kafka/issues

## Contributing

Development of the project happens at: https://github.com/klen/muffin-kafka

## License

Licensed under a [MIT License](http://opensource.org/licenses/MIT)
