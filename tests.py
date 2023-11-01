import muffin
import pytest


@pytest.fixture()
def app():
    return muffin.Application()


@pytest.fixture()
def kafka(app):
    import muffin_kafka

    return muffin_kafka.Plugin(app)


async def test_base(kafka):
    assert "TODO"


# ruff: noqa: TRY002, ARG001
