import pytest


def pytest_addoption(parser):
    parser.addoption("--llm", action="store_true", default=False, help="run tests with llm calls")


def pytest_configure(config):
    config.addinivalue_line("markers", "llm: mark test as making llm calls")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--llm"):
        # --llm given in cli: do not skip llm tests
        return
    skip_llm = pytest.mark.skip(reason="need --llm option to run")
    for item in items:
        if "llm" in item.keywords:
            item.add_marker(skip_llm)
