from dz_fastapi.core.db import get_async_session, get_engine


def test_get_engine_reuses_shared_instance():
    engine_1 = get_engine()
    engine_2 = get_engine()

    assert engine_1 is engine_2


def test_get_async_session_reuses_shared_factory():
    factory_1 = get_async_session()
    factory_2 = get_async_session()

    assert factory_1 is factory_2
    assert factory_1.kw["bind"] is get_engine()
