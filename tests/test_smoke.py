"""Proves the test harness can import both components."""


def test_can_import_scraper():
    from athlinks_scraper import core

    assert callable(core.get_results)


def test_can_import_dashboard_queries():
    import dashboard_queries

    assert callable(dashboard_queries.init_db)
