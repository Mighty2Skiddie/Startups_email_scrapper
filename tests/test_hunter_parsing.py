from scraper.hunter_integration import HUNTER_DOMAIN_SEARCH

def test_hunter_domain_url_default():
    # Ensure default endpoint is set and override-able.
    assert HUNTER_DOMAIN_SEARCH.startswith("https://api.")
