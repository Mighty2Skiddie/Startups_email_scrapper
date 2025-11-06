from scraper.utils import normalize_domain, same_domain

def test_normalize_domain_variants():
    assert normalize_domain("https://Sub.Example.co.uk") == "example.co.uk"
    assert normalize_domain("example.com") == "example.com"
    assert normalize_domain("http://example.ai/path") == "example.ai"
    assert normalize_domain("") is None
    assert normalize_domain("not a url") is None or isinstance(normalize_domain("not a url"), str)

def test_same_domain():
    assert same_domain("example.com", "https://www.example.com/about")
    assert not same_domain("example.com", "https://othersite.com/contact")
