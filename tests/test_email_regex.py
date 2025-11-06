from scraper.extract import extract_emails_from_html

def test_extract_emails_basic():
    html = """
    <a href="mailto:hello@example.com">Email</a>
    support@example.com
    firstname.lastname@sub.domain.ai
    image@2x.png
    """
    res = extract_emails_from_html(html)
    assert "hello@example.com" in res
    assert "support@example.com" in res
    assert "firstname.lastname@sub.domain.ai" in res
    assert all(not x.endswith(".png") for x in res)

def test_extract_emails_with_domain_bias():
    html = "a@alphaai.com other@other.com"
    res = extract_emails_from_html(html, domain="alphaai.com")
    assert res[0].endswith("@alphaai.com")
