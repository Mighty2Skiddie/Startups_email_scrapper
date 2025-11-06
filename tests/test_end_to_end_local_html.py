from pathlib import Path
from scraper.extract import extract_emails_from_html

def test_local_html_fixture():
    html = Path(Path(__file__).parent / "fixtures" / "sample_about.html").read_text(encoding="utf-8")
    emails = extract_emails_from_html(html, domain="example.com")
    assert "jane.doe@example.com" in emails
    assert "info@example.com" in emails
