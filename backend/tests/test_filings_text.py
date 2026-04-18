from app.util.filings_text import analyze_filing_html, count_chapter_11, detect_going_concern


def test_detect_going_concern_positive():
    text = (
        "management has concluded that there is substantial doubt about the company's ability "
        "to continue as a going concern for a period of one year from the issuance of these "
        "financial statements."
    ).lower()
    assert detect_going_concern(text) is True


def test_detect_going_concern_negated():
    text = (
        "the auditors concluded that there is no substantial doubt about the company's ability "
        "to continue as a going concern."
    ).lower()
    assert detect_going_concern(text) is False


def test_detect_going_concern_absent():
    text = "the company reported a profit and has strong liquidity.".lower()
    assert detect_going_concern(text) is False


def test_count_chapter_11_case_and_spacing():
    text = "The Chapter 11 filing was made. Another chapter  11 reference. CHAPTER 11 again."
    assert count_chapter_11(text) == 3


def test_analyze_filing_html_strips_tags_and_counts():
    html = (
        "<html><body><p>Our subsidiary filed for Chapter 11 protection.</p>"
        "<p>There is substantial doubt about the Company's ability to continue as a going concern.</p>"
        "<script>ignore()</script></body></html>"
    )
    gc, ch11 = analyze_filing_html(html)
    assert gc is True
    assert ch11 == 1
