from app.sources.universe import clean_company_name


def test_strips_common_stock():
    assert clean_company_name("Apple Inc. - Common Stock") == "Apple Inc."


def test_strips_class_a_ordinary_shares():
    assert (
        clean_company_name("Artius II Acquisition Inc. - Class A Ordinary Shares")
        == "Artius II Acquisition Inc."
    )


def test_strips_class_b_common_stock():
    assert clean_company_name("Foo Corp - Class B Common Stock") == "Foo Corp"


def test_strips_depositary_shares():
    assert (
        clean_company_name("Taiwan Semiconductor - American Depositary Shares")
        == "Taiwan Semiconductor"
    )


def test_leaves_dashless_name_alone():
    assert clean_company_name("American Airlines Group") == "American Airlines Group"


def test_leaves_non_security_dash_alone():
    # A company name with a dash but no security-type suffix should be preserved.
    assert clean_company_name("Coca-Cola Holdings") == "Coca-Cola Holdings"


def test_only_strips_rightmost_security_suffix():
    assert clean_company_name("A - B Holdings - Common Stock") == "A - B Holdings"


def test_strips_class_without_separator():
    """Nasdaq names sometimes omit the ' - ' separator entirely."""
    assert (
        clean_company_name("Evolent Health, Inc Class A Common Stock") == "Evolent Health, Inc"
    )


def test_strips_trailing_bare_class():
    assert clean_company_name("Foo Corp Class B") == "Foo Corp"


def test_strips_adr_suffix():
    assert clean_company_name("Some Foreign Co ADR") == "Some Foreign Co"


def test_empty_and_none():
    assert clean_company_name("") == ""
    assert clean_company_name(None) is None
