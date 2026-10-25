import pytest

from scripts.email_relay import printing

HEAD = "<html><head>{}</head><body>x</body></html>"


@pytest.mark.parametrize("head", [
    '<meta http-equiv="refresh" content="0;url=http://e.com/">',
    "<meta http-equiv=refresh content='0'>",
    '<base href="file:///C:/">',
    "<style>@import 'x.css';</style>",
])
def test_unsafe_print_html_is_rejected(head):
    with pytest.raises(ValueError):
        printing.safe_print_html(HEAD.format(head))


def test_plain_print_html_gets_csp():
    assert "Content-Security-Policy" in printing.safe_print_html(HEAD.format(""))
