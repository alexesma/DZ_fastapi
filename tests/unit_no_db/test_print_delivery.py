import pytest
from pydantic import ValidationError

from dz_fastapi.api.print_jobs import PrintJobInput
from scripts.email_relay.printing import safe_print_html


def test_print_job_rejects_external_resources():
    with pytest.raises(ValidationError, match="внешний ресурс"):
        PrintJobInput(
            request_key="print-12345", kind="label", title="Test",
            html='<html><head></head><body><img src="https://bad.example/a.png"></body></html>',
        )


def test_print_helper_disables_script_and_remote_loading():
    html = safe_print_html(
        '<html><head></head><body><p>Этикетка</p>'
        '<script>window.print()</script></body></html>'
    )
    assert "window.print()" not in html
    assert "Content-Security-Policy" in html
    assert "default-src 'none'" in html


def test_print_helper_rejects_external_stylesheet():
    with pytest.raises(ValueError, match="внешний ресурс"):
        safe_print_html('<html><head><link href="https://bad.example"></head></html>')
