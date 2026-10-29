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


class _Result:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def test_render_retries_classic_headless_when_pdf_is_missing(tmp_path, monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command[1])
        if command[1] == "--headless":
            pdf = next(a for a in command if a.startswith("--print-to-pdf="))
            (tmp_path / "print.pdf").write_bytes(b"%PDF" + b"x" * 200)
            assert pdf.endswith("print.pdf")
        return _Result()

    monkeypatch.setattr(printing, "find_edge", lambda path=None: "edge")
    monkeypatch.setattr(printing.subprocess, "run", fake_run)
    monkeypatch.setattr(printing, "_wait_for_pdf",
                        lambda path, timeout=0: printing._pdf_ready(path))
    result = printing.render_html_to_pdf(HEAD.format(""), tmp_path)
    assert result.name == "print.pdf"
    assert calls == ["--headless=new", "--headless"]


def test_render_failure_message_is_explicit(tmp_path, monkeypatch):
    monkeypatch.setattr(printing, "find_edge", lambda path=None: "edge")
    monkeypatch.setattr(printing.subprocess, "run",
                        lambda command, **kwargs: _Result())
    monkeypatch.setattr(printing, "_wait_for_pdf",
                        lambda path, timeout=0: printing._pdf_ready(path))
    with pytest.raises(RuntimeError, match="код возврата 0, файл не создан"):
        printing.render_html_to_pdf(HEAD.format(""), tmp_path)
