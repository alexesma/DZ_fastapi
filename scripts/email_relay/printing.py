"""Windows printer discovery and HTML-to-printer delivery for DZEmailRelay."""

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


def installed_printers() -> list[str]:
    """Return printers connected to this Windows user session."""
    if os.name != "nt":
        return []
    import win32print

    flags = win32print.PRINTER_ENUM_LOCAL | win32print.PRINTER_ENUM_CONNECTIONS
    names = {row["pPrinterName"] for row in win32print.EnumPrinters(flags, None, 2)}
    return sorted(name for name in names if name)


def safe_print_html(html: str) -> str:
    """Remove browser auto-print JavaScript and reject external resources."""
    cleaned = re.sub(
        r"<script\b[^>]*>.*?</script\s*>", "", html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    lowered = cleaned.lower()
    for forbidden in (
        "file://", "<iframe", "<object", "<embed", "<link",
        "<script", "@import", "url(http", "url(//",
        'src="http', "src='http", "src=//",
    ):
        if forbidden in lowered:
            raise ValueError("Печатная форма содержит внешний ресурс")
    if re.search(r'''\b(?:src|href)\s*=\s*["']?\s*(?:https?:|//|file:)''',
                 cleaned, re.IGNORECASE) or re.search(
                     r'''url\(\s*["']?\s*(?:https?:|//|file:)''',
                     cleaned, re.IGNORECASE):
        raise ValueError("Печатная форма содержит внешний ресурс")
    policy = ("<meta http-equiv=\"Content-Security-Policy\" "
              "content=\"default-src 'none'; style-src 'unsafe-inline'; "
              "img-src data:; font-src data:\">")
    if not re.search(r"<head\b[^>]*>", cleaned, re.IGNORECASE):
        raise ValueError("Печатная форма должна содержать HTML head")
    return re.sub(r"<head\b[^>]*>", lambda match: match.group() + policy,
                  cleaned, count=1, flags=re.IGNORECASE)


def find_edge(configured_path: str | None = None) -> str:
    if configured_path and Path(configured_path).is_file():
        return configured_path
    candidates = [
        Path(os.environ.get("PROGRAMFILES(X86)", ""))
        / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("PROGRAMFILES", ""))
        / "Microsoft/Edge/Application/msedge.exe",
    ]
    for path in candidates:
        if path.is_file():
            return str(path)
    found = shutil.which("msedge")
    if found:
        return found
    raise RuntimeError("Microsoft Edge не найден для подготовки печатной формы")


def render_html_to_pdf(html: str, directory: Path,
                       edge_path: str | None = None) -> Path:
    html_path = directory / "label.html"
    pdf_path = directory / "print.pdf"
    html_path.write_text(safe_print_html(html), encoding="utf-8")
    edge = find_edge(edge_path)
    result = subprocess.run(
        [
            edge, "--headless=new", "--disable-gpu",
            "--disable-extensions", "--disable-background-networking",
            "--disable-javascript", "--no-pdf-header-footer",
            f"--user-data-dir={directory / 'edge-profile'}",
            f"--print-to-pdf={pdf_path}", html_path.as_uri(),
        ],
        capture_output=True, text=True, timeout=90, check=False,
    )
    if result.returncode or not pdf_path.is_file() or pdf_path.stat().st_size < 100:
        raise RuntimeError(
            "Edge не смог подготовить PDF: "
            + (result.stderr or result.stdout or str(result.returncode))[-500:]
        )
    return pdf_path


def spool_pdf(pdf_path: Path, printer_name: str, title: str) -> None:
    """Render each PDF page to the selected printer through Windows GDI.

    The driver owns the physical paper size. For thermal printers configure
    the correct label stock in Windows once; the HTML @page size controls PDF.
    EndDoc confirms acceptance by the Windows spooler, not paper delivery.
    """
    if os.name != "nt":
        raise RuntimeError("Автоматическая печать доступна только в Windows")

    import pypdfium2 as pdfium
    import win32con
    import win32ui
    from PIL import ImageWin

    pdf = pdfium.PdfDocument(str(pdf_path))
    dc = win32ui.CreateDC()
    started = False
    try:
        dc.CreatePrinterDC(printer_name)
        dpi_x = dc.GetDeviceCaps(win32con.LOGPIXELSX)
        dpi_y = dc.GetDeviceCaps(win32con.LOGPIXELSY)
        printable_w = dc.GetDeviceCaps(win32con.HORZRES)
        printable_h = dc.GetDeviceCaps(win32con.VERTRES)
        if not dpi_x or not dpi_y or not printable_w or not printable_h:
            raise RuntimeError("Драйвер принтера не сообщил размер страницы")
        dc.StartDoc(title[:100])
        started = True
        for index in range(len(pdf)):
            page = pdf[index]
            width_pt, height_pt = page.get_size()
            image = page.render(scale=300 / 72).to_pil().convert("RGB")
            target_w = max(1, round(width_pt / 72 * dpi_x))
            target_h = max(1, round(height_pt / 72 * dpi_y))
            scale = min(printable_w / target_w, printable_h / target_h, 1)
            target_w = max(1, round(target_w * scale))
            target_h = max(1, round(target_h * scale))
            left = (printable_w - target_w) // 2
            top = (printable_h - target_h) // 2
            dc.StartPage()
            ImageWin.Dib(image).draw(
                dc.GetHandleOutput(),
                (left, top, left + target_w, top + target_h),
            )
            dc.EndPage()
        dc.EndDoc()
        started = False
    finally:
        if started:
            try:
                dc.AbortDoc()
            except Exception:
                pass
        dc.DeleteDC()
        pdf.close()


def print_html_job(job: dict, edge_path: str | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix="dz-print-") as temp:
        directory = Path(temp)
        pdf_path = render_html_to_pdf(job["html"], directory, edge_path)
        spool_pdf(pdf_path, job["printer_name"], job["title"])
