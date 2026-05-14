import imaplib
import smtplib

IMAP_CONNECT_TIMEOUT = 30  # seconds


def test_imap_connection(
    host: str,
    port: int,
    username: str,
    password: str,
    folder: str = "INBOX",
    use_ssl: bool = True,
) -> None:
    if use_ssl:
        client = imaplib.IMAP4_SSL(host, port)
    else:
        client = imaplib.IMAP4(host, port)
        client.starttls()
    # Set socket-level timeout so an unreachable server doesn't hang forever
    client.socket().settimeout(IMAP_CONNECT_TIMEOUT)
    try:
        client.login(username, password)
        client.select(folder)
    finally:
        try:
            client.logout()
        except Exception:
            try:
                client.shutdown()
            except Exception:
                pass


def test_smtp_connection(
    host: str,
    port: int,
    username: str,
    password: str,
    use_ssl: bool = True,
    timeout: int = 10,
) -> None:
    if use_ssl:
        server = smtplib.SMTP_SSL(host, port, timeout=timeout)
    else:
        server = smtplib.SMTP(host, port, timeout=timeout)
        server.starttls()
    try:
        server.login(username, password)
    finally:
        server.quit()
