from dz_fastapi.services.customer_orders import _is_retryable_imap_fetch_error


def test_server_closing_imap_connection_is_retryable():
    error = RuntimeError("command: UID => Server is closing this connection")

    assert _is_retryable_imap_fetch_error(error)
