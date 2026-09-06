from dz_fastapi.services.customer_orders import _is_retryable_imap_fetch_error


def test_server_closing_imap_connection_is_retryable():
    error = RuntimeError("command: UID => Server is closing this connection")

    assert _is_retryable_imap_fetch_error(error)


def test_transient_ssl_handshake_errors_are_retryable():
    assert _is_retryable_imap_fetch_error(
        RuntimeError("[SSL: WRONG_VERSION_NUMBER] wrong version number")
    )
    assert _is_retryable_imap_fetch_error(
        RuntimeError("EOF occurred in violation of protocol (_ssl.c:2427)")
    )
