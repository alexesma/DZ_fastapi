from dz_fastapi.schemas.email_account import (EmailAccountCreate,
                                              EmailAccountResponse,
                                              EmailAccountUpdate)


def test_email_account_create_normalizes_imap_folders():
    payload = EmailAccountCreate(
        name='Inbox',
        email='test@example.com',
        password='secret',
        imap_folder='',
        imap_additional_folders=' Orders ,\nArchive ; INBOX ',
    )

    assert payload.imap_folder == 'INBOX'
    assert payload.imap_additional_folders == ['Orders', 'Archive', 'INBOX']


def test_email_account_update_preserves_none_and_normalizes_blank():
    payload_none = EmailAccountUpdate()
    payload_blank = EmailAccountUpdate(
        imap_folder=' ',
        imap_additional_folders=[' Archive ', '', 'Orders'],
    )

    assert payload_none.imap_folder is None
    assert payload_none.imap_additional_folders is None
    assert payload_blank.imap_folder == 'INBOX'
    assert payload_blank.imap_additional_folders == ['Archive', 'Orders']


def test_email_account_response_defaults_imap_folder():
    payload = EmailAccountResponse(
        id=1,
        name='Inbox',
        email='test@example.com',
        password='',
        imap_folder=None,
        imap_additional_folders=None,
        purposes=[],
        is_active=True,
    )

    assert payload.imap_folder == 'INBOX'
    assert payload.imap_additional_folders == []
