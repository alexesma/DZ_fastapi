from types import SimpleNamespace

from dz_fastapi.services.customer_orders import _pick_configs_for_account


def _cfg(config_id, email_account_id=None, email_account_ids=None):
    return SimpleNamespace(
        id=config_id,
        email_account_id=email_account_id,
        email_account_ids=email_account_ids,
    )


def test_pick_configs_for_account_supports_multiple_mailboxes():
    cfg_multi = _cfg(1, email_account_ids=[3, 5])
    cfg_global = _cfg(2)

    result = _pick_configs_for_account([cfg_multi, cfg_global], 5)

    assert [cfg.id for cfg in result] == [1]


def test_pick_configs_for_account_falls_back_to_global_for_other_mailbox():
    cfg_single = _cfg(1, email_account_id=3)
    cfg_global = _cfg(2)

    result = _pick_configs_for_account([cfg_single, cfg_global], 7)

    assert [cfg.id for cfg in result] == [2]
