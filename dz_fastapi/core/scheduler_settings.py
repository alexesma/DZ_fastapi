SCHEDULER_SETTING_DEFAULTS = {
    'watchlist_site_check': {
        'days': [],
        'times': ['02:00'],
    },
    'watchlist_notify': {
        'days': [],
        'times': ['09:00'],
    },
    'pricelist_stale_notify': {
        'days': [],
        'times': ['09:00'],
    },
    'pricelist_stale_cleanup': {
        'days': [],
        'times': ['02:40'],
    },
    'cleanup_old_pricelists': {
        'days': [],
        'times': ['02:30'],
    },
    'metrics_snapshot': {
        'days': [],
        'times': ['09:00'],
    },
    'customer_orders_check': {
        'days': [],
        'times': [],
    },
    'supplier_responses_check': {
        'days': [],
        'times': [],
    },
    'supplier_orders_send': {
        'enabled': False,
        'days': [],
        'times': [],
    },
}

SCHEDULER_SETTING_KEYS = list(SCHEDULER_SETTING_DEFAULTS.keys())
