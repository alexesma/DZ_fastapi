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
    'cleanup_old_pricelists': {
        'days': [],
        'times': ['02:30'],
    },
    'metrics_snapshot': {
        'days': [],
        'times': ['09:00'],
    },
}

SCHEDULER_SETTING_KEYS = list(SCHEDULER_SETTING_DEFAULTS.keys())
