from datetime import timedelta

# Frequency intervals for scheduling
FREQUENCY_INTERVALS = {
    'Hourly': timedelta(hours=1),
    'Every 3 Hours': timedelta(hours=3),
    'Daily': timedelta(days=1),
    'Every 12 Hours': timedelta(hours=12),
    'Weekly': timedelta(weeks=1),
    'Monthly': timedelta(days=30)
}

# Trigger arguments for APScheduler
TRIGGER_ARGS = {
    'Hourly': {'hours': 1},
    'Every 3 Hours': {'hours': 3},
    'Daily': {'days': 1},
    'Every 12 Hours': {'hours': 12},
    'Weekly': {'weeks': 1},
    'Monthly': {'days': 30}
}

# Mapping of tender types to scraping functions
SCRAPING_FUNCTIONS = {
    'UNGM Tenders': 'scrape_ungm_tenders',
    'ReliefWeb Jobs': 'fetch_reliefweb_tenders',
    'Job in Rwanda': 'jobinrwanda_tenders',
    'Kenya Treasury': 'treasury_ke_tenders',
    'UNDP': 'scrape_undp_tenders',
    'PPIP': 'scrape_ppip_tenders',
    'Search Query Tenders': None
}