index_unit_map = {
    # --- temperature-based ---
    # no threshold
    'txx': '°C',
    'tnn': '°C',
    'txn': '°C',
    'tnx': '°C',
    'dtr': '°C',
    # relative threshold
    'tx90p': '%',
    'tn10p': '%',
    'tx10p': '%',
    'tn90p': '%',
    'wsdi': 'days',
    'csdi': 'days',
    # absolute threshold
    'su': 'days',
    'id': 'days',
    'fd': 'days',
    'tr': 'days',
    'gsl': 'days',
    # --- precipitation-based ---
    # no threshold
    'prcptot': 'mm',
    'sdii': 'mm',
    'rx1day': 'mm',
    'rx5day': 'mm',
    'cwd': 'days',
    'cdd': 'days',
    # relative threshold
    'r95p': 'mm',
    'r99p': 'mm',
    # absolute threshold
    'r10mm': 'days',
    'r20mm': 'days',
}

index_acronym_map = {
    # --- temperature-based ---
    # no threshold
    'txx': 'TXx',
    'tnn': 'TNn',
    'txn': 'TXn',
    'tnx': 'TNx',
    'dtr': 'DTR',
    # relative threshold
    'tx90p': 'TX90p',
    'tn10p': 'TN10p',
    'tx10p': 'TX10p',
    'tn90p': 'TN90p',
    'wsdi': 'WSDI',
    'csdi': 'CSDI',
    # absolute threshold
    'su': 'SU',
    'id': 'ID',
    'fd': 'FD',
    'tr': 'TR',
    'gsl': 'GSL',
    # --- precipitation-based ---
    # no threshold
    'prcptot': 'PRCPTOT',
    'sdii': 'SDII',
    'rx1day': 'Rx1day',
    'rx5day': 'Rx5day',
    'cwd': 'CWD',
    'cdd': 'CDD',
    # relative threshold
    'r95p': 'R95p',
    'r99p': 'R99p',
    # absolute threshold
    'r10mm': 'R10mm',
    'r20mm': 'R20mm',    
}

# TODO: Settle on a terminology
# - Just the acronyms (e.g., 'SU')
# - Meaning (e.g., tasmax > 25degC)
# - Longname (e.g., Summer days) --> in particular for pr indices this differs between studies!

# Lay descriptions of the annual index calculation (percentile-based indices
# use the 1961-1990 base period as reference)
index_explanation_map = {
    # --- temperature-based ---
    # no threshold
    'txx': 'The highest daily maximum temperature reached in a year.',
    'tnn': 'The lowest daily minimum temperature reached in a year.',
    'txn': 'The lowest daily maximum temperature reached in a year.',
    'tnx': 'The highest daily minimum temperature reached in a year.',
    'dtr': 'The average difference between the daily maximum and minimum temperature.',
    # relative threshold
    'tx90p': 'The share of days per year with unusually warm daytime temperatures, i.e., a daily maximum above 90% of the values for the same location and time of year in the reference period.',
    'tn10p': 'The share of days per year with unusually cold nights, i.e., a daily minimum below 90% of the values for the same location and time of year in the reference period.',
    'tx10p': 'The share of days per year with unusually cool daytime temperatures, i.e., a daily maximum below 90% of the values for the same location and time of year in the reference period.',
    'tn90p': 'The share of days per year with unusually warm nights, i.e., a daily minimum above 90% of the values for the same location and time of year in the reference period.',
    'wsdi': 'The number of days per year in warm spells: at least six consecutive days with unusually warm daytime temperatures (as for warm days).',
    'csdi': 'The number of days per year in cold spells: at least six consecutive days with unusually cold nights (as for cool nights).',
    # absolute threshold
    'su': 'The number of days per year with a daily maximum temperature above 25 °C.',
    'id': 'The number of days per year on which the temperature never rises above 0 °C.',
    'fd': 'The number of days per year on which the daily minimum temperature drops below 0 °C.',
    'tr': 'The number of nights per year on which the temperature stays above 20 °C.',
    'gsl': 'The number of days per year between the first six-day warm period (daily mean above 5 °C) and the first six-day cold period in the second half of the year (daily mean below 5 °C).',
    # --- precipitation-based ---
    # no threshold
    'prcptot': 'The total precipitation falling on wet days (at least 1 mm) in a year.',
    'sdii': 'The average precipitation falling on wet days (at least 1 mm) in a year.',
    'rx1day': 'The largest one-day precipitation amount in a year.',
    'rx5day': 'The largest precipitation amount falling within five consecutive days in a year.',
    'cwd': 'The longest streak of consecutive wet days (at least 1 mm each) in a year.',
    'cdd': 'The longest streak of consecutive dry days (less than 1 mm each) in a year.',
    # relative threshold
    'r95p': 'The precipitation per year falling on very wet days, i.e., days with more rain than 95% of the wet days in the reference period.',
    'r99p': 'The precipitation per year falling on extremely wet days, i.e., days with more rain than 99% of the wet days in the reference period.',
    # absolute threshold
    'r10mm': 'The number of days per year with at least 10 mm of precipitation.',
    'r20mm': 'The number of days per year with at least 20 mm of precipitation.',
}

index_longname_map = {
    # --- temperature-based ---
    # no threshold
    'txx': 'Hottest daily maximum',
    'tnn': 'Coldest daily minimum',
    'txn': 'Coldest daily maximum',
    'tnx': 'Hottest daily minimum',
    'dtr': 'Daily temperature range',
    # relative threshold
    'tx90p': 'Warm days',
    'tn10p': 'Cool nights',
    'tx10p': 'Cool days',
    'tn90p': 'Warm nights',
    'wsdi': 'Average warm spell duration',
    'csdi': 'Average cold spell duration',
    # absolute threshold
    'su': 'Summer days',
    'id': 'Ice days',
    'fd': 'Frost days',
    'tr': 'Tropical nights',
    'gsl': 'Growing season length',
    # --- precipitation-based ---
    # no threshold
    'prcptot': 'Total precipitation',
    'sdii': 'Mean precipitation from wet days',
    'rx1day': 'Maximum 1-day precipitation',
    'rx5day': 'Maximum 5-day precipitation',
    'cwd': 'Maximum consecutive wet days',
    'cdd': 'Maximum consecutive dry days',
    # relative threshold
    'r95p': 'Precipitation from heavy rain days',
    'r99p': 'Precipitation from very heavy rain days',
    # absolute threshold
    'r10mm': 'Number of heavy rain days',
    'r20mm': 'Number of very heavy rain days',
}