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
    'prcptot': 'pr',
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
    'cdd': 'Maximum consecuitve dry days',
    # relative threshold
    'r95p': 'Precipitation from heavy rain days',
    'r99p': 'Precipitation from very heavy rain days',
    # absolute threshold
    'r10mm': 'Number of heavy rain days',
    'r20mm': 'Number of very heavy rain days',
}