"""Generate the 20-year (1995-2014) mean ETCCDI climatology files in ../data,
one per index, from the raw MPI-GE annual/monthly index files.

Run with `python generate_climatologies.py` from this directory (code/).
"""
import os

from core.utils import index_acronym_map
from core.calc_means import load_aggregate_data, add_metadata, SOURCE_MPI_GE

save_path = '../data'
startyear = 1995
endyear = 2014

if __name__ == '__main__':
    for index in index_acronym_map.keys():
        ds = load_aggregate_data(index, startyear=startyear, endyear=endyear)
        ds = add_metadata(
            ds,
            title=f'{startyear}-{endyear} mean of annual ETCCDI Extreme Indices',
            source=SOURCE_MPI_GE,
        )
        fn_save = f'{index}_{startyear}-{endyear}.nc'
        ds.to_netcdf(os.path.join(save_path, fn_save))
        print(f'Saved {fn_save}')
