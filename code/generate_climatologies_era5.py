"""Generate the 20-year (1995-2014) mean ERA5 reference climatology file(s) in
../data, from the raw ERA5 ETCCDI index files.

Currently restricted to tx90p (only index needed so far for the ERA5
comparison figure); extend the loop below to add more.

Run with `python generate_climatologies_era5.py` from this directory (code/).
"""
import os

from core.calc_means import load_aggregate_data_era5, add_metadata, SOURCE_ERA5

save_path = '../data'
startyear = 1995
endyear = 2014

if __name__ == '__main__':
    for index in ['tx90p']:  # only one index needed for now
        ds = load_aggregate_data_era5(index, startyear=startyear, endyear=endyear)
        ds = add_metadata(
            ds,
            title=f'{startyear}-{endyear} mean of annual ETCCDI Extreme Indices',
            source=SOURCE_ERA5,
        )
        fn_save = f'{index}_{startyear}-{endyear}_era5.nc'
        ds.to_netcdf(os.path.join(save_path, fn_save))
        print(f'Saved {fn_save}')
