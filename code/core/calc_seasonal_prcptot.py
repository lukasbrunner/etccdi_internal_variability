import os
import numpy as np
import xarray as xr
from natsort import natsorted
from glob import glob
from datetime import datetime

from core.utils import index_unit_map, index_acronym_map, index_longname_map

time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

index = 'prcptot'
startyear = 1995
endyear = 2014
scenario = 'historical'
base_path = '/work/uc1275/MPI-GE_ETCCDI_indices'  # Adjust to local data path
dir_path = os.path.dirname(os.path.realpath(__file__))
save_path = os.path.join(dir_path, '..', '..', 'data')


def load_aggregate_seasonal_prcptot(
    season: str,
    startyear: int = startyear,
    endyear: int = endyear,
    overwrite: bool = False,
) -> xr.Dataset:
    assert season in ('DJF', 'JJA')

    print(f'Load {index=}, {season=}')
    files = natsorted(glob(os.path.join(base_path, index, scenario, '*_mon_*.nc')))
    da_list = []
    for fn in files:
        member = os.path.basename(fn).split('_')[4]
        da = xr.open_dataset(fn, decode_timedelta=False, decode_times=time_coder)[f'{index}ETCCDI']

        if np.any(np.isnan(da)):
            print(f'nan found in {index=}, {member=}')  # nans are allowed to happen for prcptot

        if da['time.month'][0].item() != 1:
            raise ValueError('File does not start in January')

        # pad with December of the year before startyear so the first DJF season is complete
        da = da.sel(time=slice(f'{startyear - 1}-12', f'{endyear}-12'))
        da_season = da.resample(time='QS-DEC').sum()
        counts = da.resample(time='QS-DEC').count()
        da_season = da_season.where(counts == 3, drop=True)  # drop incomplete boundary seasons
        da_season = da_season.sel(time=da_season['time.season'] == season)
        da_season = da_season.mean('time')  # 20-year climatological mean

        da_season = da_season.expand_dims({'member': [member]})
        da_list.append(da_season)

    da = xr.concat(da_list, dim='member')
    da.attrs = dict(
        units=index_unit_map[index],
        long_name=index_acronym_map[index],
        description=index_longname_map[index],
    )
    ds = da.to_dataset(name=index)
    return ds


def add_metadata(ds: xr.Dataset, season: str) -> xr.Dataset:
    ds.attrs = {
        'title': f'{startyear}-{endyear} {season} mean of PRCPTOT',
        'further_info': 'https://etccdi.pacificclimate.org/list_27_indices.shtml',
        'processing_scripts': 'TODO: Git',
        'source': 'MPI-ESM1-2-LR; Olonscheck et al. (2023): https://doi.org/10.1029/2023MS003790',
        'creator': 'CC BY Lukas Brunner, University of Hamburg (lukas.brunner@uni-hamburg.de; https://orcid.org/0000-0001-5760-4524)',
        'institution': 'University of Hamburg',
        'creation_date': datetime.today().strftime('%Y-%m-%d %H:%M'),
        'reference': 'TODO: paper',
    }
    return ds


if __name__ == '__main__':
    for season in ('DJF', 'JJA'):
        ds = load_aggregate_seasonal_prcptot(season, startyear=startyear, endyear=endyear)
        ds = add_metadata(ds, season)
        fn_save = f'{index}-{season.lower()}_{startyear}-{endyear}.nc'
        ds.to_netcdf(os.path.join(save_path, fn_save))
        print(f'Saved {fn_save}')
