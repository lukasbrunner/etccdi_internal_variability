"""Ancillary DJF/JJA seasonal PRCPTOT climatology, beyond the manuscript's main
(annual-only) analysis. Reads monthly (`*_mon_*.nc`) MPI-GE files directly,
since seasonal binning requires sub-annual resolution.
"""
import os
import numpy as np
import xarray as xr
from natsort import natsorted
from glob import glob

from core.utils import index_unit_map, index_acronym_map, index_longname_map, index_explanation_map
from core.calc_means import add_metadata, SOURCE_MPI_GE

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
    """Build the 20-year DJF or JJA PRCPTOT climatology Dataset (dims: member, lat, lon).

    Parameters
    ----------
    season : {'DJF', 'JJA'}
        Season to aggregate to.
    startyear : int, optional
        First year of the analysis period. Default is ``1995``.
    endyear : int, optional
        Last year of the analysis period. Default is ``2014``.
    overwrite : bool, optional
        Unused; reserved for a future skip-if-exists check. Default is
        ``False``.

    Returns
    -------
    xarray.Dataset
        Single-variable Dataset named ``'prcptot'``, dims
        ``(member, lat, lon)``.
    """
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
        explanation=index_explanation_map[index],
    )
    ds = da.to_dataset(name=index)
    return ds


if __name__ == '__main__':
    for season in ('DJF', 'JJA'):
        ds = load_aggregate_seasonal_prcptot(season, startyear=startyear, endyear=endyear)
        ds = add_metadata(ds, title=f'{startyear}-{endyear} {season} mean of PRCPTOT', source=SOURCE_MPI_GE)
        fn_save = f'{index}-{season.lower()}_{startyear}-{endyear}.nc'
        ds.to_netcdf(os.path.join(save_path, fn_save))
        print(f'Saved {fn_save}')
