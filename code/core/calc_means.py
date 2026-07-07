"""Compute 20-year (1995-2014) mean ETCCDI climatologies from raw annual/monthly
index files, for both the MPI-GE ensemble and the ERA5 reference.

For each index and ensemble member, the (already-annual or monthly-rolled-up-
to-annual) index values are averaged over the analysis period to represent one
possible climate state, per the manuscript's Data & Methods section. Driven by
code/generate_climatologies.py and code/generate_climatologies_era5.py.
"""
import os
import numpy as np
import xarray as xr
from natsort import natsorted
from glob import glob
from datetime import datetime, timedelta

from core.utils import index_unit_map, index_acronym_map, index_longname_map, index_explanation_map

time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

startyear = 1995
endyear = 2014
scenario = 'historical'
base_path = '/work/uc1275/MPI-GE_ETCCDI_indices'  # Adjust to local data path
era_path = '/work/uc1275/LukasBrunner/ERA5/ETCCDI_gMPI'  # Adjust to local data path

SOURCE_MPI_GE = 'MPI-ESM1-2-LR; Olonscheck et al. (2023): https://doi.org/10.1029/2023MS003790'
SOURCE_ERA5 = 'ERA5; Hersbach et al. (2020): doi.org/10.1002/qj.3803'


def add_metadata(ds: xr.Dataset, title: str, source: str) -> xr.Dataset:
    """Attach the standard set of global (file-level) metadata attrs.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to annotate (modified in place and returned).
    title : str
        Value for the ``title`` attr.
    source : str
        Value for the ``source`` attr (e.g. ``SOURCE_MPI_GE`` or
        ``SOURCE_ERA5``).

    Returns
    -------
    xarray.Dataset
        ``ds`` with its global attrs set.
    """
    ds.attrs = {
        'title': title,
        'further_info': 'https://etccdi.pacificclimate.org/list_27_indices.shtml',
        'processing_scripts': 'https://github.com/lukasbrunner/etccdi_internal_variability',
        'source': source,
        'creator': 'CC BY Lukas Brunner, University of Hamburg (lukas.brunner@uni-hamburg.de; https://orcid.org/0000-0001-5760-4524)',
        'institution': 'University of Hamburg',
        'creation_date': datetime.today().strftime('%Y-%m-%d %H:%M'),
        'reference': 'Brunner et al. (submitted): The Variability Atlas: How internal climate variability affects the estimation of climate extreme indices, Environmental Research Letters',
    }
    return ds


def aggregations(index: str) -> str:
    """Return the annual roll-up method ('none'/'mean'/'sum'/'max'/'min') for an index.

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name.

    Returns
    -------
    str
        One of ``'none'``, ``'mean'``, ``'sum'``, ``'max'``, ``'min'``.
    """
    if index in ['cdd', 'cwd', 'csdi', 'wsdi', 'gsl', 'prcptot', 'r10mm', 'r20mm', 'r95p', 'r99p', 'sdii', 'wsdi']:
        return 'none'
    elif index in ['dtr', 'tn10p', 'tn90p', 'tx10p', 'tx90p']:
        return 'mean'
    elif index in ['fd', 'id', 'su', 'tr']:
        return 'sum'
    elif index in ['rx1day', 'rx5day', 'tnx', 'txx']:
        return 'max'
    elif index in ['tnn', 'txn']:
        return 'min'


def aggregate_period(da: xr.DataArray, index: str) -> xr.DataArray:
    """Roll up to annual values per ``aggregations(index)``, then average over the period.

    Parameters
    ----------
    da : xarray.DataArray
        Raw index time series (``time`` dimension), already sliced to the
        analysis period.
    index : str
        Lowercase ETCCDI index short-name, used to look up the roll-up method.

    Returns
    -------
    xarray.DataArray
        The period-mean value (``time``/``year`` dimension collapsed).
    """
    aggregation = aggregations(index)
    if aggregation == 'none':
        da = da.rename({'time': 'year'})
    elif aggregation == 'mean':  # redundant, could just calculate period mean
        da = da.groupby('time.year').mean()
    elif aggregation == 'sum':
        da = da.groupby('time.year').sum()
    elif aggregation == 'max':
        da = da.groupby('time.year').max()
    elif aggregation == 'min':
        da = da.groupby('time.year').min()
    return da.mean('year')


def _load_aggregate_files(
    index: str,
    files: list,
    startyear: int,
    endyear: int,
    nan_allowed_indices: list,
) -> xr.Dataset:
    """Shared loader: per-member load, NaN check, Feb-start fix, aggregate, concat over member."""
    da_list = []
    for fn in files:
        member = os.path.basename(fn).split('_')[4]
        da = xr.open_dataset(fn, decode_timedelta=False, decode_times=time_coder)[f'{index}ETCCDI']
        da = da.drop_vars('height', errors='ignore')  # scalar 2m coord from tasmax/tasmin, not needed

        if np.any(np.isnan(da)):
            if index in nan_allowed_indices:  # nans are alowed to happen
                print(f'nan found in {index=}, {member=}')
            else:
                raise ValueError(f'nan found in {index=}, {member=}')

        if da['time.month'][0].item() == 2:
            if index in ['tx90p', 'tx10p', 'tn90p', 'tn10p']:
                da = da.assign_coords(time=da['time'] - timedelta(days=31))  # fix time shift
            else:
                raise ValueError('File does not start in January')

        da = aggregate_period(da.sel(time=slice(str(startyear), str(endyear))), index)
        da = da.expand_dims({'member': [member]})
        da_list.append(da)

    da = xr.concat(da_list, dim='member')
    da.attrs = dict(
        units=index_unit_map[index],
        long_name=index_acronym_map[index],
        description=index_longname_map[index],
        explanation=index_explanation_map[index],
    )
    return da.to_dataset(name=index)


def load_aggregate_data(
    index: str,
    startyear: int=startyear,
    endyear: int=endyear,
    overwrite: bool=False
) -> xr.Dataset:
    """Build the 20-year MPI-GE climatology Dataset for ``index`` (dims: member, lat, lon).

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name.
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
        Single-variable Dataset named ``index``, dims ``(member, lat, lon)``.
    """
    print(f'Load {index=}')
    # 'none'-aggregation indices are pre-computed annually (_yr_ files; some,
    # e.g. prcptot, also have _mon_ files in the same directory, so this must
    # be explicit); every other index is only available as monthly (_mon_)
    # files, rolled up to annual by aggregate_period().
    frequency = '_yr_' if aggregations(index) == 'none' else '_mon_'
    files = natsorted(glob(os.path.join(base_path, index, scenario, f'*{frequency}*.nc')))
    return _load_aggregate_files(
        index, files, startyear, endyear,
        nan_allowed_indices=['cwd', 'cdd', 'gsl', 'sdii'],
    )


def load_aggregate_data_era5(index: str, startyear: int=startyear, endyear: int=endyear, overwrite: bool=False) -> xr.Dataset:
    """Build the 20-year ERA5 reference climatology Dataset for ``index``.

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name.
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
        Single-variable Dataset named ``index``, dims ``(member, lat, lon)``.
    """
    print(f'Load {index=}')
    files = natsorted(glob(os.path.join(era_path, '{}ETCCDI_*.nc'.format(index))))
    return _load_aggregate_files(
        index, files, startyear, endyear,
        nan_allowed_indices=['CWD', 'CCD'],
    )