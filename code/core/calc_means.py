import os
import numpy as np
import xarray as xr
from natsort import natsorted
from glob import glob
from datetime import datetime, timedelta

from core.utils import index_unit_map, index_acronym_map, index_longname_map

time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

startyear = 1995
endyear = 2014
scenario = 'historical'
base_path = '/work/uc1275/MPI-GE_ETCCDI_indices'  # Adjust to local data path
era_path = '/work/uc1275/LukasBrunner/ERA5/ETCCDI_g025'  # Adjust to local data path


def aggregations(index: str) -> str:
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


def load_aggregate_data(
    index: str, 
    startyear: int=startyear, 
    endyear: int=endyear, 
    overwrite: bool=False
) -> xr.Dataset:
    
    print(f'Load {index=}')
    fn_save = f'{index}_{startyear}-{endyear}.nc'       
    files = natsorted(glob(os.path.join(base_path, index, scenario, '*.nc')))
    da_list = []
    for fn in files:
        member = os.path.basename(fn).split('_')[4]
        da = xr.open_dataset(fn, decode_timedelta=False, decode_times=time_coder)[f'{index}ETCCDI']
        
        if np.any(np.isnan(da)):
            if index in ['cwd', 'cdd', 'gsl', 'sdii']:  # nans are alowed to happen
                print(f'nan found in {index=}, {member=}')
            else:
                raise ValueError(f'nan found in {index=}, {member=}')
                
        if da['time.month'][0].item() == 2:
            if index in ['tx90p', 'tx10p', 'tn90p', 'tn10p']:
                da = da.assign_coords(time=da['time'] - timedelta(days=31))  # fix time shift
                # print(f'Fixed time shift for {index}')
            else:
                raise ValueError('File does not start in January')
                
        da = aggregate_period(da.sel(time=slice(str(startyear), str(endyear))), index)
        da = da.expand_dims({'member': [member]})
        da_list.append(da)
    
    da = xr.concat(da_list, dim='member') 
    da.attrs = dict(
        units = index_unit_map[index],
        long_name = index_acronym_map[index],
        description = index_longname_map[index],
    )
    ds = da.to_dataset(name=index)
    return ds


def load_aggregate_data_era5(index: str, startyear: int=startyear, endyear: int=endyear, overwrite: bool=False) -> xr.Dataset:
    print(f'Load {index=}')
    fn_save = f'{index}_{startyear}-{endyear}.nc'       
    files = natsorted(glob(os.path.join(era_path, '{}ETCCDI_*.nc'.format(index))))
    da_list = []
    for fn in files:
        member = os.path.basename(fn).split('_')[4]
        da = xr.open_dataset(fn, decode_timedelta=False, decode_times=time_coder)[f'{index}ETCCDI']
        
        if np.any(np.isnan(da)):
            if index in ['CWD', 'CCD']:  # nans are alowed to happen
                print(f'nan found in {index=}, {member=}')
            else:
                raise ValueError(f'nan found in {index=}, {member=}')
                
        if da['time.month'][0].item() == 2:
            if index in ['tx90p', 'tx10p', 'tn90p', 'tn10p']:
                da = da.assign_coords(time=da['time'] - timedelta(days=31))  # fix time shift
                # print(f'Fixed time shift for {index}')
            else:
                raise ValueError('File does not start in January')
                
        da = aggregate_period(da.sel(time=slice(str(startyear), str(endyear))), index)
        da = da.expand_dims({'member': [member]})
        da_list.append(da)
    
    da = xr.concat(da_list, dim='member') 
    da.attrs = dict(
        units = index_unit_map[index],
        long_name = index_acronym_map[index],
        description = index_longname_map[index],
    )
    ds = da.to_dataset(name=index)
    return ds