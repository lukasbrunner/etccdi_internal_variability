import os
import xarray as xr
time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

base_path = '/work/uc1275/LukasBrunner/data/ETCCDI-MPI/time_mean_indices'


def load_data(index):
    fn = f'{index}_1995-2014.nc'
    da = xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder)[f'{index}']
    return da  