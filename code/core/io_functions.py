import os
import xarray as xr
time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

dir_path = os.path.dirname(os.path.realpath(__file__))
base_path = os.path.join(dir_path, '..', '..', 'data')


def set_temperature_unit(da, to_celsius=False):
    unit_old = da.attrs.get('units', '').lower()
    if to_celsius:
        if unit_old in ['k', 'kelvin']:
            da.values -= 273.15
            da.attrs['units'] = '°C'
        elif unit_old in ['°c', 'celsius', 'degc', 'deg c', 'c']:
            pass  # already in °C
        else:
            raise ValueError(f"Cannot convert from unit '{unit_old}' to °C")
    else:
        if unit_old in ['°c', 'celsius', 'degc', 'deg c', 'c']:
            da.values += 273.15
            da.attrs['units'] = 'K'
        elif unit_old in ['k', 'kelvin']:
            pass  # already in K
    return da


def load_data(index, celsius=True):
    fn = f'{index}_1995-2014.nc'
    da = xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder)[f'{index}']
    da = set_temperature_unit(da, to_celsius=celsius)
    return da  


def load_data_era(index):
    fn = f'{index}_1995-2014_era5.nc'
    da = xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder)[f'{index}']
    return da  