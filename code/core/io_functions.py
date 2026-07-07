"""Load the pre-computed 20-year (1995-2014) climatology files from ../../data."""
import os
import xarray as xr
time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)

dir_path = os.path.dirname(os.path.realpath(__file__))
base_path = os.path.join(dir_path, '..', '..', 'data')


def set_temperature_unit(da, to_celsius=False):
    """Convert a temperature DataArray between Kelvin and Celsius, in place.

    Celsius is more intuitive for display, but the coefficient of variation
    must be computed in Kelvin to avoid the arbitrary Celsius zero-point
    inflating CV near 0 degC (see the manuscript's discussion of temperature
    indices) -- callers pass ``to_celsius=False`` for that case.

    Parameters
    ----------
    da : xarray.DataArray
        Temperature field with a ``units`` attr (Kelvin or Celsius, in
        various spellings).
    to_celsius : bool, optional
        Convert to Celsius if True, to Kelvin if False. Default is ``False``.

    Returns
    -------
    xarray.DataArray
        ``da`` converted to the requested unit (``units`` attr updated).
    """
    unit_old = da.attrs.get('units', '').lower()
    if to_celsius:
        if unit_old in ['k', 'kelvin']:
            da.values -= 273.15
            da.attrs['units'] = '°C'
        elif unit_old in ['°c', 'celsius', 'degc', 'deg c', 'c']:
            pass  # already in °C
    else:
        if unit_old in ['°c', 'celsius', 'degc', 'deg c', 'c']:
            da.values += 273.15
            da.attrs['units'] = 'K'
        elif unit_old in ['k', 'kelvin']:
            pass  # already in K
    return da


def load_data(index, celsius=True):
    """Load the 1995-2014 MPI-GE climatology for ``index`` (dims: member, lat, lon).

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name (e.g. ``'txx'``, ``'prcptot'``).
    celsius : bool, optional
        For temperature-based indices, load in Celsius (True) or Kelvin
        (False); ignored for non-temperature indices. Default is ``True``.

    Returns
    -------
    xarray.DataArray
        The index climatology, dims ``(member, lat, lon)``.
    """
    fn = f'{index}_1995-2014.nc'
    da = xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder)[f'{index}']
    if index != 'dtr':  # dtr is a temperature difference: identical in K and °C, offset conversion would be wrong
        da = set_temperature_unit(da, to_celsius=celsius)
    return da


def load_metadata(index):
    """Read the per-index metadata attrs for ``index`` without loading array data.

    Cheaper than ``load_data`` for e.g. populating a UI description, since it
    does not materialize the (member, lat, lon) array.

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name.

    Returns
    -------
    dict
        The index DataArray's attrs (``units``, ``long_name``,
        ``description``, ``explanation``).
    """
    fn = f'{index}_1995-2014.nc'
    with xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder) as ds:
        return dict(ds[index].attrs)


def load_data_era(index):
    """Load the 1995-2014 ERA5 reference climatology for ``index``.

    Parameters
    ----------
    index : str
        Lowercase ETCCDI index short-name.

    Returns
    -------
    xarray.DataArray
        The ERA5 reference climatology, dims ``(lat, lon)`` (single realization).
    """
    fn = f'{index}_1995-2014_era5.nc'
    da = xr.open_dataset(os.path.join(base_path, fn), decode_timedelta=False, decode_times=time_coder)[f'{index}']
    return da