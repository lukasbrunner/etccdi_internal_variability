"""Core data-transformation utilities: area/member aggregation and region selection."""
import numpy as np
import regionmask


def aggregate_area(da):
    """Cosine-latitude-weighted spatial mean over ``lat``/``lon`` (no-op if absent).

    Parameters
    ----------
    da : xarray.DataArray
        Field with ``lat`` and ``lon`` coordinates (degrees). Returned
        unchanged if ``lat`` is not a coordinate.

    Returns
    -------
    xarray.DataArray
        ``da`` averaged over ``lat`` and ``lon``, weighted by cos(latitude).
    """
    if not 'lat' in da.coords:
        return da
    return da.weighted(np.cos(np.deg2rad(da['lat']))).mean(('lat', 'lon'), keep_attrs=True)


def set_longitude_convention(da, convention='180'):
    """Rewrap ``lon`` to the ``'180'`` ([-180, 180]) or ``'360'`` ([0, 360]) convention.

    Parameters
    ----------
    da : xarray.DataArray
        Field with a ``lon`` coordinate (degrees).
    convention : {'180', '360'}, optional
        Target longitude convention. Default is ``'180'``.

    Returns
    -------
    xarray.DataArray
        ``da`` with ``lon`` rewrapped and sorted ascending.
    """
    da = da.copy()
    if convention == '180':
        da.coords['lon'] = (da.coords['lon'] + 180) % 360 - 180
    elif convention == '360':
        da.coords['lon'] = da.coords['lon'] % 360
    else:
        raise ValueError
    return da.sortby(da['lon'])


def mask_domain(da, mask='ocean'):
    """Mask out ``'ocean'`` or ``'land'`` grid cells (Natural Earth land mask); ``None`` is a no-op.

    Parameters
    ----------
    da : xarray.DataArray
        Field with ``lat``/``lon`` coordinates.
    mask : {'ocean', 'land', None}, optional
        Which cells to mask out. ``'ocean'`` keeps land only, ``'land'``
        keeps ocean only, ``None`` returns ``da`` unchanged. Default is
        ``'ocean'``.

    Returns
    -------
    xarray.DataArray
        ``da`` with the excluded domain set to NaN.
    """
    land = regionmask.defined_regions.natural_earth_v5_0_0.land_110.mask_3D(da).squeeze()
    if mask == 'ocean':
        return da.where(land)
    if mask == 'land':
        return da.where(~land)
    if mask is None:
        return da
    raise ValueError(mask)


def cut_region(da, lon_bounds=None, lat_bounds=None):
    """Subset to a lat/lon box; ``lon_bounds`` may use either [-180, 180] or [0, 360].

    Parameters
    ----------
    da : xarray.DataArray
        Field with ``lat``/``lon`` coordinates.
    lon_bounds : sequence of float, optional
        ``(min, max)`` longitude bounds, in either the [-180, 180] or
        [0, 360] convention (detected automatically). Default is ``None``
        (no longitude subsetting).
    lat_bounds : sequence of float, optional
        ``(min, max)`` latitude bounds. Default is ``None`` (no latitude
        subsetting).

    Returns
    -------
    xarray.DataArray
        ``da`` subset to the requested region.
    """
    if lon_bounds is None and lat_bounds is None:
        return da
    if lon_bounds is None:
        return da.sel(lat=slice(*lat_bounds))

    da = set_longitude_convention(da, '180')  # default: [-180, 180] convention of bounds
    if np.max(lon_bounds) > 180:
        da = set_longitude_convention(da, '360')

    if lat_bounds is None:
        return da.sel(lon=slice(*lon_bounds))
    return da.sel(lon=slice(*lon_bounds), lat=slice(*lat_bounds))
    
    
    
def aggregate_members(da, method='mean'):
    """Reduce the ``member`` dim to one of mean/median/min/max/a quantile/std/cv.

    ``'cv'`` (coefficient of variation, std/mean) is returned as a unitless
    fraction, matching the manuscript's convention of not using percent (to
    avoid confusion with percent-based indices like TX90p). CV is undefined
    for fields with a negative ensemble mean (e.g. temperature in Celsius) and
    raises a ``ValueError`` in that case; convert to Kelvin first.

    Parameters
    ----------
    da : xarray.DataArray
        Field with a ``member`` dimension.
    method : {'mean', 'median', 'min', 'max', 'std', 'cv'} or float, optional
        Aggregation statistic; a float in [0, 1] computes that quantile
        across members. Default is ``'mean'``.

    Returns
    -------
    xarray.DataArray
        ``da`` reduced over ``member``, with ``long_name`` (and, for
        ``'cv'``, ``units``) updated accordingly.
    """
    if method == 'mean':
        da = da.mean('member', keep_attrs=True)
        da.attrs['long_name'] = '{} members mean'.format(
            da.attrs.get('long_name', da.name))
        return da
    if method == 'median':
        da = da.median('member', keep_attrs=True)
        da.attrs['long_name'] = '{} members median'.format(
            da.attrs.get('long_name', da.name))
        return da
    if method == 'min':
        da = da.min('member', keep_attrs=True)
        da.attrs['long_name'] = '{} members minimum'.format(
            da.attrs.get('long_name', da.name))
        return da
    if method == 'max':
        da = da.max('member', keep_attrs=True)
        da.attrs['long_name'] = '{} members maximum'.format(
            da.attrs.get('long_name', da.name))
        return da
    if isinstance(method, (int, float)):
        da = da.quantile(method, 'member', keep_attrs=True)
        da.attrs['long_name'] = '{} members perc{}'.format(
            da.attrs.get('long_name', da.name),
            int(method * 100)) 
    if method == 'std':
        da = da.std('member', keep_attrs=True)
        da.attrs['long_name'] = '{} members standard deviation'.format(
            da.attrs.get('long_name', da.name))
        return da
    if method == 'cv':
        mean = da.mean('member')
        
        # NOTE: cv is not well defined for negative means
        # this happens if temperature is in degC
        if np.any(mean < 0):
            raise ValueError(
                'The coefficient of variation is not well defined for fields with '
                'negative values. For temperature indices use Kelvin instead of °C.')
            
        attrs = da.attrs
        da = da.std('member') / mean
        da.attrs = attrs
        da.attrs['long_name'] = '{} members coefficient of variation'.format(
            da.attrs.get('long_name', da.name))
        da.attrs['units'] = '-'
        return da


def get_representative_member(da, select_by='mean'):
    """Pick the ensemble member whose area-weighted regional value is closest to
    the ensemble's mean/median/min/max/a quantile (e.g. the "median member" used
    for single-realization illustration in Figure 1a).

    Parameters
    ----------
    da : xarray.DataArray
        Field with ``member`` and ``lat``/``lon`` dimensions.
    select_by : {'mean', 'median', 'min', 'max'} or float, optional
        Statistic (across members, of the area-weighted mean) that the
        selected member's value should be closest to; a float in [0, 1]
        targets that quantile. Default is ``'mean'``.

    Returns
    -------
    xarray.DataArray
        ``da`` indexed to the single closest-matching member.
    """
    tmp = aggregate_area(da)
    if select_by == 'mean':
        target = tmp.mean('member')
    elif select_by == 'median':
        target = tmp.median('member')
    elif select_by == 'min':
        target = tmp.min('member')
    elif select_by == 'max':
        target = tmp.max('member')
    elif isinstance(select_by, (int, float)):
        target = tmp.quantile(select_by, 'member')
    else:
        raise ValueError
    return da.isel(member=np.abs(tmp - target).argmin('member'))