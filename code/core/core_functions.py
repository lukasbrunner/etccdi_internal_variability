import numpy as np
import regionmask

def aggregate_area(da):
    if not 'lat' in da.coords:
        return da
    return da.weighted(np.cos(np.deg2rad(da['lat']))).mean(('lat', 'lon'), keep_attrs=True)
    
    
def set_longitude_convention(da, convention='180'):
    da = da.copy()
    if convention == '180':
        da.coords['lon'] = (da.coords['lon'] + 180) % 360 - 180
    elif convention == '360':
        da.coords['lon'] = da.coords['lon'] % 360
    else:
        raise ValueError
    return da.sortby(da['lon'])


def mask_domain(da, mask='ocean'):
    land = regionmask.defined_regions.natural_earth_v5_0_0.land_110.mask_3D(da).squeeze()
    if mask == 'ocean':
        return da.where(land)
    if mask == 'land':
        return da.where(~land)
    if mask is None:
        return da
    raise ValueError(mask)


def cut_region(da, lon_bounds=None, lat_bounds=None):
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
            raise ValueError
            
        attrs = da.attrs
        da = da.std('member') / mean
        da.attrs = attrs
        da.attrs['long_name'] = '{} members coefficient of variation'.format(
            da.attrs.get('long_name', da.name))
        da.attrs['units'] = '1'
        return da


def get_representative_member(da, select_by='mean'):
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