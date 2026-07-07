"""Boxplot of ensemble-member values aggregated over a region."""
import numpy as np
import seaborn as sns

from core.core_functions import aggregate_area


def _get_region_string(da):
    """Format a lon/lat bounding box as a short label string."""
    lon_min = da['lon'].min().item()
    lon_max = da['lon'].max().item()
    lat_min = da['lat'].min().item()
    lat_max = da['lat'].max().item()
    return f'{lon_min:.2f}-{lon_max:.2f}E {lat_min:.2f}-{lat_max:.2f}N'


def plot_box_base(da):
    """Draw a boxplot of ensemble-member values (area-weighted mean) for a region.

    A secondary y-axis shows the same values normalized by the ensemble mean.

    Parameters
    ----------
    da : xarray.DataArray
        Field with ``member`` and ``lat``/``lon`` dimensions, already cut
        to the region of interest.

    Returns
    -------
    matplotlib.axes.Axes
        The boxplot axes.
    """
    region_string = _get_region_string(da)
    da = aggregate_area(da)
    
    ax = sns.boxplot(da, width=.5)
    ax.set_xlim(-.5, .5)
    ax.set_xticks([])
    ax.set_title('{varn} ({unit} / 1): {region}'.format(
        varn=da.attrs.get('long_name', da.name), 
        unit=da.attrs.get('units', 'MISS'),
        region=region_string
    ))
    axr = ax.twinx()
    axr.set_ylim(np.array([*ax.get_ylim()]) / da.mean('member').item())
    return ax

    