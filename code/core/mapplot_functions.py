"""Map-plotting helpers built on cartopy's PlateCarree projection."""
import numpy as np
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs


def _cmap_defaults(kwargs):
    kwargs_defaults = {'levels': 10}
    kwargs_defaults.update(kwargs)
    return kwargs_defaults
    

def _title_default(ax, da):
    ax.set_title(
        '{varn} ({unit})'.format(
            varn=da.attrs.get('long_name', da.name), 
            unit=da.attrs.get('units', 'MISS')
        )
    )


def _add_nice_colorbar(p):
    cax = plt.gcf().add_axes(
        [p.axes.get_position().x1+0.01, p.axes.get_position().y0,0.02, p.axes.get_position().height])
    plt.colorbar(p, cax=cax)


def plot_map_base(da, ax=None, nice_colorbar=True, dpi=None, **kwargs):
    """Plot a 2D (lat, lon) DataArray as a pcolormesh map.

    Auto-picks a central longitude for [0, 360]-convention data.

    Parameters
    ----------
    da : xarray.DataArray
        2D field with ``lat``/``lon`` coordinates.
    ax : matplotlib.axes.Axes, optional
        Existing (cartopy) axes to plot into. Default is ``None``, which
        creates a new figure with a ``PlateCarree`` projection.
    nice_colorbar : bool, optional
        Add a manually-sized colorbar with matched height rather than
        matplotlib's default. Default is ``True``.
    dpi : int, optional
        Figure resolution, only used when ``ax`` is ``None``. Default is
        ``None``.
    **kwargs
        Passed through to xarray's ``.plot.pcolormesh`` (e.g. ``cmap``,
        ``levels``, ``vmin``, ``vmax``).

    Returns
    -------
    fig : matplotlib.figure.Figure
    ax : matplotlib.axes.Axes
    p : matplotlib.collections.QuadMesh
        The pcolormesh artist.
    """
    kwargs = _cmap_defaults(kwargs)

    if ax is None:
        central_longitude = 0
        if da['lon'].min() > 45 and da['lon'].max() > 180:
            central_longitude = da['lon'].mean().item()
        fig, ax = plt.subplots(dpi=dpi, subplot_kw={'projection': ccrs.PlateCarree(central_longitude=central_longitude)})
    else:
        fig = ax.get_figure()

    p = da.plot.pcolormesh(
        ax=ax,
        transform=ccrs.PlateCarree(),
        add_colorbar=not nice_colorbar,  # add manually to ensure nice height
        robust=True,
        **kwargs,
    )

    if nice_colorbar:
        _add_nice_colorbar(p)
    p.axes.coastlines(lw=.5)
    _title_default(p.axes, da)
    return fig, ax, p