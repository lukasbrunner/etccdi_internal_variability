import numpy as np
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

# def _cbar_defaults(kwargs):
#     cbar_kwargs_defaults = {'label': '', 'fraction': .024, 'pad': .01}
#     cbar_kwargs_defaults.update(kwargs.get('cbar_kwargs', {}))
#     kwargs['cbar_kwargs'] = cbar_kwargs_defaults
#     return kwargs


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


def plot_map_base(da, ax=None, nice_colorbar=True, **kwargs):
    # kwargs = _cbar_defaults(kwargs)
    kwargs = _cmap_defaults(kwargs)

    if ax is None:
        central_longitude = 0
        if da['lon'].min() > 45 and da['lon'].max() > 180:
            central_longitude = da['lon'].mean().item()
        fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree(central_longitude=central_longitude)})

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