import io
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from shiny import render, reactive, req
from shiny import ui as core_ui
from shiny.express import input, ui
from shiny.types import SafeException

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, '..', 'code'))

from core.io_functions import load_data
from core.core_functions import aggregate_members, mask_domain, cut_region
from core.mapplot_functions import plot_map_base
from core.utils import index_acronym_map, index_longname_map, index_explanation_map


def Mauritsen2019():
    url = "https://doi.org/10.1029/2018MS001400"
    return ui.tags.a("2019", href=url, target='_blank')

def Olonscheck2023():
    url = "https://doi.org/10.1029/2023MS003790"
    return ui.tags.a("2023", href=url, target='_blank')

def ETCCDI():
    url = "https://etccdi.pacificclimate.org/list_27_indices.shtml"
    return ui.tags.a("ETCCDI", href=url, target='_blank')

def url_git():
    url = "https://github.com/lukasbrunner/etccdi_internal_variability"
    return ui.tags.a("GitHub", href=url, target='_blank')

def url_by():
    url = "https://creativecommons.org/licenses/by/4.0/"
    return ui.tags.a("CC BY", href=url, target='_blank')


ui.panel_title("Variability Atlas for ETCCDI climate extreme indices")
ui.p("For more information, see the sidebar (click '>' top left) or the accompanying publication TODO: add link once published.")
ui.HTML('<div style="height:0.75rem"></div>')


# Add a bit of inner spacing for card contents
ui.tags.style('''
.bslib-card {
    padding: 0.5rem 1rem;
}
''')

with ui.layout_column_wrap(width=.5):
    with ui.card():
        ui.input_select(
            "index",
            ui.tags.span(
                "Extreme index: ",
                core_ui.tooltip(
                    ui.tags.span("ⓘ", style="cursor: help;"),
                    f"TXx – {index_longname_map['txx']}: {index_explanation_map['txx']}",  # default index; kept in sync below
                    id="index_info",
                ),
            ),
            index_acronym_map,
        )
        ui.input_select(
            "aggregation",
            "Member aggregation:",
            {"mean": "Mean",
            "median": "Median",
            "max": "Maximum",
            "min": "Minimum",
            "std": "Standard deviation",
            "cv": "Coefficient of variation",
            },
            selected='std'
        )

        ui.input_switch(
            "mask_ocean",
            "Mask ocean",
            False,
        )

        ui.input_switch(
            'celsius',
            ui.tags.span(
                "Temperature in °C ",
                core_ui.tooltip(
                    ui.tags.span("ⓘ", style="cursor: help;"),
                    "Only affects temperature-based indices. "
                    "The coefficient of variation is always computed with temperatures in Kelvin "
                    "(see the accompanying publication for details).",
                ),
            ),
            True,
        )

    with ui.card():
        ui.p("Longitude extent (either [-180, 180] or [0, 360] convention)")
        with ui.layout_column_wrap(width=.5):
            ui.input_numeric("lon_min", "Minimum", -180)
            ui.input_numeric("lon_max", "Maximum", 180)

        ui.p("Latitude extent")
        with ui.layout_column_wrap(width=.5):
            ui.input_numeric("lat_min", "Minimum", -90, min=-90, max=90)
            ui.input_numeric("lat_max", "Maximum", 90, min=-90, max=90)

# explicit spacer between the two card groups (guaranteed gap)
ui.HTML('<div style="height:1rem"></div>')

with ui.layout_column_wrap(width=1):
    with ui.card():
        ui.input_switch("plot_options", "Manual plot options", False)
        with ui.panel_conditional("input.plot_options"):
            ui.input_numeric("levels", "Colorbar levels:", 10)
            ui.input_numeric("min", "Colorbar minimum", None)
            ui.input_numeric("max", "Colorbar maximum", None)
            ui.input_text("cmap", "Colormap", "viridis")

with ui.sidebar(open='closed'):
    ui.HTML("<b>Data and Methods</b>")
    ui.div(
        "The Variability Atlas is based on data from the global climate model MPI-ESM1.2 (CMIP6 configuration) as described ",
        "in Mauritsen et al. (", Mauritsen2019(), ") and Olonscheck et al. (", Olonscheck2023(), "). ",
        "It includes 26 extreme climate indices as defined by the ", ETCCDI(), " (all core indices except the user-defined Rnnmm), ",
        "based on daily maximum and minimum temperature and daily precipitation. ",
        "For each of the 50 initial-condition ensemble members, the annual indices are averaged over the 20-year period 1995-2014; ",
        "relative-threshold indices use the 1961-1990 base period. ",
        "The atlas provides different metrics of the spread across the members, which isolates the effect of internal climate variability. ",
        "For more details, please see the accompanying publication (TODO: add link once published)."
    )


@reactive.calc
def calc_data():
    req(input.lon_min() is not None, input.lon_max() is not None,
        input.lat_min() is not None, input.lat_max() is not None)
    # CV is always computed from Kelvin (the app's celsius setting only affects the other statistics)
    da = load_data(input.index(), celsius=input.celsius() and input.aggregation() != 'cv')
    try:
        da = aggregate_members(da, input.aggregation())  # defaults to member mean
    except ValueError as e:
        raise SafeException(str(e)) from e
    da = cut_region(da, lat_bounds=[input.lat_min(), input.lat_max()], lon_bounds=[input.lon_min(), input.lon_max()])
    req(da.size > 0)
    if input.mask_ocean():
        da = mask_domain(da)
    return da


def make_figure(dpi=None):
    return plot_map_base(
        calc_data(),
        cmap=input.cmap() if input.plot_options() and input.cmap() in plt.colormaps() else 'viridis',
        levels=input.levels() if input.plot_options() else 10,
        vmin=input.min() if input.plot_options() else None,
        vmax=input.max() if input.plot_options() else None,
        nice_colorbar=False,
        dpi=dpi,
    )


with ui.div(style="max-width: 900px; margin: 0 auto;"):
    with ui.hold():
        @render.plot()
        def plot():
            fig, _, _ = make_figure()
            return fig
    core_ui.output_plot("plot", hover=True)

    with ui.div(style="text-align: center; min-height: 1.5rem; font-size: 0.875rem; opacity: 0.7;"):
        @render.text
        def hover_info():
            h = input.plot_hover()
            if not h:
                return ""
            da = calc_data()
            # hover x is in projection coordinates: shift by the central longitude
            # (same rule as in plot_map_base) and wrap into the data's lon convention
            central_lon = da['lon'].mean().item() if da['lon'].min() > 45 and da['lon'].max() > 180 else 0
            lon = h['x'] + central_lon
            lon = lon % 360 if da['lon'].max() > 180 else (lon + 180) % 360 - 180
            val = da.sel(lon=lon, lat=h['y'], method='nearest')
            unit = da.attrs.get('units', '')
            value = 'no data' if np.isnan(val) else f"{float(val):.2f}{'' if unit == '-' else ' ' + unit}"
            return f"lat {val['lat'].item():.1f}°, lon {val['lon'].item():.1f}°: {value}"


with ui.div(style="display: flex; flex-wrap: wrap; gap: 1rem; justify-content: center; align-items: center; margin-top: 0.5rem;"):
    @render.download(filename=lambda: f"{input.index()}_{input.aggregation()}.png", label='Download plot', media_type='image/png')
    def download_plot():
        fig, _, _ = make_figure(dpi=300)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=300, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        yield buf.getvalue()

    @render.download(filename=lambda: f"{input.index()}_{input.aggregation()}.nc", label='Download data', media_type='application/x-netcdf')
    def download():
        yield calc_data().to_netcdf(None)


# keep the index explanation tooltip in sync with the selected index
@reactive.effect
def _update_index_info():
    idx = input.index()
    ui.update_tooltip(
        "index_info",
        f"{index_acronym_map[idx]} – {index_longname_map[idx]}: {index_explanation_map[idx]}",
    )


ui.div(
    url_by(), " Lukas Brunner; Source code on ", url_git(),
    style="margin-top: 3rem; padding-top: 0.75rem; border-top: 1px solid rgba(128, 128, 128, .35); text-align: center; font-size: 0.875rem;",
)
