import io
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from shiny import render, reactive
from shiny.express import input, ui

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, 'code'))

from core.io_functions import load_data
from core.core_functions import aggregate_members, mask_domain, cut_region
from core.mapplot_functions import plot_map_base
from core.utils import index_acronym_map

ui.panel_title("Shiny test suit for ETCCDI - LE paper")
ui.p("For more information, see the sidebar (click '>' top left) or the accompanying publication TODO: add link.")
ui.HTML('<div style="height:0.75rem"></div>')


# Custom CSS styles added by AI
# Add a bit of inner horizontal spacing for card contents
ui.tags.style('''
.bslib-card {
    padding-left: 1rem;
    padding-right: 1rem;
    padding-top: 0.5rem;
    padding-bottom: 0.5rem;
}
/* center download links and add spacing between them */
.shiny-download-link {
    display: block;
    margin-left: auto;
    margin-right: auto;
    margin-bottom: 0.75rem; /* general gap after download links */
}
/* larger explicit gap between plot and data download */
#download_plot {
    display: block;
    width: 50vw; /* half the viewport width */
    max-width: 900px; /* optional cap for very wide screens */
    min-width: 180px; /* keep usable on very small screens */
    margin: 0 auto 1.0rem auto; /* bottom gap between plot and data download */
}
#download {
    display: block;
    width: 50vw; /* half the viewport width */
    max-width: 900px; /* optional cap for very wide screens */
    min-width: 180px; /* keep usable on very small screens */
    margin: 0.25rem auto; /* breathing room for data download */
}
''')

# ui.input_switch("advanced", "Advanced options", False) 

with ui.layout_column_wrap(width=.5):
    with ui.card():
        ui.input_select(  
            "index",  
            "Extreme index:", 
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
            """
            Temperature in °C (default Kelvin)
            Note that this only affects temperature-based for indices. 
            The coefficient of variation is not available for all indices in °C.
            """,
            False,
            )

    with ui.card():
        # Longitude extent
        ui.p("Longitude extent (either [-180, 180] or [0, 360] convention)")
        with ui.layout_column_wrap(width=.5):
            with ui.card():
                ui.input_numeric("lon_min", "Minimum", -180)
            with ui.card():
                ui.input_numeric("lon_max", "Maximum", 180)
        # ui.p("Note that either [-180, 180] or [0, 360] conventions are possible.")

        ui.p("Latitude extent")
        with ui.layout_column_wrap(width=.5):
            with ui.card():
                ui.input_numeric("lat_min", "Minimum", -90)
            with ui.card():
                ui.input_numeric("lat_max", "Maximum", 90)

        # ui.input_slider(
        #     "lon_range",
        #     "Longitude range",
        #     min=0,
        #     max=360,
        #     value=[0, 360],
        # )

        # ui.input_slider(
        #     "lat_range", 
        #     "Latitude range", min=-90, max=90, 
        #     value=[-90, 90],
        #     ) 

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
            # ui.input_slider("cbar_fraction", "Colorbar size", value=.02, min=0, max=.1)  
# added by AI
# Reset manual plot option inputs to their defaults when the switch is disabled
@reactive.Effect
def _reset_manual_options():
    if not input.plot_options():
        # Try to obtain a session object in a few ways depending on shiny version
        session = None
        try:
            if hasattr(reactive, "get_current_session"):
                session = reactive.get_current_session()
        except Exception:
            session = None

        if session is None:
            try:
                # some versions expose get_current_session at top-level
                from shiny import get_current_session as _gcs

                try:
                    session = _gcs()
                except Exception:
                    session = None
            except Exception:
                session = None

        if session is None:
            return

        # set inputs back to sensible defaults
        try:
            session.set_input_value("levels", 10)
            session.set_input_value("min", None)
            session.set_input_value("max", None)
            session.set_input_value("cmap", "viridis")
        except Exception:
            # fail silently if the session API is not available
            pass

        
# nr_members = 50
# with ui.panel_conditional("input.advanced"):
#     # if input.advanced():
#     with ui.layout_column_wrap(width=1):
#         with ui.card():
#             ui.input_slider(
#             "nr_members", 
#             "Number of members", min=1, max=50, 
#             value=nr_members,
#             )
#         nr_members = input.nr_members()


with ui.sidebar(open='closed'):  
    ui.HTML("<b>Data and Methods</b>")
    ui.HTML("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.")


def calc_data():
    da = load_data(input.index(), celsius=input.celsius())
    tmp = aggregate_members(da, input.aggregation())  # defaults to member mean
    # tmp = cut_region(tmp, lat_bounds=input.lat_range(), lon_bounds=input.lon_range())
    tmp = cut_region(tmp, lat_bounds=[input.lat_min(), input.lat_max()], lon_bounds=[input.lon_min(), input.lon_max()])
    # tmp = set_temperature_unit(tmp, to_celsius=input.celsius())
    if input.mask_ocean():
        tmp = mask_domain(tmp)
    return tmp


@render.plot()  
def plot():
    tmp = calc_data()
    fig, ax, _ = plot_map_base(
        tmp, 
        cmap=input.cmap() if input.plot_options() else 'viridis', 
        levels=input.levels() if input.plot_options() else 10,
        vmin=input.min() if input.plot_options() else None,
        vmax=input.max() if input.plot_options() else None,
        # cbar_kwargs={'fraction': input.cbar_fraction()} if input.plot_options() else {}
        nice_colorbar=False,
    )
    # ax.set_extent([*input.lon_range(), *input.lat_range()])
    return fig


@render.download(filename="plot.png", label='Download plot', media_type='image/png')
def download_plot():
    tmp = calc_data()
    # create the same figure as the plot output
    fig, _, _ = plot_map_base(
        tmp, 
        dpi=150,
        cmap=input.cmap() if input.plot_options() else 'viridis', 
        levels=input.levels() if input.plot_options() else 10,
        vmin=input.min() if input.plot_options() else None,
        vmax=input.max() if input.plot_options() else None,
        nice_colorbar=False,
        # cbar_kwargs={'fraction': input.cbar_fraction()} if input.plot_options() else {}
    )
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    yield buf.getvalue()


@render.download(filename="file.nc", label='Download data', media_type='nc')
def download():
    tmp = calc_data()
    yield tmp.to_netcdf(None)

def url_git():
    url = "https://github.com/lukasbrunner/shiny_test"
    return ui.tags.a("GitHub", href=url, target='_blank')

def url_by():
    url = "https://creativecommons.org/licenses/by/4.0/"
    return ui.tags.a("CC BY", href=url, target='_blank')

@render.ui
def text(): 
    return ui.div(
        url_by(), 
        " Lukas Brunner; Source code on ", 
        url_git()
        )
