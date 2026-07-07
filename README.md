# Variability Atlas

Code companion to *"The Variability Atlas: How internal climate variability
affects the estimation of climate extremes indices"* (Brunner et al., University
of Hamburg). The paper uses a 50-member MPI-ESM1.2 large ensemble to quantify
how internal climate variability affects the 26 core ETCCDI climate extreme
indices, and introduces the *Variability Atlas* as a tool for exploring this
effect interactively.

Shiny app: https://019bbc84-e87b-2fef-8974-9e37125a2551.share.connect.posit.cloud/

## The three layers of the Variability Atlas

Per the manuscript, the Atlas is provided in three layers of increasing
flexibility:

1. **A browser-based Shiny app** ([`code/app.py`](code/app.py)) — pick an
   index, a member-aggregation statistic (mean/median/min/max/std/CV), a
   region, and get a map, no coding required.
2. **This repository** — the Python scripts, notebooks, and pre-computed
   20-year (1995-2014) mean climatologies (`data/`) used for the paper
   figures and the app.
3. **The full 1850-2014 time series** for all indices/members, to be
   published separately on the World Data Center for Climate (WDC Climate) —
   not part of this repository.

Raw daily ETCCDI indices themselves are computed from the MPI-ESM output
externally via CDO, in a separate repository
([`etccdi_cdo`](https://github.com/lukasbrunner/etccdi_cdo)). This repo
consumes those per-member NetCDF files (external cluster path, not tracked
here) to build the climatologies in `data/`.

## Repo layout

- [`code/app.py`](code/app.py) — the Shiny app (layer 1 above).
- [`code/core/`](code/core/) — library modules imported by the app, the
  climatology scripts, and the notebooks:
  - `core_functions.py` — area/member aggregation (mean/std/CV) and region
    selection/masking.
  - `io_functions.py` — loads the pre-computed climatologies from `data/`.
  - `mapplot_functions.py` — the shared map-plotting style.
  - `boxplot_functions.py` — regional boxplot helper.
  - `calc_means.py` — builds the 20-year mean climatologies from the raw
    per-member index files (used by `generate_climatologies*.py`).
  - `calc_seasonal_prcptot.py` — ancillary DJF/JJA seasonal PRCPTOT
    climatology (not part of the manuscript's main analysis).
  - `utils.py` — per-index metadata (unit, acronym, plain-language
    description).
- [`code/generate_climatologies.py`](code/generate_climatologies.py) /
  [`generate_climatologies_era5.py`](code/generate_climatologies_era5.py) —
  regenerate the files in `data/` from the raw MPI-GE / ERA5 index files.
- `code/*.ipynb` — paper-figure and analysis notebooks (kept as notebooks
  intentionally): `figures_paper.ipynb` (main figures), `interactive.ipynb`
  and `reviewer2_plots.ipynb` (exploratory/response-to-reviewer examples),
  `supplement_*.ipynb` (supplementary figures, including the MMLEA-v2
  large-ensemble comparison).
- [`code/download_mmleav2/`](code/download_mmleav2/) — notebooks to download
  the MMLEA-v2 comparison ensemble data used in the supplement.
- `data/` — pre-computed 20-year mean climatologies, one NetCDF file per
  index (plus DJF/JJA and ERA5 variants).
- `figures/` — the rendered paper figures.

## Running things

Install the dependencies from `requirements.txt`. All `core.*` imports assume
the working directory is `code/` (this is how Jupyter runs notebooks that
live directly in `code/`, and how the scripts below are meant to be invoked):

```bash
cd code
python generate_climatologies.py       # regenerate data/*_1995-2014.nc
python generate_climatologies_era5.py  # regenerate the ERA5 reference file(s)
shiny run app.py                       # run the app locally
```

## Known issues

A few pre-existing issues in the (intentionally untouched) analysis
notebooks, noted here rather than fixed in place:

- `interactive.ipynb`'s boxplot section calls `plot_box_base(...)` /
  `aggregate_area(...)` without importing either — raises `NameError` on a
  fresh kernel.
- `reviewer2_plots.ipynb` labels a North-America longitude box
  (`[-155, -55]`) as "select Europe" in several comments.
