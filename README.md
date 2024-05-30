# Apply CWD algorithm globally

*Author: Benjamin Stocker*

## Workflow

### Prepare forcing

`analysis/make_tidy_<DATAPRODUCT>.R`

{rsofun} requires model forcing data as *tidy* data frames. The first step is to make climate and EO forcing data, commonly provided as NetCDF, tidy. That is, data is saved as nested data frames, where each row is one grid cell and time series is nested in the column `data`. 


2.  `analysis/apply_cwd_global.R`: Script for parallel applying the CWD algorithm (or anything else that operates on full time series) separately for on gridcell. Distributes by longitudinal band. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>.rds`

    `src/apply_cwd_global.sh`: Bash script that calls `apply_cwd_global.R` , to be used as an alternative and containing submission statement for HPC.

    Note: This step creates data at the original temporal resolution. Data is not collected at this stage to avoid memory limitation.

3.  `analysis/get_cwd_annmax.R`: Script for parallel applying function for determining annual maximum. Reading files written in previous step and writing files (in the example):

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_<ilon>_ANNMAX.rds`

4.  `collect_cwd_annmax.R`: Script for collecting annual time series of each gridcell - is much smaller data and can now be handled by reading all into memory. Writes file containing global data with annual resolution:

    `~/data/cmip6-ng/tidy/cwd/<fileprefix>_cum_ANNMAX.rds`

5.  `analysis/create_nc_annmax.R`: Script for writing the global annual data into a NetCDF file. This uses the function `write_nc2()` from the package {rgeco}. Install it from [here](https://github.com/geco-bern/rgeco). Writes file containing global data with annual resolution as NetCDF:

    `~/data/cmip6-ng/tidy/cwd/evspsbl_cum_ANNMAX.nc`

**Note**: Adjust paths and file name prefixes for your own case in scripts (located in subdirectory `analysis/`)
