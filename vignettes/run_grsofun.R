#' Global simulation using grsofun
#'
#' Demonstrates a full global rsofun simulation, using WATCH-WFDEI climate and
#' MODIS fAPAR forcing. This script prepares tidy input files, runs the P-model
#' over a global grid, and visualizes GPP output as monthly maps.
#'
#' @details The grsofun framework allows running {rsofun} over large gridded
#'   inputs by first tidying NetCDF inputs into gridcell-specific time series,
#'   running simulations in parallel, and collecting outputs back into a tidy
#'   structure.
#'
# -----------------------------------------------------------
# Global rsofun simulation with grsofun: WATCH-WFDEI + MODIS
# -----------------------------------------------------------

library(BayesianTools)
library(ggplot2)
library(dplyr)
library(rnaturalearth)
library(cowplot)
library(map2tidy)
library(purrr)
library(parallel)
library(here)
library(sf)
library(tictoc)
library(terra)

message("Starting program..")


# Load all R scripts from the R/ directory
source_files <- list.files(here::here("R/"), pattern = "*.R$")
purrr::walk(paste0(here::here("R/"), source_files), source)

# -----------------------------------------------------------
# Model and I/O configuration
# -----------------------------------------------------------

settings <- list(
  ### simulation config:
  fileprefix = "test",
  model = "pmodel",
  year_start = 2018,  # xxx not yet handled
  year_end = 2018,    # xxx not yet handled
  spinupyears = 10,
  recycle = 1,

  ### HPC config:
  nnodes = 1,
  ncores_max = 1, # parallel::detectCores(),

  ### final model output
  dir_out = "/storage/research/giub_geco/data_2/scratch/akurth/grsofun_output/",
  dir_out_nc = "/storage/research/giub_geco/data_2/scratch/akurth/grsofun_output/",
  save = list(gpp = "mon"),
  overwrite = FALSE,

  ### tidy model input config:
  grid = list(
    lon_start = -179.75,
    dlon      = 0.5,
    len_ilon  = 720,
    lat_start = -89.75,
    dlat      = 0.5,
    len_ilat  = 360
  ),

  # Source
  source_fapar = "modis",
  source_climate = "watch-wfdei",

  # Dir in
  dir_in_ssr = "/storage/research/giub_geco/data_2/scratch/akurth/ERA5Land/remap",
  dir_in_str = "/storage/research/giub_geco/data_2/scratch/akurth/ERA5Land/remap",
  file_in_canopy = "/storage/research/giub_geco/data_2/scratch/akurth/vegheight_lang_2023/canopy_0.5deg_avg.tif",
  file_in_co2 = "/storage/research/giub_geco/data_2/scratch/akurth/global/co2_annmean_mlo.csv",
  dir_in_climate = "/storage/research/giub_geco/data_2/scratch/akurth/wfdei_weedon_2014/data",
  file_in_fapar = "/storage/research/giub_geco/data_2/scratch/akurth/MODIS-C006_MOD15A2_LAI_FPAR_zmaw/MODIS-C006_MOD15A2__LAI_FPAR__LPDAAC__GLOBAL_0.5degree__UHAM-ICDC__2000_2018__MON__fv0.02.nc",
  file_in_whc = "/storage/research/giub_geco/data_2/scratch/fbernhard/whc_stocker_2023/data/remap/cwdx80_forcing_0.5degbil.nc",
  file_in_landmask = "/storage/research/giub_geco/data_2/scratch/akurth/wfdei_weedon_2014/data/WFDEI-elevation.nc",
  file_in_elv = "/storage/research/giub_geco/data_2/scratch/akurth/wfdei_weedon_2014/data/WFDEI-elevation.nc",

  # Dir out
  dir_out_tidy_ssr = "/storage/research/giub_geco/data_2/scratch/akurth/ERA5Land/remap/tidy",
  dir_out_tidy_str = "/storage/research/giub_geco/data_2/scratch/akurth/ERA5Land/remap/tidy",

  dir_out_drivers = "/storage/research/giub_geco/data_2/scratch/akurth/grsofun_input",
  dir_out_tidy_climate = "/storage/research/giub_geco/data_2/scratch/akurth/watch_wfdei/tidy",
  dir_out_tidy_fapar = "/storage/research/giub_geco/data_2/scratch/akurth/MODIS-C006_MOD15A2_LAI_FPAR_zmaw/tidy",
  dir_out_tidy_whc = "/storage/research/giub_geco/data_2/scratch/akurth/mct_data/tidy",
  dir_out_tidy_landmask = "/storage/research/giub_geco/data_2/scratch/akurth/watch_wfdei/tidy",
  dir_out_tidy_elv = "/storage/research/giub_geco/data_2/scratch/akurth/watch_wfdei/tidy"
)

# -----------------------------------------------------------
# Model parameters
# -----------------------------------------------------------

par <- list(
  kphio = 5.000000e-02,
  # chosen to be too high for demonstration
  kphio_par_a = -2.289344e-03,
  kphio_par_b = 1.525094e+01,
  soilm_thetastar = 1.577507e+02,
  soilm_betao = 1.169702e-04,
  beta_unitcostratio = 146.0,
  rd_to_vcmax = 0.014,
  tau_acclim = 20.0,
  kc_jmax = 0.41
)

# -----------------------------------------------------------
# Preprocess tidy input data from NetCDF
# -----------------------------------------------------------

tictoc::tic("Tidying input")
tidy_out <- grsofun_tidy(settings)
tictoc::toc()
gc()

# -----------------------------------------------------------
# Run grsofun model simulation
# -----------------------------------------------------------
tictoc::tic("Run model")
error <- grsofun_run(par, settings)
tictoc::toc()
gc()

# -----------------------------------------------------------
# Collect model output data
# -----------------------------------------------------------

tictoc::tic("Collect model output")
df <- grsofun_collect(settings, return_data = TRUE)
tictoc::toc()
gc()

# -----------------------------------------------------------
# Generate example GPP map plots for Jan & Jul and save
# -----------------------------------------------------------

coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# January
gg1 <- df |>
  filter(month == 1) |>
  ggplot() +
  geom_raster(aes(x = lon, y = lat, fill = gpp), show.legend = TRUE) +
  geom_sf(data = coast,
          colour = "black",
          linewidth = 0.3) +
  coord_sf(ylim = c(-60, 85), expand = FALSE) +
  scale_fill_viridis_c(name = expression(paste("gC m" ^ -2, "s" ^ -1)),
                       option = "cividis",
                       limits = c(0, 15)) +
  theme_void() +
  labs(subtitle = "January monthly mean")

# July
gg2 <- df |>
  filter(month == 7) |>
  ggplot() +
  geom_raster(aes(x = lon, y = lat, fill = gpp), show.legend = TRUE) +
  geom_sf(data = coast,
          colour = "black",
          linewidth = 0.3) +
  coord_sf(ylim = c(-60, 85), expand = FALSE) +
  scale_fill_viridis_c(name = expression(paste("gC m" ^ -2, "s" ^ -1)),
                       option = "cividis",
                       limits = c(0, 15)) +
  theme_void() +
  labs(subtitle = "July monthly mean")

cowplot::plot_grid(gg1, gg2, ncol = 1)
rm(df, gg1, gg2, coast)
gc()

# -----------------------------------------------------------
# Save to disk (tiny bookkeeping)
# -----------------------------------------------------------

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
year_tag <- if (!is.null(settings$year_start)) {
  as.character(settings$year_start)
} else {
  "unknown_year"
}

fig_dir <- "~/data/figures"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
fig_file <- file.path(fig_dir,
                      paste0(
                        "gpp_",
                        settings$fileprefix,
                        "_",
                        year_tag,
                        "_",
                        timestamp,
                        ".png"
                      ))

ggsave(fig_file, width = 6, height = 5)
message("Saved plot to: ", fig_file)
message("All done!")