# list demo file path
# path <- file.path(system.file(package = "grsofun"),"extdata")
# WFDEI_elev_path <- "~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc" # works only locally
# WFDEI_clim_path <- "~/data/archive/wfdei_weedon_2014/data"                    # works only locally
WFDEI_elev_path <- "/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc" # works only locally
WFDEI_clim_path <- "/data/archive/wfdei_weedon_2014/data"                    # works only locally

#---- test functions ----
testthat::test_that("test grsofun_tidy()", {

  tmpdir <- file.path(tempdir(), "test_grsofun/")

  settings <- list(
    ### simulation config:
    fileprefix  = "test",   # simulation name defined by the user
    model       = "pmodel", # in future could also be "biomee", but not yet implemented
    year_start  = 2018,     # xxx not yet handled
    year_end    = 2018,     # xxx not yet handled
    spinupyears = 10,       # model spin-up length
    recycle     = 1,        # climate forcing recycling during the spinup

    ### HPC config:
    nnodes = 1,   # distribute to multiple nodes in a HPC environment: nnodes=1 no distribution, otherwise nnodes number of nodes to use in a HPC cluster setup to use (e.g. UBELIX) - xxx not yet implemented"
    ncores_max = 12, # number of multiple CPU-cores used per node for shared-memory parallel programming (e.g. with models like OpenMP)"

    ### final model output
    # dir_out = here::here("output/out_tidy/"),                # path for tidy model output
    dir_out     = "/data_2/scratch/fbernhard/grsofun-output/",     # path for tidy model output
    dir_out_nc  = "xxx",     # xxx not yet handled
    save = list(            # a named list where names correspond to variable names
      gpp = "mon"           #   in rsofun output and the value is a string specifying
    ),                      #   the temporal resolution to which global output is to be aggregated.

    ### intermediate model output
    dir_out_drivers = NA, #"/data_2/scratch/fbernhard/grsofun-input-drivers/",  # If a path is provided rsofun driver object is to be saved. Uses additional disk space but substantially speeds up grsofun_run(). If dir_out_drivers is 'NA' driver object are not stored.
    overwrite_intermediate = FALSE,      # whether files with tidy forcing data and drivers are to be overwritten. If false, reads files if available instead of re-creating them.

    ### tidy model input config:
    grid = list( # a list specifying the grid which must be common to all forcing data
      lon_start = -179.75,
      dlon      = 0.5,
      len_ilon  = 720,
      lat_start = -89.75,
      dlat      = 0.5,
      len_ilat  = 360
    ), # TODO: xxx unused: in future ensure grid$... is enforced/asserted

    source_climate       = "watch-wfdei",  # a string specifying climate forcing dataset-specific variables
    dir_in_climate       = NA,
    dir_out_tidy_climate = file.path(tmpdir, "tidy_climate"),

    source_fapar       = "modis",   # a string specifying fAPAR forcing dataset-specific variables
    file_in_fapar      = NA,
    dir_out_tidy_fapar = file.path(tmpdir, "tidy_fapar"),

    file_in_whc      = NA,
    dir_out_tidy_whc = file.path(tmpdir, "tidy_whc"),

    file_in_landmask      = NA,
    dir_out_tidy_landmask = file.path(tmpdir, "tidy_landmask"),

    file_in_elv      = NA,
    dir_out_tidy_elv = file.path(tmpdir, "tidy_elv")
  )


  # Test NULL output
  out_tidy_01 <- grsofun::grsofun_tidy(settings)
  testthat::expect_identical(
    out_tidy_01,
    list(res_landmask     = data.frame(input_path = NA, msg = "No landmask file found."),
         res_whc          = data.frame(input_path = NA, msg = "No whc file found."),
         res_elv          = data.frame(input_path = NA, msg = "No elv file found."),
         res_climate_df   = data.frame(input_path = NA, msg = "No climate file found."),
         res_fapar        = data.frame(input_path = NA, msg = "No fapar file found.")))
  testthat::expect_equal(nrow(dplyr::bind_rows(out_tidy_01)), 5)

  # Test subset of certain longitudes: (only test landmask and , keep others at NA)
  settings$file_in_landmask <- WFDEI_elev_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  settings$file_in_elv      <- WFDEI_elev_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  out_tidy_02 <- grsofun::grsofun_tidy(settings,
                              filter_lon_between_degrees = c(-21, -17.5))

  testthat::expect_equal(nrow(dplyr::bind_rows(out_tidy_02)), 17)

  testthat::expect_identical(
    list.files(settings$dir_out_tidy_landmask),
    c(
      "WFDEI-elevation_LON_-017.750.rds", "WFDEI-elevation_LON_-018.250.rds",
      "WFDEI-elevation_LON_-018.750.rds", "WFDEI-elevation_LON_-019.250.rds",
      "WFDEI-elevation_LON_-019.750.rds", "WFDEI-elevation_LON_-020.250.rds",
      "WFDEI-elevation_LON_-020.750.rds"))

  fresh_file <- readRDS(file.path(settings$dir_out_tidy_landmask,"WFDEI-elevation_LON_-017.750.rds"))
  testthat::expect_equal(
    fresh_file[c(1,5,25,53),],
    dplyr::tibble(lon       = c(-17.75, -17.75, -17.75, -17.75),
                  lat       = c(-89.75, -87.75, -77.75, 81.75),
                  elevation = c(2865.37524414062, 2527.84228515625, 1588.33532714844, 44)),
    tolerance = 0.01)


  # Test also some climate files
  settings$dir_in_climate <- WFDEI_clim_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  out_tidy_03 <- grsofun::grsofun_tidy(settings,
                              filter_lon_between_degrees = c(-21, -17.5))


  testthat::expect_equal(nrow(bind_rows(out_tidy_03)), 58)
  testthat::expect_equal(nrow(out_tidy_03$res_climate_df), 42)
  testthat::expect_equal(nrow(out_tidy_03$res_elv), 7)
  testthat::expect_equal(nrow(out_tidy_03$res_landmask), 7)
  testthat::expect_equal(nrow(out_tidy_03$res_whc), 1)
  testthat::expect_equal(nrow(out_tidy_03$res_fapar), 1)
  testthat::expect_identical(
    list.files(settings$dir_out_tidy_climate),
    c("PSurf_daily_WFDEI_LON_-017.750.rds", "PSurf_daily_WFDEI_LON_-018.250.rds", "PSurf_daily_WFDEI_LON_-018.750.rds", "PSurf_daily_WFDEI_LON_-019.250.rds",
      "PSurf_daily_WFDEI_LON_-019.750.rds", "PSurf_daily_WFDEI_LON_-020.250.rds", "PSurf_daily_WFDEI_LON_-020.750.rds",
      "Qair_daily_WFDEI_LON_-017.750.rds", "Qair_daily_WFDEI_LON_-018.250.rds", "Qair_daily_WFDEI_LON_-018.750.rds", "Qair_daily_WFDEI_LON_-019.250.rds",
      "Qair_daily_WFDEI_LON_-019.750.rds", "Qair_daily_WFDEI_LON_-020.250.rds", "Qair_daily_WFDEI_LON_-020.750.rds",
      "Rainf_daily_WFDEI_LON_-017.750.rds", "Rainf_daily_WFDEI_LON_-018.250.rds", "Rainf_daily_WFDEI_LON_-018.750.rds", "Rainf_daily_WFDEI_LON_-019.250.rds",
      "Rainf_daily_WFDEI_LON_-019.750.rds", "Rainf_daily_WFDEI_LON_-020.250.rds", "Rainf_daily_WFDEI_LON_-020.750.rds",
      "Snowf_daily_WFDEI_LON_-017.750.rds", "Snowf_daily_WFDEI_LON_-018.250.rds", "Snowf_daily_WFDEI_LON_-018.750.rds", "Snowf_daily_WFDEI_LON_-019.250.rds",
      "Snowf_daily_WFDEI_LON_-019.750.rds", "Snowf_daily_WFDEI_LON_-020.250.rds", "Snowf_daily_WFDEI_LON_-020.750.rds",
      "SWdown_daily_WFDEI_LON_-017.750.rds", "SWdown_daily_WFDEI_LON_-018.250.rds", "SWdown_daily_WFDEI_LON_-018.750.rds", "SWdown_daily_WFDEI_LON_-019.250.rds",
      "SWdown_daily_WFDEI_LON_-019.750.rds", "SWdown_daily_WFDEI_LON_-020.250.rds", "SWdown_daily_WFDEI_LON_-020.750.rds",
      "Tair_daily_WFDEI_LON_-017.750.rds", "Tair_daily_WFDEI_LON_-018.250.rds", "Tair_daily_WFDEI_LON_-018.750.rds", "Tair_daily_WFDEI_LON_-019.250.rds",
      "Tair_daily_WFDEI_LON_-019.750.rds", "Tair_daily_WFDEI_LON_-020.250.rds", "Tair_daily_WFDEI_LON_-020.750.rds"
    ))

})
