# list demo file path
# path <- file.path(system.file(package = "grsofun"),"extdata")
WFDEI_elev_path <- file.path("~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc") # works only locally
WFDEI_clim_path <- "~/data/archive/wfdei_weedon_2014/data"

#---- test functions ----
testthat::test_that("test grsofun_tidy()", {

  tmpdir <- file.path(tempdir(), "test_grsofun/")

  settings <- list(
    fileprefix = "test",  # simulation name defined by the user
    model = "pmodel",     # in future could also be "biomee", but not yet implemented
    year_start = 2018,    # xxx not yet handled
    year_end = 2018,      # xxx not yet handled
    grid = list(          # a list specifying the grid which must be common to all forcing data
      lon_start = -179.75,
      dlon = 0.5,
      len_ilon = 720,
      lat_start = -89.75,
      dlat = 0.5,
      len_ilat = 360
    ),
    #------------------------------------------------ -
    dir_climate      = NA,#"~/data/archive/wfdei_weedon_2014/data", # path to where climate forcing data is located
    dir_climate_tidy = file.path(tmpdir, "tidy_climate"),                         # path to where tidy climate forcing data is to be written
    source_climate   = "watch-wfdei",  # a string specifying climate forcing dataset-specific variables

    file_fapar     = NA,#"~/data/scratch/bstocker/data/MODIS-C006_MOD15A2_LAI_FPAR_zmaw/MODIS-C006_MOD15A2__LAI_FPAR__LPDAAC__GLOBAL_0.5degree__UHAM-ICDC__2000_2018__MON__fv0.02.nc",  # path to where fAPAR forcing data is located
    dir_fapar_tidy = file.path(tmpdir, "tidy_fapar"),                             # path to where tidy fAPAR forcing data is to be written
    source_fapar   = "modis",   # a string specifying fAPAR forcing dataset-specific variables

    file_whc      = NA,#"~/data/archive/whc_stocker_2023/data/cwdx80_forcing.nc", # get from zenodo: https://doi.org/10.5281/zenodo.10885724 and add to archive
    dir_whc_tidy  = file.path(tmpdir, "tidy_whc"),                                # path to where tidy  root zone storage capacity forcing data is to be written

    file_landmask     = NA,#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc",      # path to where land mask data is located
    dir_landmask_tidy = file.path(tmpdir, "tidy_landmask"),                       # path to where tidy land mask data is to be written

    file_elv          = NA,#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc",      # path to where elevation data is located
    dir_elv_tidy      = file.path(tmpdir, "tidy_elv"),                            # path to where tidy elevation data is to be written
    #------------------------------------------------ -
    save_drivers = TRUE,  # whether rsofun driver object is to be saved. Uses additional disk space but substantially speeds up grsofun_run().
    # dir_drivers = here::here("input/out_tidy/"),             # path where rsofun drivers are to be written
    dir_drivers = "~/data/scratch/fbernhard/grsofun-input/",  # path where rsofun drivers are to be written
    overwrite = FALSE,    # whether files with tidy forcing data and drivers are to be overwritten. If false, reads files if available instead of re-creating them.
    spinupyears = 10,     # model spin-up length
    recycle = 1,          # climate forcing recycling during the spinup
    # dir_out = here::here("output/out_tidy/"),                # path for tidy model output
    dir_out = "~/data/scratch/fbernhard/grsofun-output/",     # path for tidy model output
    dir_out_nc = "xxx",  # xxx not yet handled
    save = list(         # a named list where names correspond to variable names in rsofun output and the value is a string specifying the temporal resolution to which global output is to be aggregated.
      gpp = "mon"
    ),
    nthreads = 1,   # distribute to multiple nodes for high performance computing - xxx not yet implemented -
    ncores_max = 12  # number of parallel jobs, set to 1 for un-parallel run
  )


  # Test NULL output
  out_tidy_01 <- grsofun_tidy(settings)
  testthat::expect_identical(
    out_tidy_01,
    list(res_landmask     = data.frame(input_path = NA, msg = "No landmask file found."),
         res_whc          = data.frame(input_path = NA, msg = "No whc file found."),
         res_elv          = data.frame(input_path = NA, msg = "No elv file found."),
         res_climate_list = data.frame(input_path = NA, msg = "No climate file found."),
         res_fapar        = data.frame(input_path = NA, msg = "No fapar file found.")))

  # Test subset of certain longitudes: (only test landmask and , keep others at NA)
  settings$file_landmask <- WFDEI_elev_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  settings$file_elv      <- WFDEI_elev_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  out_tidy_02 <- grsofun_tidy(settings,
                              filter_lon_between_degrees = c(-21, -17.5))

  testthat::expect_equal(nrow(bind_rows(out_tidy_02)), 10)

  testthat::expect_identical(
    list.files(settings$dir_landmask_tidy),
    c(
      "WFDEI-elevation_LON_-017.750.rds", "WFDEI-elevation_LON_-018.250.rds",
      "WFDEI-elevation_LON_-018.750.rds", "WFDEI-elevation_LON_-019.250.rds",
      "WFDEI-elevation_LON_-019.750.rds", "WFDEI-elevation_LON_-020.250.rds",
      "WFDEI-elevation_LON_-020.750.rds"))

  fresh_file <- readRDS(file.path(settings$dir_landmask_tidy,"WFDEI-elevation_LON_-017.750.rds"))
  testthat::expect_equal(
    fresh_file[c(1,5,25,53),],
    tibble(lon       = c(-17.75, -17.75, -17.75, -17.75),
           lat       = c(-89.75, -87.75, -77.75, 81.75),
           elevation = c(2865.37524414062, 2527.84228515625, 1588.33532714844, 44)),
    tolerance = 0.01)


  # Test also some climate files
  settings$dir_climate <- WFDEI_clim_path#"~/data/archive/wfdei_weedon_2014/data/WFDEI-elevation.nc"
  out_tidy_03 <- grsofun_tidy(settings,
                              filter_lon_between_degrees = c(-21, -17.5))


  testthat::expect_equal(nrow(bind_rows(out_tidy_03)), 58)
  testthat::expect_equal(nrow(out_tidy_03$res_climate_df), 42)
  testthat::expect_equal(nrow(out_tidy_03$res_elv), 7)
  testthat::expect_equal(nrow(out_tidy_03$res_landmask), 7)
  testthat::expect_equal(nrow(out_tidy_03$res_whc), 1)
  testthat::expect_equal(nrow(out_tidy_03$res_fapar), 1)
  testthat::expect_identical(
    list.files(settings$dir_climate_tidy),
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
