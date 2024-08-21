#' @export
grsofun_tidy <- function(settings){

  # land mask and elevation in one
  error <- map2tidy::map2tidy(
    nclist = settings$file_landmask,
    varnam = "elevation",
    lonnam = "lon",
    latnam = "lat",
    do_chunks = TRUE,
    outdir = settings$dir_landmask_tidy,
    fileprefix = "WFDEI-elevation",
    overwrite = settings$overwrite
  )

  # root zone total whc
  error <- map2tidy::map2tidy(
    nclist = settings$file_whc,
    varnam = "cwdx80_forcing",
    lonnam = "lon",
    latnam = "lat",
    do_chunks = TRUE,
    outdir = settings$dir_whc_tidy,
    fileprefix = "cwdx80_forcing_halfdeg",
    overwrite = settings$overwrite
  )

  # # elevation
  # error <- map2tidy::map2tidy(
  #   nclist = settings$file_elv,
  #   varnam = "ETOPO1_Bed_g_geotiff",
  #   lonnam = "lon",
  #   latnam = "lat",
  #   do_chunks = TRUE,
  #   outdir = settings$dir_elv_tidy,
  #   fileprefix = "ETOPO1_Bed_g_geotiff_halfdeg"
  # )


  # climate
  if (settings$source_climate == "watch-wfdei"){

    # data-product specific variable names
    vars <- c("Tair", "Rainf", "Snowf", "Qair", "SWdown", "PSurf")

    # make files tidy for each variable
    error <- purrr::map(
      vars,
      ~grsofun_tidy_byvar(., settings)
    )

  }

  # fapar
  if (settings$source_fapar == "modis"){
    error <- map2tidy::map2tidy(
      nclist = settings$file_fapar,
      varnam = "fpar",
      lonnam = "lon",
      latnam = "lat",
      timenam = "time",
      timedimnam = "time",
      do_chunks = TRUE,
      outdir = settings$dir_fapar_tidy,
      fileprefix = "MODIS-C006_MOD15A2_LAI_FPAR_zmaw",
      single_basedate = TRUE,
      overwrite = TRUE
      # ncores = 2  # parallel::detectCores()
    )
  } else {
    stop("
    FAPAR inputs need case-by-case modification of the code in grsofun.
    Your input to 'settings$source_fapar' does not (yet) appear to be supported.")
  }

  return(settings)
}

#' @export
grsofun_tidy_byvar <- function(var, settings){

  # consider data product-specific directory structure and netcdf variable and
  # dimension names
  if (settings$source_climate == "watch-wfdei"){
    list_filnams <- list.files(
      paste0(settings$dir_climate, "/", var, "_daily/"),
      pattern = ".nc",
      full.names = TRUE
    )

    outfile_prefix_value <- paste0(var, "_daily_WFDEI")

    # define grid of climate files # TODO: this is actually not needed specifically for climate files, but all files need to be checked and asserted
    settings$grid_climate <- list(
      lonnam = "lon",
      latnam = "lat",
      timenam = "tstep" # or timestp??
    )

    # since watch-wfdei files do not appear to have a CF-compliant time coordinate description we need to define a workaround
    fgetdate_function <- function(fn){
      first_day <-
        gsub(".*_WFDEI_((CRU)*_*)([0-9]*).nc","\\3", x=basename(fn)) |>
        lubridate::ym() #|>
      # lubridate::days_in_month()

      return(seq(from = first_day,
                 to   = first_day + months(1) - 1, # go to the end of the month
                 by   = "day") |>
               as.character())
    }
  } else {
    stop("
    Climate data inputs need case-by-case modification of the code in grsofun.
    Your input to 'settings$source_climate' does not (yet) appear to be supported.")
  }

  # load and convert
  result_climate_list <- map2tidy::map2tidy(
    nclist  = list_filnams,
    varnam  = var,
    lonnam  = settings$grid_climate$lonnam,
    latnam  = settings$grid_climate$latnam,
    timenam = settings$grid_climate$timenam,
    do_chunks  = TRUE,
    outdir     = settings$dir_climate_tidy,
    fileprefix = outfile_prefix_value,
    overwrite  = settings$overwrite,
    fgetdate   = fgetdate_function,
    ncores     = settings$ncores_max  # parallel::detectCores()
  )

  return(result_climate_list)
}
