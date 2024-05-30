grsofun_make_tidy <- function(settings){

  if (settings$source_climate == "watch-wfdei"){

    vars <- c("Tair", "Rainf", "Snowf", "Qair", "SWdown", "PSurf")

    error <- purrr::map(
      vars,
      ~grsofun_make_tidy_byvar(., settings)
    )

  }
  return(error)
}

grsofun_make_tidy_byvar <- function(var, settings){

  # consider data product-specific directory structure and netcdf variable and
  # dimension names
  if (settings$source_climate == "watch-wfdei"){
    filnam <- list.files(
      paste0(settings$dir_climate, "/", var, "_daily/"),
      pattern = ".nc",
      full.names = TRUE
    )
    lonnam <- "lon"
    latnam <- "lat"
    timenam = "timestp"
    timedimnam = "tstep"
  }

  # load and convert
  error <- map2tidy::map2tidy(
    nclist = filnam,
    varnam = var,
    lonnam = lonnam,
    latnam = latnam,
    timenam = timenam,
    timedimnam = timedimnam,
    do_chunks = TRUE,
    outdir = settings$dir_climate_tidy,
    fileprefix = paste0(var, "_daily_WFDEI"),
    single_basedate = FALSE
    # ncores = 2  # parallel::detectCores()
  )

  return(error)
}
