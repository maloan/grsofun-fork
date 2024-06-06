grsofun_run <- function(par, settings){

  # xxx remove 'par' from arguments

  # distribute to separate notes (distributed to cores within node is done
  # inside functions)
  purrr::map(
    seq(settings$nthreads),
    ~grsofun_run_bychunk(., settings$nthreads, par, settings)
  )


}

grsofun_run_bychunk <- function(chunk, nthreads, par, settings){

  # XXX: make this a system call for running a script containing this code

  # determine longitude indices to process
  vec_index <- map2tidy::get_index_by_chunk(
    chunk,                           # counter for chunks
    nthreads,                        # total number of chunks
    settings$grid_climate$len_ilon   # total number of longitude indices
  )

  # number of cores to use for this thread
  ncores <- max(parallel::detectCores() - 2, 1)

  # parallelize job
  # set up the cluster, sending required objects to each core
  cl <- multidplyr::new_cluster(ncores) |>
    multidplyr::cluster_library(c("map2tidy",
                                  "dplyr",
                                  "purrr",
                                  "tidyr",
                                  "magrittr",
                                  "grsofun"
                                  )) |>
    multidplyr::cluster_assign(
      grsofun_run_byilon = grsofun_run_byilon,   # make the function known for each core
      read_forcing_byvar_byilon = read_forcing_byvar_byilon,
      calc_vp = calc_vp,
      calc_vpd = calc_vpd,
      calc_vp_inst = calc_vp_inst,
      calc_vpd_inst = calc_vpd_inst
    )

  # distribute computation across the cores, calculating for all longitudinal
  # indices of this chunk
  out <- tibble(ilon = vec_index) |>
    multidplyr::partition(cl) |>
    dplyr::mutate(out = purrr::map(
      ilon,
      ~grsofun_run_byilon(
        .,
        par,
        settings
      ))
    )

}

grsofun_run_byilon <- function(ilon, par, settings){

  # That's where it all happens...
  message(paste("Running rsofun for ilon =", ilon))

  # xxx test: ilon = 388
  # for DE-Tha (DE-Tha  lon = 13.6, lat = 51.0, elv = 380 m), use ilon = 388 (lon = 13.75, lat = 50.75, sitename = grid_ilon_388_ilat_174)

  # get land mask - variable name hard coded
  df <- readr::read_rds(paste0(settings$dir_landmask_tidy, "WFDEI-elevation_ilon_", ilon, ".rds")) |>
    dplyr::rename(elv = elevation) |>

    # # get elevation
    # dplyr::left_join(
    #   readr::read_rds(paste0(settings$dir_elv_tidy, "ETOPO1_Bed_g_geotiff_halfdeg_ilon_", ilon, ".rds")) |>
    #     dplyr::rename(elv = ETOPO1_Bed_g_geotiff),
    #   dplyr::join_by(lon, lat)
    # ) |>

    # get rooting zone total whc
    dplyr::left_join(
      readr::read_rds(paste0(settings$dir_whc_tidy, "cwdx80_forcing_halfdeg_ilon_", ilon, ".rds")) |>
        dplyr::rename(whc = cwdx80_forcing),
      dplyr::join_by(lon, lat)
    )

  # Read tidy climate forcing data by longitudinal band and convert units -
  # product-specific
  if (settings$source_climate == "watch-wfdei"){

    vars <- c("Tair", "Rainf", "Snowf", "Qair", "SWdown", "PSurf")

    # read tidy data for all variables and join into single data frame
    kfFEC <- 2.04
    df_climate <- purrr::map(
      vars,
      ~read_forcing_byvar_byilon(., ilon, settings)
      ) |>
      purrr::reduce(
        left_join,
        join_by(lon, lat, time)
      ) |>

      # convert units and rename
      dplyr::rowwise() |>
      dplyr::mutate(
        Tair = Tair - 273.15,  # K -> deg C
        ppfd = SWdown * kfFEC * 1.0e-6,  # W m-2 -> mol m-2 s-1
        vapr = calc_vp(
          qair = Qair,
          patm = PSurf
        ),
        vpd = calc_vpd(eact = vapr, tc = Tair)
      ) |>

      # XXX try
      mutate(
        netrad = NA,
        ccov = 0.5,
        co2 = 400,
      ) |>
      dplyr::select(
        lon,
        lat,
        date = time,
        temp = Tair,
        rain = Rainf,
        vpd,
        ppfd,
        netrad,
        ccov,
        snow = Snowf,
        co2,
        patm = PSurf,
        tmin = Tair,
        tmax = Tair
      ) |>

      dplyr::group_by(lon, lat) |>
      tidyr::nest()

    # # xxx test
    # df_climate |>
    #   filter(lat == 50.75) |>
    #   unnest(data) |>
    #   ggplot(aes(date, vpd)) +
    #   geom_line()
    #
    # df_climate |>
    #   filter(lat == 50.75) |>
    #   unnest(data) |>
    #   visdat::vis_miss()

  }

  if (settings$source_fapar == "modis"){

    # read monthly fAPAR data
    df_fapar_mon <- read_rds(paste0(settings$dir_fapar_tidy, "MODIS-C006_MOD15A2_LAI_FPAR_zmaw_ilon_", ilon, ".rds")) |>
      mutate(data = purrr::map(data, ~rename(., date = time))) |>

      # xxx try
      filter(lat == 50.75)

    # df_fapar_mon$data[[1]] |>
    #   ggplot(aes(date, fpar)) +
    #   geom_line()

    # linearly interpolate monthly fAPAR values to daily
    dates <- df_fapar_mon$data[[1]]$date
    year_start <- min(lubridate::year(dates[lubridate::month(dates) == 1]))
    year_end <- max(lubridate::year(dates[lubridate::month(dates) == 12]))

    ddf <- tibble(
      date = seq(
        from = lubridate::ymd(paste0(year_start, "-01-01")),
        to = lubridate::ymd(paste0(year_end, "-12-31")),
        by = "days"
      ))

    interpolate2daily_fpar <- function(df, ddf){
      # linearly interpolate (leaves training NAs)
      ddf <- ddf |>
        dplyr::left_join(
          df,
          by = "date"
        ) |>
        dplyr::mutate(
          fpar_daily = zoo::na.approx(fpar, na.rm = FALSE)
        )

      # fill remaining with mean seasonal cycle
      meandf <- ddf |>
        dplyr::mutate(doy = lubridate::yday(date)) |>
        dplyr::group_by(doy) |>
        dplyr::summarise(fpar_meandoy = mean(fpar_daily, na.rm = TRUE))

      ddf <- ddf |>
        dplyr::mutate(doy = lubridate::yday(date)) |>
        dplyr::left_join(
          meandf,
          by = "doy"
        ) |>
        dplyr::mutate(fpar_daily = ifelse(is.na(fpar_daily), fpar_meandoy, fpar_daily)) |>
        dplyr::select(-fpar_meandoy, -doy)

      return(ddf)
    }

    df_fapar <- df_fapar_mon |>
      mutate(data = purrr::map(data, ~interpolate2daily_fpar(., ddf))) |>
      mutate(data = purrr::map(data, ~dplyr::select(., -fpar))) |>
      mutate(data = purrr::map(data, ~dplyr::rename(., fapar = fpar_daily)))

    # df_fapar$data[[1]] |>
    #   ggplot(aes(date, fpar)) +
    #   geom_line()

    # # reduce to match years available from climate forcing
    # dates <- df_climate$data[[1]]$date
    # year_start <- min(lubridate::year(dates[lubridate::month(dates) == 1 & lubridate::mday(dates) == 1]))
    # year_end <- max(lubridate::year(dates[lubridate::month(dates) == 12 & lubridate::mday(dates) == 31]))


  }

  # combine to the most limiting time span among the climate and fapar time series
  df_forcing <- df_climate |>
    tidyr::unnest(data) |>
    dplyr::inner_join(
      df_fapar |>
        tidyr::unnest(data),
      by = c("lon", "lat", "date")
    ) |>
    dplyr::group_by(lon, lat) |>
    tidyr::nest()

  # # xxx test
  # df_forcing |>
  #   filter(lat == 50.75) |>
  #   unnest(data) |>
  #   ggplot(aes(date, vpd)) +
  #   geom_line()
  #
  # df_forcing |>
  #   filter(lat == 50.75) |>
  #   unnest(data) |>
  #   visdat::vis_miss()

  # populate driver object
  if (settings$model == "pmodel"){

    df <- df |>

      # merge with forcing time series
      left_join(
        df_forcing |>
          dplyr::rename(forcing = data),
        join_by(lon, lat)
      ) |>

      mutate(ilat = seq(n())) |>

      # create simulation parameters (common for all)
      # construct site name from longitude and latitude indices
      dplyr::mutate(
        sitename = paste0("grid_ilon_", ilon, "_ilat_", ilat),
        spinup = TRUE,
        spinupyears = settings$spinupyears,
        recycle = settings$recycle,
        outdt = 1,
        ltre = FALSE,
        ltne = FALSE,
        ltrd = FALSE,
        ltnd = FALSE,
        lgr3 = TRUE,
        lgn3 = FALSE,
        lgr4 = FALSE,
      ) |>

      # group simulation parameters
      tidyr::nest(
        params_siml = c(
          spinup,
          spinupyears,
          recycle,
          outdt,
          ltre,
          ltne,
          ltrd,
          ltnd,
          lgr3,
          lgn3,
          lgr4
        )
      ) |>

      # group site meta info
      tidyr::nest(
        site_info = c(
          lon,
          lat,
          elv,
          whc
        )
      ) |>

      # put columns in order
      dplyr::select(
        sitename,
        params_siml,
        site_info,
        forcing
      )
  }

  # xxx test (near DE-Tha)
  df |>
    filter(sitename == "grid_ilon_388_ilat_174") |>
    unnest(forcing) |>
    ggplot(aes(date, vpd)) +
    geom_line()

  df |>
    filter(sitename == "grid_ilon_388_ilat_174") |>
    unnest(forcing) |>
    visdat::vis_miss()

  # run model
  out <- rsofun::runread_pmodel_f(
    drivers = df |>
      filter(sitename == "grid_ilon_388_ilat_174"),
    par = par
  )

  # xxx test
  out$data[[1]] |>
    ggplot(aes(date, fapar)) +
    geom_line()

  out$data[[1]] |>
    ggplot(aes(date, gpp)) +
    geom_line()

}

read_forcing_byvar_byilon <- function(var, ilon, settings){

  if (settings$source_climate == "watch-wfdei"){
    df <- read_rds(paste0(settings$dir_climate, "/tidy/", var, "_daily_WFDEI_ilon_", ilon, ".rds")) |>
    tidyr::unnest(data)
  }

  return(df)
}
