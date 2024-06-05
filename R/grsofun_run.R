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

  # xxx test: for DE-Tha (DE-Tha  lon = 13.6, lat = 51.0, elv = 380 m), use ilon = 388 (lon = 13.75, lat = 50.75, sitename = grid_ilon_388_ilat_174)

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
        fapar = 1,
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
        fapar,
        patm = PSurf,
        tmin = Tair,
        tmax = Tair
      ) |>

      dplyr::group_by(lon, lat) |>
      tidyr::nest()

  }

  if (settings$source_fapar == "modis"){
    df <- read_rds(paste0(settings$dir_fapar_tidy, "MODIS-C006_MOD15A2_LAI_FPAR_zmaw_ilon_", ilon, ".rds")) |>
      tidyr::unnest(data)

    # match years read from climate
    dates <- df_forcing$data[[1]]$date
    year_end <- max(lubridate::year(dates[lubridate::month(dates) == 12 & lubridate::mday(dates) == 31]))
    year_start <- min(lubridate::year(dates[lubridate::month(dates) == 1 & lubridate::mday(dates) == 1]))

  }

  # populate driver object
  if (settings$model == "pmodel"){

    df <- df |>

      # merge with forcing time series
      left_join(
        df_climate |>
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

  # run model
  out <- rsofun::runread_pmodel_f(
    drivers = df |>
      filter(sitename == "grid_ilon_388_ilat_174"),
    par = par
  )

  # xxx test (near DE-Tha)
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
