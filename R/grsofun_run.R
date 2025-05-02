#' @param par ...
#' @param settings ...
#'
#' @export
grsofun_run <- function(par, settings){

  # Create necessary output directories
  if (!is.na(settings$dir_out)){
    dir.create(settings$dir_out, recursive = TRUE, showWarnings = FALSE)
  }
  if (!is.na(settings$dir_out_drivers)){
    dir.create(settings$dir_out_drivers, recursive = TRUE, showWarnings = FALSE)
  }

  # Create vector of strings for identifying tidy files by longitudinal band
  df_lon_index <- map2tidy::get_df_lon_index(settings$grid)
  list_of_LON_str <- map2tidy::get_file_suffix(
    ilon = df_lon_index$lon_index,
    df_lon_index = df_lon_index
    )

  if (settings$nnodes == 1){
    if (settings$ncores_max == 1){
      # Do not parallelize
      # out <- dplyr::tibble(ilon = 292) |>
      out <- dplyr::tibble(LON_str = list_of_LON_str) |>
          dplyr::mutate(out = purrr::map(
            LON_str,
            ~grsofun_run_byLON(
              .,
              par,
              settings
            ))
          )

    } else {
      # Parallelise by longitudinal bands on multiple cores of a single node
      # number of cores to use for this thread
      # keep one core free
      ncores <- ifelse(
        is.na(settings$ncores_max),
        max(parallel::detectCores() - 1, 1),
        min(settings$ncores_max, max(parallel::detectCores() - 1, 1))
      )

      # parallelize job
      # set up the cluster, sending required objects to each core
      cl <- multidplyr::new_cluster(ncores) |>
        multidplyr::cluster_library(c("map2tidy",
                                      "dplyr",
                                      "purrr",
                                      "tidyr",
                                      "readr",
                                      "grsofun"
        )) |>
        multidplyr::cluster_assign(
          grsofun_run_byLON        = grsofun_run_byLON,   # make the function known for each core
          read_forcing_byvar_byLON = read_forcing_byvar_byLON,
          par                      = par,
          settings                 = settings
        )

      # distribute computation across the cores, calculating for all longitudinal
      # indices of this chunk
      out <- dplyr::tibble(LON_str = list_of_LON_str) |>
        multidplyr::partition(cl) |>
        dplyr::mutate(out = purrr::map(
          LON_str,
          ~grsofun_run_byLON(
            .,
            par,
            settings
          ))
        )

    }

  } else {
    # Create chunks of longitude indices and send to separate threads
    # distribute to separate nodes (distributed to cores within node is done
    # inside functions)
    stop("Option nnodes > 1 is currently not implemented.")
    purrr::map(
      seq(settings$nnodes),
      ~grsofun_run_bychunk(., settings$nnodes, par, settings)
    )
  }
}

##' @export
# grsofun_run_bychunk <- function(chunk, nnodes, par, settings){
#
#   stop("Option nnodes > 1 is currently not implemented.")
#
#   # XXX: make this a system call for running a script containing this code
#
#   # determine longitude indices to process
#   list_of_LON_str <- map2tidy::get_index_by_chunk(
#     chunk,                           # counter for chunks
#     nnodes,                        # total number of chunks
#     len_ilon                         # total number of longitude indices
#   )
#
#   # number of cores to use for this thread
#   # keep one core free
#   ncores <- ifelse(
#     is.na(settings$ncores_max),
#     max(parallel::detectCores() - 1, 1),
#     min(settings$ncores_max, max(parallel::detectCores() - 1, 1))
#   )
#
#   # parallelize job
#   # set up the cluster, sending required objects to each core
#   cl <- multidplyr::new_cluster(ncores) |>
#     multidplyr::cluster_library(c("map2tidy",
#                                   "dplyr",
#                                   "purrr",
#                                   "tidyr",
#                                   "magrittr",
#                                   "readr",
#                                   "grsofun",
#                                   )) |>
#     multidplyr::cluster_assign(
#       grsofun_run_byLON = grsofun_run_byLON,   # make the function known for each core
#       read_forcing_byvar_byLON = read_forcing_byvar_byLON
#     )
#
#   # distribute computation across the cores, calculating for all longitudinal
#   # indices of this chunk
#   out <- dplyr::tibble(ilon = list_of_LON_str) |>
#     multidplyr::partition(cl) |>
#     dplyr::mutate(out = purrr::map(
#       ilon,
#       ~grsofun_run_byLON(
#         .,
#         par,
#         settings
#       ))
#     )
#
# }

#' @export
grsofun_run_byLON <- function(LON_string, par, settings){
  # e.g LON_string = "LON_+046.750"

  # for DE-Tha (DE-Tha  lon = 13.6, lat = 51.0, elv = 380 m),
  #            use (lon = 13.75, lat = 50.75, sitename = grid_LON_+013.750_LAT_+050.750)

  if (!is.na(settings$dir_out_drivers)){
    dir.create(settings$dir_out_drivers, recursive = TRUE, showWarnings = FALSE) # TODO: make this emit a message
    filnam_drivers <- file.path(settings$dir_out_drivers, paste0(settings$fileprefix, "_", LON_string, ".rds"))
  }

  if (settings$overwrite_intermediate || !file.exists(filnam_drivers)){

    # get land mask - variable name hard coded
    filnam <- paste0(settings$dir_out_tidy_landmask, "/WFDEI-elevation", LON_string, ".rds")

    if (!file.exists(filnam)){
      stop(paste("File does not exist:", filnam))
    }

    df <- readr::read_rds(filnam) |>
      dplyr::rename(elv = elevation)

    # # get elevation
    # dplyr::left_join(
    #   readr::read_rds(paste0(settings$dir_out_tidy_elv, "ETOPO1_Bed_g_geotiff_halfdeg_ilon_", ilon, ".rds")) |>
    #     dplyr::rename(elv = ETOPO1_Bed_g_geotiff),
    #   dplyr::join_by(lon, lat)
    # ) |>

    # get rooting zone water storage capacity information. If missing, assume 200 mm.
    filnam <- paste0(
      settings$dir_out_tidy_whc,
      "/",
      gsub(".nc", "" , basename(settings$file_in_whc)),
      LON_string,
      ".rds"
      )
    if (!file.exists(filnam)){
      stop(paste("File does not exist:", filnam))
    }
    df_whc <- readr::read_rds(filnam)
    if (nrow(df_whc) > 0){
      df <- df |>
        dplyr::left_join(
          df_whc |>
            dplyr::rename(whc = cwdx80_forcing),
          dplyr::join_by(lon, lat)
        )
    } else {
      df <- df |>
        dplyr::mutate(whc = 200)
    }

    df <- df |>
      dplyr::mutate(whc = ifelse(is.na(whc), 200, whc))

    # Read tidy climate forcing data by longitudinal band and convert units -
    # product-specific
    if (settings$source_climate == "watch-wfdei"){

      vars <- c("Tair", "Rainf", "Snowf", "Qair", "SWdown", "PSurf")

      # read tidy data for all variables and join into single data frame
      kfFEC <- 2.04
      df_climate <- purrr::map(
        vars,
        ~read_forcing_byvar_byLON(., LON_string, settings)
      ) |>
        purrr::reduce(
          dplyr::left_join,
          dplyr::join_by(lon, lat, datetime)
        ) |>

        # convert units and rename
        dplyr::rowwise() |>
        dplyr::mutate(
          Tair = Tair - 273.15,  # K -> deg C
          ppfd = SWdown * kfFEC * 1.0e-6,  # W m-2 -> mol m-2 s-1
          vapr = rgeco::calc_vp(
            qair = Qair,
            patm = PSurf
          ),
          vpd = rgeco::calc_vpd(
            eact = vapr,
            tc = Tair,
            patm = PSurf
          )
        ) |>

        # XXX try
        dplyr::mutate(
          netrad = NA,
          ccov = 0.5,
          co2 = 400,
        ) |>
        dplyr::select(
          lon,
          lat,
          date = datetime, # map2tidy outputs 'datetime', but pmodel requires 'date'
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

      df_climate <- df_climate |>
        # parse datetimes from string (output of map2tidy) to datetimes
        dplyr::mutate(data = purrr::map(data, ~dplyr::mutate(., date = lubridate::ymd(date))))

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
      filnam <- paste0(settings$dir_out_tidy_fapar, "/MODIS-C006_MOD15A2_LAI_FPAR_zmaw", LON_string, ".rds")
      if (!file.exists(filnam)){
        stop(paste("File does not exist: ", filnam))
      }

      df_fapar_mon <- readr::read_rds(filnam)

      # check if something was read
      if ("data.frame" %in% class(df_fapar_mon)){
        avl_fapar <- TRUE
      } else {
        avl_fapar <- FALSE
      }

      if (avl_fapar){
        df_fapar_mon <- df_fapar_mon |>

          # map2tidy outputs 'datetime', but pmodel requires 'date'
          dplyr::mutate(data = purrr::map(data, ~dplyr::rename(., date = datetime))) |>

          # parse datetimes from string (output of map2tidy) to datetimes
          dplyr::mutate(data = purrr::map(data, ~dplyr::mutate(., date = lubridate::ymd(date))))

        # # xxx problem: no decembers are read after 2014
        # df_fapar_mon |> slice(1) |>
        #   unnest(data) |>
        #   select(date) |>
        #   distinct() |>
        #   arrange(date) |>
        #   View()

        # # xxx try
        # filter(lat == 50.75)

        # df_fapar_mon$data[[1]] |>
        #   ggplot(aes(date, fpar)) +
        #   geom_line()

        # linearly interpolate monthly fAPAR values to daily
        dates <- df_fapar_mon$data[[1]]$date
        year_start <- min(lubridate::year(dates))
        year_end <- max(lubridate::year(dates))

        # create a data frame that spans all dates between start and end of simulation
        # consider only complete years
        ddf <- dplyr::tibble(
          date = seq(
            from = lubridate::ymd(paste0(year_start, "-01-01")),
            to = lubridate::ymd(paste0(year_end, "-12-31")),
            by = "days"
          ))

        # function to linearly interpolate (leaves trailing NAs)
        interpolate2daily_fpar <- function(df, ddf){
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
          dplyr::mutate(data = purrr::map(data, ~interpolate2daily_fpar(., ddf))) |>
          dplyr::mutate(data = purrr::map(data, ~dplyr::select(., -fpar))) |>
          dplyr::mutate(data = purrr::map(data, ~dplyr::rename(., fapar = fpar_daily)))

        # df_fapar$data[[1]] |>
        #   ggplot(aes(date, fpar)) +
        #   geom_line()

        # combine to capture all gridcells of the climate forcing
        # if fapar forcing is missing, set fapar = 0
        df_forcing <- df_climate |>
          tidyr::unnest(data) |>
          dplyr::left_join(
            df_fapar |>
              tidyr::unnest(data),
            by = c("lon", "lat", "date")
          ) |>
          dplyr::mutate(fapar = ifelse(is.na(fapar), 0, fapar)) |>
          dplyr::group_by(lon, lat) |>
          tidyr::nest()

      } else {
        # fapar not available - set to zero
        df_forcing <- df_climate |>
          dplyr::mutate(data = purrr::map(data, ~dplyr::mutate(., fapar = 0)))
      }
    }

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
        dplyr::left_join(
          df_forcing |>
            dplyr::rename(forcing = data),
          dplyr::join_by(lon, lat)
        ) |>

        # construct site name from longitude and latitude indices
        dplyr::mutate(
          # LAT_string = sprintf("LAT_%+08.3f", lat),
          # sitename = paste0("grid_", LON_string, "_", LAT_string)
          sitename = paste0("grid_", LON_string, "_", sprintf("LAT_%+08.3f", lat))
        ) |>

        # create simulation parameters (common for all)
        dplyr::mutate(
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

    if (!is.na(settings$dir_out_drivers)){
      message(paste("Writing file", filnam_drivers, "..."))
      readr::write_rds(df, file = filnam_drivers)
    }

  } else {

    df <- readr::read_rds(filnam_drivers)

  }

  # # some gridcells have no forcing because no fapar data is available
  # # fill them with NA forcing as forcing which triggers an empty rsofun run
  # df <- df |>
  #   mutate(isnull = purrr::map_lgl(forcing, ~is.null(.)))
  # df_forcing_empty <- df |>
  #   filter(!isnull) |>
  #   slice(1) |>
  #   pull(forcing)
  # df_forcing_empty <- df_forcing_empty[[1]] |>
  #   slice(1) |>
  #   mutate(
  #     dplyr::across(
  #       c(temp, rain, vpd, ppfd, netrad, ccov, snow, co2, patm, tmin, tmax, fapar),
  #       ~ NA
  #       ))
  # df <- df |>
  #   mutate(forcing = purrr::map(forcing, ~ifelse(isnull, df_forcing_empty, forcing)))

  # # xxx test forcing (near DE-Tha)
  # df_fdk <- readRDS("~/data/FluxDataKit/v3.1/rsofun_driver_data_v3.1.rds")
  #
  # tmp <- df_fdk |>
  #   filter(sitename == "DE-Tha") |>
  #   select(forcing) |>
  #   unnest(forcing) |>
  #   rename_with(~stringr::str_c("fdk_", .), 2:17) |>
  #   right_join(
  #     df |>
  #       select(sitename, forcing) |>
  #       filter(sitename == "grid_LON_+013.750_LAT_+050.750") |>
  #       unnest(forcing),
  #     by = c("date")
  #   )
  #
  # tmp |>
  #   visdat::vis_miss()
  #
  # skimr::skim(tmp)
  #
  # tmp |>
  #   ggplot() +
  #   geom_line(aes(date, fapar), color = "royalblue") +
  #   geom_line(aes(date, fdk_fapar))

  # run model
  out <- rsofun::runread_pmodel_f(
    # drivers = df,
    # Remove a day in the leap years...     # TODO: is this really needed for rsofun?
    drivers = mutate(df,
                     forcing = purrr::map(forcing,
                                          ~dplyr::filter(., !(format(date, "%m-%d") == "02-29")))),
    par = par
  )

  # # xxx test run model
  # out2 <- rsofun::runread_pmodel_f(
  #   drivers = df_fdk |>
  #     filter(sitename == "DE-Tha") |>
  #     mutate(forcing = purrr::map(forcing, ~mutate(., patm = zoo::na.approx(patm)))),
  #   par = par
  # )
  #
  # # xxx test
  # tmp2 <- out2 |>
  #   select(data) |>
  #   unnest(data) |>
  #   rename_with(~stringr::str_c("fdk_", .), 2:21) |>
  #   right_join(
  #     out |>
  #       select(data) |>
  #       unnest(data),
  #     by = c("date")
  #   )
  #
  # tmp2 |>
  #   ggplot() +
  #   geom_line(aes(date, wcont), color = "royalblue") +
  #   geom_line(aes(date, fdk_wcont))

  # # xxx test
  # out |>
  #   filter(sitename == "grid_LON_+013.750_LAT_+050.750") |>
  #   unnest(data) |>
  #   ggplot(aes(date, gpp)) +
  #   geom_line()

  dir.create(settings$dir_out,     recursive = TRUE, showWarnings = FALSE)  # TODO: make this emit a message

  outpath <- paste0(settings$dir_out, settings$fileprefix, LON_string, ".rds")
  message(paste("Writing file", outpath, "..."))
  readr::write_rds(out, file = outpath)
}

#' @export
read_forcing_byvar_byLON <- function(var, LON_string, settings){

  if (settings$source_climate == "watch-wfdei"){
    filnam <- paste0(settings$dir_out_tidy_climate, "/", var, "_daily_WFDEI", LON_string, ".rds")
    if (!file.exists(filnam)){
      stop(paste("File does not exist:", filnam))
    }
    df <- readr::read_rds(filnam) |>
    tidyr::unnest(data)
  }

  return(df)
}
