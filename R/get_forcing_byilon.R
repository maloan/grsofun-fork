

get_forcing_byilon <- function(
  ilon,
  source_climate = "watch-wfdei",
  dir_climate = "~/data/watch_wfdei/tidy/",
  outdir = "~/data/rsofun_global/"
){

  # climate --------------------------------------------------------------------
  ## watch-wfdei ---------------------------------------------------------------
  if (source_climate == "watch-wfdei"){

    # collect data from tidy data written to rds files
    df <- readr::read_rds(paste0("~/data/watch_wfdei/tidy/Tair_daily__ilon_", ilon, ".rds")) |>
      tidyr::unnest(data) |>
      dplyr::left_join(
        readr::read_rds(paste0("~/data/watch_wfdei/tidy/Rainf_daily__ilon_", ilon, ".rds")) |>
          tidyr::unnest(data),
        by = c("lon", "lat", "time")
      ) |>
      dplyr::left_join(
        readr::read_rds(paste0("~/data/watch_wfdei/tidy/Snowf_daily__ilon_", ilon, ".rds")) |>
          tidyr::unnest(data),
        by = c("lon", "lat", "time")
      ) |>
      dplyr::left_join(
        readr::read_rds(paste0("~/data/watch_wfdei/tidy/Qair_daily__ilon_", ilon, ".rds")) |>
          tidyr::unnest(data),
        by = c("lon", "lat", "time")
      ) |>
      dplyr::left_join(
        readr::read_rds(paste0("~/data/watch_wfdei/tidy/SWdown_daily__ilon_", ilon, ".rds")) |>
          tidyr::unnest(data),
        by = c("lon", "lat", "time")
      ) |>
      dplyr::left_join(
        readr::read_rds(paste0("~/data/watch_wfdei/tidy/PSurf_daily__ilon_", ilon, ".rds")) |>
          tidyr::unnest(data),
        by = c("lon", "lat", "time")
      )

    # change units and transform variables
    my_calc_vpd = Vectorize(calc_vpd)

    kfFEC <- 2.04
    df <- df |>
      ungroup() |>
      dplyr::rename(patm = PSurf,
             date = time) |>
      dplyr::mutate(temp = Tair - 273.15,
                    ppfd = SWdown * kfFEC * 1.0e-6,  # W m-2 -> mol m-2 s-1
                    vapr = calc_vp(
                      qair = Qair,
                      patm = patm
                    )) |>
      dplyr:: mutate(vpd = my_calc_vpd(
          eact = vapr,
          tc = temp,
          patm = patm))

  }



  # |>
  #   select(date, temp, vpd, ppfd, netrad, patm, snow, rain, tmin, tmax, fapar, co2, ccov)
  #

  filnam <- paste0(outdir, "rsofun_forcing_", ilon, ".rds")
  message(paste0("Writing file ", filnam, " ..."))
  readr::write_rds(df, file = filnam)
}
