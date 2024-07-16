# issue described by Myriam Terristi (unnumbered, not on github):
# error when making land mask tidy

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
  file_landmask = "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation.nc",
  dir_landmask_tidy = "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation_tidy/",
  file_elv = "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation.nc",
  dir_elv_tidy = "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation_tidy/",
  save_drivers = TRUE,   # whether rsofun driver object is to be saved. Uses additional disk space but substantially speeds up grsofun_run().
  dir_drivers = here::here("input/tidy/"),  # path where rsofun drivers are to be written
  overwrite = FALSE,    # whether files with tidy forcing data and drivers are to be overwritten. If false, reads files if available instead of re-creating them.
  spinupyears = 10,     # model spin-up length
  recycle = 1,          # climate forcing recycling during the spinup
  dir_out = here::here("output/tidy/"),     # path for tidy model output
  dir_out_nc = "xxx",  # xxx not yet handled
  save = list(         # a named list where names correspond to variable names in rsofun output and the value is a string specifying the temporal resolution to which global output is to be aggregated. 
    gpp = "mon"
  ),
  nthreads = 1,   # distribute to multiple nodes for high performance computing - xxx not yet implemented
  ncores_max = 1  # set to 1 for un-parallel run
)

error <- map2tidy::map2tidy(
  nclist = settings$file_landmask,
  varnam = "elevation",
  lonnam = "lon",
  latnam = "lat",
  timenam= NA,
  timedimnam= NA,
  do_chunks = TRUE,
  outdir = settings$dir_landmask_tidy,
  fileprefix = "WFDEI-elevation",
  overwrite = settings$overwrite
)

> rlang::last_trace()
<error/purrr_error_indexed>
Error in `purrr::map()`:
ℹ In index: 1.
Caused by error in `purrr::map()`:
ℹ In index: 1.
Caused by error in `nc_atts.NetCDF()`:
! specified variable not found
---
Backtrace:
     ▆
  1. ├─map2tidy::map2tidy(...)
  2. │ └─purrr::map(...)
  3. │   └─purrr:::map_("list", .x, .f, ..., .progress = .progress)
  4. │     ├─purrr:::with_indexed_errors(...)
  5. │     │ └─base::withCallingHandlers(...)
  6. │     ├─purrr:::call_with_cleanup(...)
  7. │     └─map2tidy (local) .f(.x[[i]], ...)
  8. │       └─map2tidy::nclist_to_df_byilon(...)
  9. │         └─purrr::map(...)
 10. │           └─purrr:::map_("list", .x, .f, ..., .progress = .progress)
 11. │             ├─purrr:::with_indexed_errors(...)
 12. │             │ └─base::withCallingHandlers(...)
 13. │             ├─purrr:::call_with_cleanup(...)
 14. │             └─map2tidy (local) .f(.x[[i]], ...)
 15. │               └─map2tidy::nclist_to_df_byfil(...)
 16. │                 ├─... %>% lubridate::ymd()
 17. │                 ├─ncmeta::nc_atts(filnam, timenam)
 18. │                 └─ncmeta:::nc_atts.character(filnam, timenam)
 19. │                   ├─ncmeta::nc_atts(nc, variable = variable)
 20. │                   └─ncmeta:::nc_atts.NetCDF(nc, variable = variable)
 21. │                     └─base::stop("specified variable not found")
 22. ├─lubridate::ymd(.)
 23. │ └─lubridate:::.parse_xxx(...)
 24. │   ├─base::unlist(lapply(list(...), .num_to_date), use.names = FALSE)
 25. │   └─base::lapply(list(...), .num_to_date)
 26. ├─stringr::str_remove(., " 0:0:0")
 27. │ └─stringr::str_replace(string, pattern, "")
 28. │   └─stringr:::check_lengths(string, pattern, replacement)
 29. │     └─vctrs::vec_size_common(...)
 30. ├─stringr::str_remove(., " 00:00:00")
 31. │ └─stringr::str_replace(string, pattern, "")
 32. │   └─stringr:::check_lengths(string, pattern, replacement)
 33. │     └─vctrs::vec_size_common(...)
 34. ├─stringr::str_remove(., "days since ")
 35. │ └─stringr::str_replace(string, pattern, "")
 36. │   └─stringr:::check_lengths(string, pattern, replacement)
 37. │     └─vctrs::vec_size_common(...)
 38. ├─dplyr::pull(., value)
 39. ├─dplyr::filter(., name == "units")
 40. └─tidyr::unnest(., cols = c(value))
> 
