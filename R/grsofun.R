grsofun <- function(par, settings){

  # Convert forcing files to a tidy format
  # Checks by file. If tidy already, skips automatically
  error <- grsofun_make_tidy(settings)

  # Run rsofun
  # Parallelizes runs to chunks of longitudinal bands
  error <- grsofun_run(par, settings)

  # Collect output
  # Only variables and at aggregation level required for NetCDF output
  error <- grsofun_collect(settings)

  # Write to NetCDF files
  error <- grsofun_save_nc(settings)

}
