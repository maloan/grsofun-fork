# issue described by Myriam Terristi (unnumbered, not on github):
# error when making land mask tidy

error <- map2tidy::map2tidy(
  nclist = "~/data/archive_legacy/landmasks/WFDEI-elevation.nc",  # "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation.nc"
  varnam = "elevation",
  lonnam = "lon",
  latnam = "lat",
  timenam= NA,
  timedimnam= NA,
  do_chunks = TRUE,
  outdir = "~/data/archive_legacy/landmasks/WFDEI-elevation_tidy/",  # "/Net/Groups/BGI/scratch/mterristi/PhD/Pmodel/grsofun/data/elevation/WFDEI-elevation_tidy/"
  fileprefix = "WFDEI-elevation",
  overwrite = TRUE
)
