# issue described by Myriam Terristi (unnumbered, not on github):
# error when making land mask tidy

error <- map2tidy::map2tidy(
  nclist = ???,
  varnam = "elevation",
  lonnam = "lon",
  latnam = "lat",
  do_chunks = TRUE,
  outdir = ???,
  fileprefix = "WFDEI-elevation",
  overwrite = TRUE
)
