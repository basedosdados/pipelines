# Convert the V-Dem RData files shipped in the official vdemdata R package
# (github.com/vdeminstitute/vdemdata) to parquet, so the Python cleaning step
# can read them. The v-dem.net downloads sit behind a form that asks for an
# email address; the package carries the same Full+Others country-year release.
#
# Usage: Rscript models/se_gu_vdem/code/extract_rdata.R [data_dir]
# data_dir defaults to $SE_GU_VDEM_DATA_DIR or ~/Library/Caches/se_gu_vdem_data.

args <- commandArgs(trailingOnly = TRUE)
data_dir <- if (length(args) > 0) args[[1]] else Sys.getenv(
  "SE_GU_VDEM_DATA_DIR", path.expand("~/Library/Caches/se_gu_vdem_data")
)
input <- file.path(data_dir, "input")
base_url <- "https://github.com/vdeminstitute/vdemdata/raw/master/data"

for (f in c("vdem", "codebook")) {
  path <- file.path(input, paste0(f, ".RData"))
  if (!file.exists(path)) {
    dir.create(input, recursive = TRUE, showWarnings = FALSE)
    download.file(paste0(base_url, "/", f, ".RData"), path, mode = "wb")
  }
  env <- new.env()
  load(path, envir = env)
  arrow::write_parquet(env[[f]], file.path(input, paste0(f, ".parquet")))
  cat(f, ":", nrow(env[[f]]), "rows,", ncol(env[[f]]), "columns\n")
}
