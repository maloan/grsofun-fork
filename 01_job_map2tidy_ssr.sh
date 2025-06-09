#!/bin/bash
#SBATCH --job-name="grsofun_ssr"
#SBATCH --output=log/grsofun_ssr_%j.log
#SBATCH --error=log/grsofun_ssr_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=64G         
#SBATCH --cpus-per-task=16          

module purge
module load R/4.4.2-gfbf-2024a
module load UDUNITS/2.2.28-GCCcore-13.3.0
module load PROJ/9.4.1-GCCcore-13.3.0
module load GDAL/3.10.0-foss-2024a

export R_LIBS_USER=/storage/homefs/ak24h624/R/x86_64-pc-linux-gnu-library/4.4

# Navigate to the working directory
cd /storage/homefs/ak24h624/grsofun-fork/vignettes

# Run the script
Rscript run_grsofun.R
