#!/bin/bash -e
#SBATCH --job-name=test-scrapping         # Job name
#SBATCH --nodes=1              	     	  # Number of nodes
#SBATCH --ntasks=1                        # Number of tasks (processes)
#SBATCH --output=logs/out-%x.%j.out       # Stdout (%j expands to jobId)
#SBATCH --error=logs/error-%x.%j.err      # Stderr (%j expands to jobId)
#SBATCH --time=14-00:00:00                # Walltime
#SBATCH --mail-type=ALL                   # Mail notification
#SBATCH --mail-user=shinca12@eafit.edu.co # User Email
#SBATCH --partition=longjobs              # Partition

##### ENVIRONMENT CREATION #####
module load python/3.7_miniconda-4.8.3

##### JOB COMMANDS ####
python3 utils/get_dataset.py
