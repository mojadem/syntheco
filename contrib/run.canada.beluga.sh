#!/bin/bash

#SBATCH --job-name=syntheco_montreal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=40
#SBATCH --mem=256G
#SBATCH --time=4:00:00
#SBATCH --output=syntheco_mont_%j.log
#SBATCH --account=rrg-ldube

date
module load apptainer

syntheco_home=$HOME/syntheco
syntheco_input=$PWD/canada.bel.yaml
syntheco_output=$PWD/syntheco_can
syntheco_data=$HOME/projects/rrg-ldube/Data/2011-2016
syntheco_image=$HOME/syntheco_0.9.8.sif

srun apptainer run -B $PWD:$PWD -B /home:/home -B $syntheco_data:/mnt/data $syntheco_image /anaconda/bin/python $syntheco_home/main.py -i $syntheco_input -o $syntheco_output

date
