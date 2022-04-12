#!/bin/bash

## job name and output file
#SBATCH -J go_all
#SBATCH -o job.o
#SBATCH -N 16
#SBATCH -n 896
#SBATCH -p debug
#SBATCH -t 00:20:00
#SBATCH -A A-ccsc

cd $SLURM_SUBMIT_DIR

BASEMPIDIR=`grep ^LOCAL_PATH DATA/Par_file | cut -d = -f 2 `
echo "TEMP is set to $BASEMPIDIR"
# script to run the mesher and the solver
# read DATA/Par_file to get information about the run
# compute total number of nodes needed
NPROC_XI=`grep ^NPROC_XI DATA/Par_file | cut -d = -f 2 `
NPROC_ETA=`grep ^NPROC_ETA DATA/Par_file | cut -d = -f 2`
NCHUNKS=`grep ^NCHUNKS DATA/Par_file | cut -d = -f 2 `

# total number of nodes is the product of the values read
numnodes=$(( $NCHUNKS * $NPROC_XI * $NPROC_ETA ))

mkdir -p OUTPUT_FILES

# backup files used for this simulation
cp DATA/Par_file OUTPUT_FILES/
cp DATA/STATIONS OUTPUT_FILES/
cp DATA/CMTSOLUTION OUTPUT_FILES/

# Setting bbThemis
export MYFS_CONF="/path_to/myfs.param"
export LD_PRELOAD=/path_to/wrapper.so

mkdir -p /myfs/specfem
cp -r OUTPUT_FILES/. /myfs/specfem/OUTPUT_FILES
mkdir $BASEMPIDIR

##
## mesh generation
##
sleep 2

echo
echo `date`
echo "starting MPI mesher on $numnodes processors"
echo

time -p ibrun -n $numnodes $PWD/bin/xmeshfem3D

echo "  mesher done: `date`"
echo

# backup important files addressing.txt and list*.txt
cp /myfs/specfem/OUTPUT_FILES/*.txt $BASEMPIDIR/

echo "finished successfully"
echo `date`

##
## forward simulation
##
sleep 2

# set up addressing files from previous mesher run
cp $BASEMPIDIR/addr*.txt /myfs/specfem/OUTPUT_FILES/

echo
echo `date`
echo starting run in current directory $PWD
echo

time -p ibrun -n $numnodes $PWD/bin/xspecfem3D

echo "finished successfully"
echo `date`

cp -r /myfs/specfem/OUTPUT_FILES ./OUTPUT_FILES_r
