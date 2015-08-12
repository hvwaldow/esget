#!/bin/bash
targetprefix=/store/c2sm/c2sme/cordex
sourceprefix=/net/atmos/data/CORDEX

# while read line; do
#   source=$line
#   target=$(dirname $line)
#   targetdir=${targetprefix}${target#$sourceprefix}
#   target=ela.cscs.ch:$targetdir
#   echo $source
#   echo $target
#   rsync --rsync-path="mkdir -p $targetdir && rsync" -tpvh $source $target
# done < andreas_mie.csv

## check whether they exist on target:
while read line; do
  source=$line
  target=$(dirname $line)
  targetdir=${targetprefix}${target#$sourceprefix}
  targetfile=${targetdir}/$(basename $source)
  ssh -n ela.cscs.ch "stat -c %s $targetfile"
  #target=ela.cscs.ch:$targetdir
done < andreas_mie.csv

