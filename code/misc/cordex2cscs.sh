#!/bin/bash
#
# Synchronizes the CORDEX mirror a IAC with CSCS storage
# uses multiple hosts, multiple connections per host,
# and multiple entry nodes at CSCS in parallel to speed up the transfer.

## returns list of "leaf-directories" that are to be synced,
## sorted from larges to smallest size

storage_root=/net/atmos/data/CORDEX/
exclude_trees="AFR-44 db download TEST"
targets="148.187.1.6 148.187.1.7 148.187.1.8"
fromhosts="5/bio,5/atmos,5/thermo,5/litho"
targetdir="/store/c2sm/c2sme/cordex"
dbname=db/cordex_eur_ALL.db


subtrees=`find $storage_root -maxdepth 1 -mindepth 1 -type d`
echo "subtrees = ${subtrees[@]}"
excl_subtrees=`parallel echo ${storage_root}{} ::: $exclude_trees`
echo "excl_subtrees: $excl_subtrees"
subtrees_f=($subtrees)
for excl in $excl_subtrees; do
    subtrees_f=(${subtrees_f[@]/$excl/})
done
echo filtered subtrees: ${subtrees_f[@]}
echo "Making list of \"leaf-directories\""
parallel -k -j 40 find {} -type f ::: ${subtrees_f[@]} |\
  parallel -j 40 dirname |uniq |parallel -j 40 du |sort -g -r | awk '{print $2}'>syncdirs.txt

# inserting "local-dir-dot" makes rsync transfer only path starting after storage-root
sed -i "s:^\($storage_root\):\1\./:" syncdirs.txt

parallel -j5 --filter-hosts --cleanup --joblog cscstransfer.log \
    --basefile syncdirs.txt --xapply -S $fromhosts --sshdelay 0.1 \
    rsync --delete -rvutR {1} {2}:$targetdir :::: syncdirs.txt ::: $targets

onetarget=`echo $targets |cut -d ' ' -f 1`
rsync -vt --progress $storage_root/$dbname ${onetarget}:${targetdir}/$dbame
 

