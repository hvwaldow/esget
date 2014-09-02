#!/bin/bash

logfile="/home/hvwaldow/scripts/esg/esgdl/log/geomip_getter.log"

while true; do
    started=(`grep "starting job:" $logfile |sed 's/^.*starting job: \(.*\)$/\1/'`)
    finished=(`grep "finished job:" $logfile |sed 's/^.*finished job: \(.*\)$/\1/'`)
    no_running=`ssh atmos "ps aux |grep \"geomip_control.py\" | grep -v grep |wc -l"`
    no_running=$((no_running - 1))
    declare -a collected
    count=0
    for j in "${finished[@]}"; do
        count=$(($count + 1))
        collected[count]=`grep "Found finished process:.*$j" $logfile | sed "s/.*\($j\).*/\1/"`
    done
    date
    echo "started processes: ${#started[@]}"
    echo "running processes: $no_running"
    echo "finished processes: ${#finished[@]}"
    echo "collected processes: ${#collected[@]}"
    
    sleep 5
done
