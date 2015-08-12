#!/bin/bash

command="'ps aux |grep \"/bin/bash /home/hvwaldow/esget/tmp/wget/\" |grep -v \"grep\" |wc -l'"

while true; do
  atmoscount=`eval ssh atmos $command`
  lithocount=`eval ssh litho $command`
  thermocount=`eval ssh thermo $command`
  date 
  echo "atmos: $atmoscount wget-processes"
  echo "litho: $lithocount wget-processes"
  echo "thermo: $thermocount wget-processes"

  sleep 5
done
