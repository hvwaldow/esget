#!/bin/bash

killall python
killall wget
ssh thermo "killall wget"
ssh litho "killall wget"
# rm ../log/wget/*
# rm ../log/*
# rm ../tmp/*
