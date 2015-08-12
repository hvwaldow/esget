#!/bin/bash

for domain in EUR-44 EUR-11; do
  for variable in pr tas; do
    for table in esgffiles localfiles failed unlinked failed; do
      ./report_jan $domain $variable mon $table
    done
  done
  for table in esgffiles localfiles failed unlinked failed; do
    ./report_jan $domain prsn day $table
  done
done
