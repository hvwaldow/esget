#!/bin/bash

server="pcmdi9.llnl.gov"
username="haraldesg"
pass="ESGiertt11"

ESG_CREDENTIALS="./testcredits.pem"

echo $pass |myproxyclient logon -b -S -s $server -l $username -o $ESG_CREDENTIALS 

# - T
#-C $ESG_CERT_DIR
