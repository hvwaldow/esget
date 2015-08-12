#!/bin/bash
##############################################################################
# wget-script - template
# Part of Esget - HvW - 2015/04/08
###############################################################################

download_files="'/tas_EUR-44_IPSL-IPSL-CM5A-MR_historical_r1i1p1_IPSL-INERIS-WRF331F_v1_mon_200101-200512.nc' 'http://esgf.extra.cea.fr/thredds/fileServer/CORDEX/output/EUR-44/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/IPSL-INERIS-WRF331F/v1/mon/tas/v20140301/tas_EUR-44_IPSL-IPSL-CM5A-MR_historical_r1i1p1_IPSL-INERIS-WRF331F_v1_mon_200101-200512.nc' 'MD5' '12345'"

DL_DIR="./download"

customwget=" -x -nH -P $DL_DIR --no-check-certificate"

COOKIE_JAR="./globus/cookies"

ESG_CERT="./globus/credentials.pem"

[[ -f $COOKIE_JAR ]] || touch $COOKIE_JAR

PKI_WGET_OPTS="--certificate=$ESG_CERT --private-key=$ESG_CERT --save-cookies=$COOKIE_JAR --load-cookies=$COOKIE_JAR"

wget="wget $customwget  -c $PKI_WGET_OPTS"
    
    while read line
    do
        # read csv here document into proper variables
        eval $(awk -F "' '" '{$0=substr($0,2,length($0)-2); $3=tolower($3); print "file=\""$1"\";url=\""$2"\";chksum_type=\""$3"\";chksum=\""$4"\""}' <(echo $line) )

        # Process the file
        echo FILE = "$file"
	echo URL = "$url"
	echo CHKSUM TYPE = "$chksum_type"
	echo CHKSUM = "$chksum"
	echo "Downloading ..."
	$wget  $url
    done <<<"$download_files"
        

        
