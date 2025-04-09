#!/bin/sh

# download globle 0.16*0.16 GFS wave data 
 
dt1=$1
dir=$2

perl get_gfs_wave.pl data ${dt1}00 0 192 3 all all ${dir}

