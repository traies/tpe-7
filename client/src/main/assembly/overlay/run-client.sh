#!/bin/bash

java   -Daddresses="$1" -DinPath=$2 -Dquery=$3 -DoutPath=benchmark/output$3.csv   -DtimeOutPath=benchmark/time$3.csv -Dn=10 -Dprov="Santa Fe" -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"

