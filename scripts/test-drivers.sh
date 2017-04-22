#!/bin/bash
CWD=`readlink -f $0`
DIR=`dirname $CWD`
HOME=`dirname $DIR`
BIN=$HOME/bin
CMD_CREATE=$BIN/create
USERID="00000000000000000000000000000000"

echo "Creating devices..."
DRIVERS=('Blob')
for i in ${DRIVERS[*]}; do
	device=`$CMD_CREATE -s -t $i -u $USERID`
	echo "device=$device"
	echo "$i passed"
done
