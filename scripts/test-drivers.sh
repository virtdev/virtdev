#!/bin/sh
CWD=`readlink -f $0`
DIR=`dirname $CWD`
HOME=`dirname $DIR`
BIN="$HOME/bin"
CMD_CREATE="$BIN/create"
USERID="00000000000000000000000000000000"

echo "creating devices..."
DRIVERS=('Blob')
for i in ${DRIVERS[*]}; do
  `$CMD_CREATE -s -t $i -u $USERID`
  echo "$i pass"
done
