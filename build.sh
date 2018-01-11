#!/bin/bash

SYSTEM=`uname`
if [ "$SYSTEM" = "Linux" ]; then
  FILE=`readlink -f $0`
else
  echo "Error: unknown system"
  exit
fi
HOME=`dirname "$FILE"`
CURRENT=`pwd`
cd "$HOME"
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "-help" ]; then
  echo "usage: $0 [-i] [-h]"
  echo "-i: install"
  echo "-h: help"
  cd "$CURRENT"
  exit
elif [ "$1" = "-i" ]; then
  echo "Installing ..."
  {
    scripts/install.sh
  } || {
    echo "Failed to install :-("
    cd "$CURRENT"
    exit
  }
fi
echo "Finished"
cd "$CURRENT"
