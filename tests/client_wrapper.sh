#!/bin/bash

if [ -z $1 ]
then
	echo $'\n  client_wrapper.sh <executable> <args>\n  Runs the given executable with MYFS_CONF and LD_PRELOAD set for ThemisIO.\n'
  exit 0
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# echo SCRIPT_DIR = $SCRIPT_DIR

export MYFS_CONF=$SCRIPT_DIR/../myfs.param
export LD_PRELOAD=$SCRIPT_DIR/../client/wrapper.so

"$@"

