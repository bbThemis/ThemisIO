#!/bin/bash

SCRIPT=themis_client.sh
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function printHelp() {
  cat <<EOF

  themis_client.sh [opt] <client program> [<client program args>]
    opt:
      -j <jobid>
      -n <node count>
      -u <user id>
      -s <# of seconds to sleep before running client program>

  Set environment variables before running a client program.
    LD_PRELOAD - set this to ThemisIO/client/wrapper.so
    MYFS_CONF - set this to ThemisIO/myfs.param
    THEMIS_FAKE_JOBID - set this if -j specified
    THEMIS_FAKE_NNODES - set this if -n specified
    THEMIS_FAKE_USERID - set this if -u specified

  The script will automatically look for wrapper.so and myfs.param in
  a few different directories relative to the current directory and
  the directory in which this script resides.

  MYFS_CONF will not be modified if is set.
  LD_PRELOAD will not be modified if it aready includes the string 'wrapper.so'

EOF
  exit
}


function setPreload() {
  if [[ $LD_PRELOAD = *wrapper.so* ]]
  then
    # LD_PRELOAD seems to already contain wrapper.so
    return
  fi

  # echo need to set LD_PRELOAD
  for dir in $SCRIPT_DIR $SCRIPT_DIR/../client $SCRIPT_DIR/.. client ../client .
  do
    f=$dir/wrapper.so
    if [ -f $f ]
    then
      # echo FOUND $f
      export LD_PRELOAD=$f
      return
    fi
  done
  echo $SCRIPT: Failed to find wrapper.so
}


function setMyfsConf() {
  if [ -n "$MYFS_CONF" ]
  then
    # MYFS_CONF already set
    return
  fi

  for dir in . .. $SCRIPT_DIR $SCRIPT_DIR/..
  do
    myfs=$dir/myfs.param
    if [ -f $myfs ]
    then
      # echo FOUND $myfs
      export MYFS_CONF=$myfs
      return
    fi
  done
  echo $SCRIPT: Failed to find myfs.param
}


while getopts ":hj:n:u:s:" option
do
  case $option in
    j) export THEMIS_FAKE_JOBID=$OPTARG;;
    n) export THEMIS_FAKE_NNODES=$OPTARG;;
    u) export THEMIS_FAKE_USERID=$OPTARG;;
    s) sleep $OPTARG;;
    h)
      printHelp
      exit;;
    \?)
      echo "$SCRIPT: Invalid argument $OPTARG"
      exit;;
  esac
done

shift $(($OPTIND - 1))

setPreload
setMyfsConf

# run the client program

if [ -z "$*" ]
then
  echo $SCRIPT: missing client program name
  exit 1
fi

# echo LD_PRELOAD=$LD_PRELOAD
# echo MYFS_CONF=$MYFS_CONF
# echo running $@

"$@"
