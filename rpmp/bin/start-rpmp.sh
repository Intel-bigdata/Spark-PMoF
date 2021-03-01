#!/usr/bin/env bash

if [ -L ${BASH_SOURCE-$0} ]; then
  FWDIR=$(dirname $(readlink "${BASH_SOURCE-$0}"))
else
  FWDIR=$(dirname "${BASH_SOURCE-$0}")
fi

if [[ -z "${RPMP_HOME}" ]]; then
  export RPMP_HOME="$(cd "${FWDIR}/.."; pwd)"
fi
export BIN_HOME="$RPMP_HOME/bin"
export CONFIG_HOME="$RPMP_HOME/config"

#Keep this arg for future use.
USER_VARGS=
while [ $# != 0 ]; do
  case "$1" in
    "--help" | "-h")
      echo "--help -h Show this usage information"
      exit
      shift
      ;;
    *)
      USER_VARGS+=" $1"
      shift
      ;;
  esac
done

CONFIG_FILE="$CONFIG_HOME/rpmp.conf"
PROXY_ADDR_KEY="rpmp.network.proxy.address"
SERVER_ADDR_KEY="rpmp.network.server.address"
PROXY_ADDR=
SERVER_ADDR=

while IFS= read -r line; do
    tokens=( $line )
    if [ "${tokens[0]}" = "$PROXY_ADDR_KEY" ]; then
      PROXY_ADDR=${tokens[1]}
    elif [ "${tokens[0]}" = "$SERVER_ADDR_KEY" ]; then
      SERVER_ADDR=${tokens[1]}
    fi
done < $CONFIG_FILE

#Separate address by ','.
IFS=','
#Start RPMP proxy
for addr in $PROXY_ADDR; do
  echo "Starting RPMP proxy on $addr.."
  #Pass addr to RPMP proxy
  ssh $addr "cd $RPMP_HOME; ./proxyMain $addr"
done

#Start RPMP server
for addr in $SERVER_ADDR; do
  echo "Starting RPMP server on $addr.."
  #Pass addr to RPMP server
  ssh $addr "cd $RPMP_HOME; ./main $addr"
done