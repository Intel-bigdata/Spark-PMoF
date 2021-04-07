#!/usr/bin/env bash

if [ -L ${BASH_SOURCE-$0} ]; then
  FWDIR=$(dirname $(readlink "${BASH_SOURCE-$0}"))
else
  FWDIR=$(dirname "${BASH_SOURCE-$0}")
fi

if [[ -z "${RPMP_HOME}" ]]; then
  export RPMP_HOME=$(cd "${FWDIR}/.."; pwd)
fi
export BIN_HOME=$RPMP_HOME/bin
export CONFIG_HOME=$RPMP_HOME/config
PROXY_PID_FILE_PATH=/tmp/rpmp-proxy.pid
SERVER_PID_FILE_PATH=/tmp/rpmp-server.pid

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

. "$BIN_HOME/common.sh"

#Separate address by ','.
IFS=','

#Stop RPMP server
for addr in $SERVER_ADDR; do
  echo "Stopping RPMP server on $addr.."
  ssh $addr "PID=\$(cat SERVER_PID_FILE_PATH 2>/dev/null); kill -9 \$PID > /dev/null 2>&1"
done

#Stop RPMP proxy
for addr in $PROXY_ADDR; do
  echo "Stopping RPMP proxy on $addr.."
  #Pass addr to RPMP proxy
  ssh $addr "PID=\$(cat $PROXY_PID_FILE_PATH 2>/dev/null); kill -9 \$PID > /dev/null 2>&1"
done
