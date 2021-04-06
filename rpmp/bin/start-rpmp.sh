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
PROXY_LOG_FILE_PATH=$RPMP_HOME/log/rpmp-proxy.log
SERVER_LOG_FILE_PATH=$RPMP_HOME/log/rpmp-server.log
PROXY_PID_FILE_PATH=/tmp/rpmp-proxy.pid
SERVER_PID_FILE_PATH=/tmp/rpmp-server.pid

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

#TODO: check if current process is running

#Separate address by ','.
IFS=','
#Start RPMP proxy
for addr in $PROXY_ADDR; do
  echo "Starting RPMP proxy on $addr.."
  #Pass addr to RPMP proxy
  ssh $addr "cd ${BIN_HOME}; ./proxyMain --current_proxy_addr $addr --log ${PROXY_LOG_FILE_PATH} >> ${PROXY_LOG_FILE_PATH} & \
  touch ${PROXY_PID_FILE_PATH}; echo \$! > ${PROXY_PID_FILE_PATH}"
done

# TODO: parse addr in main.cc
#Start RPMP server
for addr in $SERVER_ADDR; do
  echo "Starting RPMP server on $addr.."
  #Pass addr to RPMP server
  ssh $addr "cd ${BIN_HOME}; ./main --address $addr --log ${SERVER_LOG_FILE_PATH} >> ${SERVER_LOG_FILE_PATH} & \
  touch ${SERVER_PID_FILE_PATH}; echo \$! > ${SERVER_PID_FILE_PATH}"
done