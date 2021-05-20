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
export LOG_HOME=$RPMP_HOME/log
PROXY_SERVER_LOG_PATH=$LOG_HOME/proxy-server.log
DATA_SERVER_LOG_PATH=$LOG_HOME/data-server.log
PROXY_SERVER_PID_PATH=/tmp/rpmp-proxy-server.pid
DATA_SERVER_PID_PATH=/tmp/rpmp-data-server.pid

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
  ssh $addr "cd ${BIN_HOME}; mkdir -p ${LOG_HOME}; \
  ./proxy-server --current_proxy_addr $addr --log ${PROXY_SERVER_LOG_PATH} >> ${PROXY_SERVER_LOG_PATH} & \
  echo \$! > ${PROXY_SERVER_PID_PATH}"
done

# TODO: parse addr in main.cc
#Start RPMP server
for addr in $SERVER_ADDR; do
  echo "Starting RPMP server on $addr.."
  #Pass addr to RPMP server
  ssh $addr "cd ${BIN_HOME}; mkdir -p ${LOG_HOME}; \
  ./data-server --address $addr --log ${DATA_SERVER_LOG_PATH} >> ${DATA_SERVER_LOG_PATH} & \
  echo \$! > ${DATA_SERVER_PID_PATH}"
done