bin=$(pwd)

USER_VARGS=
while [ $# != 0 ]; do
  case "$1" in
    "--help" | "-h")
      echo "--help -h Show this usage information"
      echo "--config Specify or overwrite an configure option."
      echo "-format Format the configured database."
      shift
      ;;
    *)
      USER_VARGS+=" $1"
      shift
      ;;
  esac
done

CONFIG_FILE="./config/rpmp.conf"
PROXY_ADDR_KEY="rpmp.network.proxy.address"
PROXY_ADDR=

while IFS= read -r line; do
    echo "Text read from file: $line"
    tokens=( $line )
    if [ "${tokens[0]}" = "$PROXY_ADDR_KEY" ]; then
      PROXY_ADDR=${tokens[1]}
      break;
    fi
done < $CONFIG_FILE

#Separate address by ','.
IFS=','
for addr in $PROXY_ADDR; do
  #Pass addr to proxy
  ssh $addr; cd $bin; ./proxyMain $addr
done

#TODO: get RPMP server address and launch them one by one.