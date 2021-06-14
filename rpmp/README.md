# Remote Persistent Memory Pool (RPMP)
RPMP was designed as a fully disaggregated shuffle solution for distributed compute system, leveraging state-of-the-art hardware technologies, 
including persist memory and RDMA, but are not necessarily limited to a shuffle solution. It extends local PM (Persistent Memory) to remote PM 
and targets on a distributed persistent memory pool, providing easy-to-use interfaces, like `malloc` and `free` in standard C library. 

## Contents
- [Prerequisite](#prerequisite) 
- [Hardware Enabling](#hardware-enabling)
- [Installation](#installation)
- [Configuration](#configuration)
- [Benchmark](#benchmark)

## Prerequisite
Hardware:
 - Persistent memory
 - RDMA capable NIC, e.g., Mellanox ConnectX-4 40Gb NIC
Software:
 - [HPNL](https://github.com/Intel-bigdata/HPNL)
 - [hiredis](https://github.com/redis/hiredis)
 - [jsoncpp](https://github.com/open-source-parsers/jsoncpp)
 - [PMDK](https://github.com/pmem/pmdk.git)

## <a id="hardware-enabling"></a>Hardware Enabling

### Config PMem

 - Install ipmctl
 `yum install ipmctl` if the tool is available from yum repo.

 - Install ndctl
  Install basic C library dependencies
  ```
  yum install -y autoconf asciidoctor kmod-devel.x86\_64 libudev-devel libuuid-devel json-c-devel jemalloc-devel
  yum groupinstall -y "Development Tools"
  ```

  Install ndctl which is helper tools and libraries for PMem devices.
  ```
  git clone https://github.com/pmem/ndctl.git
  cd ndctl
  git checkout v63
  ./autogen.sh
  ./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc
      --libdir=/usr/lib64
  make -j
  make check
  make install
  ```
 - List available PMem devices
  `ipmctl show -dimm`
 - Initialize devices in AppDirect mode
  `ipmctl create -goal PersistentMemoryType=AppDirect`
 - Create namespaces in devdax mode. The command can be changed per your hardware and requirement. 
 ```
  Run ndctl create-namespace -m devdax -r region0 -s 120g
  Run ndctl create-namespace -m devdax -r region0 -s 120g
  Run ndctl create-namespace -m devdax -r region1 -s 120g
  Run ndctl create-namespace -m devdax -r region1 -s 120g
 ```
 You're expected to see four devdax namespaces under /dev folder, the name is following the pattern like /dev/daxx.y, x and y are digital number. 



## Installation
### Build prerequisites
HPNL:

```
git clone https://github.com/Intel-bigdata/HPNL.git
cd HPNL
git checkout wip_rpmp
git submodule update --init --recursive
mkdir build; cd build;
cmake -DWITH_VERBS=ON ..     // for RDMA Protocol
make && make install
```

hiredis:
git clone https://github.com/redis/hiredis
cd hiredis
mkdir build; cd build
make && make install

jsoncpp:
```
git clone https://github.com/open-source-parsers/jsoncpp.git
cd jsoncpp
mkdir build; cd build
make && make install
```

PMDK:
```
yum install -y pandoc
git clone https://github.com/pmem/pmdk.git
cd pmdk
git checkout tags/1.8
make -j && make install
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH
echo “export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH” > /etc/profile.d/pmdk.sh
```

Redis-plus-plus:
```
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus
mkdir build
cd build
cmake ..
make
make install
```


### Build for C/C++
```
git clone https://github.com/Intel-bigdata/Spark-PMoF.git
git submodule update --init --recursive
git submodule add -b master https://github.com/redis/hiredis.git rpmp/include/hiredis
git submodule add -b master https://github.com/open-source-parsers/jsoncpp.git rpmp/include/jsoncpp
cd rpmp 
mkdir build
cd build
cmake ..
make && make install
```

## Configuration

Take below configs as a sample.

### Proxy settings
Set proxy address and other proxy service's preference. You can deploy multiple proxy servers for HA. To achieve this,
multiple proxy addresses, separated by comma, should be specified for `rpmp.network.proxy.address`.

```
rpmp.network.proxy.address 0.0.0.0
rpmp.proxy.client.service.port 12350
rpmp.proxy.replica.service.port 12340
rpmp.network.heartbeat-interval.sec 3
rpmp.network.heartbeat.port 12355
```

### Metastore settings
```
rpmp.metastore.redis.ip 127.0.0.1
rpmp.metastore.redis.port 6379
```
Set the redis server's IP address and port. 

### Storage settings
```
rpmp.storage.namespace.size 126833655808
rpmp.storage.namespace.list     /dev/dax0.0,/dev/dax0.1,/dev/dax1.0,/dev/dax1.1
```

Set registered namespaces and memory settings of each namespace.

### Replica settings
```
rpmp.data.replica 2
rpmp.data.min.replica 2
```

Set minimal replica and preferred replica.

## Launch RPMP cluster
Change directory to {RPMP-HOME}/build/bin
- Launch proxy
  ```./proxy-server --current_proxy_addr $addr [--log $log_path]```
- Launch RPMP nodes according to config  
  ```./data-server [--log $log_path]```

Please note if log path is not specified, /tmp/rpmp.log will be used.
  
Alternatively, you can leverage script to start/stop cluster services. To make stop script work as expected, 
please note one node CANNOT plays two same roles, e.g., launch two proxy servers on one node. RPMP proxy and 
data server logs are recorded in $RPMP_HOME/log/proxy-server.log and $RPMP_HOME/log/data-server.log respectively.

- Start cluster
  ```./start-rpmp.sh```
- Stop cluster
  ```./stop-rpmp.sh```

## Benchmark
### Put and get
 - Launch put and get executor
 ```./put_and_get --proxy_addr $addr --port $port```
   
Here, `$addr` is proxy host address (see `rpmp.network.proxy.address`), and `$port` is client service port 
(see `rpmp.proxy.client.service.port`), e.g., `./put_and_get --proxy_addr 172.168.0.101,172.168.0.102 --port 12350`
