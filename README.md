#  Spark-PMoF: RPMem extension for Spark Shuffle
Spark-PMoF (Persistent Memory over Fabric), RPMem extension for Spark Shuffle, is a Spark Shuffle Plugin which enables persistent memory and high performance fabric technology like RDMA for Spark shuffle to improve Spark performance in shuffle intensive scneario. 

## IMPORTANT NOTE
Spark-PMof has been migrated and integrated to OAP: https://github.com/Intel-bigdata/OAP/tree/master/oap-shuffle/RPMem-shuffle. Please Check OAP for most recent update. 

## Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Benchmark](#benchmark)
- [Usage](#usage)
- [Contact](#contact)

## Introduction

## Installation
Make sure you got [HPNL](https://github.com/Intel-bigdata/HPNL) installed.

```shell
git clone https://github.com/Intel-bigdata/Spark-PMoF.git
cd Spark-PMoF; mvn package -DskipTests -Pspark-2
```

If the pmem hardware is ready,it's useful to test by removing the `-DskipTests` option:

```shell
mvn package
```

## Benchmark

## Usage
This plugin current supports Spark 2.3 and works well on various Network fabrics, including Socket, **RDMA** and **Omni-Path**. Before runing Spark workload, add following contents in spark-defaults.conf, then have fun! :-)

```shell
spark.driver.extraClassPath Spark-PMoF-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.executor.extraClassPath Spark-PMoF-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.shuffle.manager org.apache.spark.shuffle.pmof.PmofShuffleManager
```

## Contact
Chendi Xue, chendi.xue@intel.com 
Jian Zhang, jian.zhang@intel.com 
