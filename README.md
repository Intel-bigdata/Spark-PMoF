# PMoF plugin for Spark
Spark-PMoF, PMoF plugin for Spark is a Spark Shuffle Plugin which aims to accelerate Shuffle operation with **AEP+RDMA**.

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
git clone https://github.com/Intel-bigdata/SSO.git
cd SSO; mvn package
```

## Benchmark

## Usage
This plugin current supports Spark 2.3 and works well on various Network fabrics, including Socket, **RDMA** and **Omni-Path**. Before runing Spark workload, add following contents in spark-defaults.conf, then have fun! :-)

```shell
spark.driver.extraClassPath SSO-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.executor.extraClassPath SSO-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.shuffle.manager org.apache.spark.shuffle.pmof.RdmaShuffleManager
```

## Contact
