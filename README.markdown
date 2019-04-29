
# ATC'19 EdgeWise Prototype
This repository contains the EdgeWise Prototype for the [ATC'19](https://www.usenix.org/conference/atc19) paper:

*Xinwei Fu, Talha Ghaffar, James C. Davis, Dongyoon Lee, "[EdgeWise: A Better Stream Processing Engine for the Edge](http://people.cs.vt.edu/fuxinwei/)", USENIX Annual Technical Conference (ATC), Renton, WA, USA, July 2019.*

### Branches
This repository contains 2 branches based on [Apache Storm v1.1.0](https://github.com/apache/storm/tree/v1.1.0): 
  - EdgeWise branch ------ EdgeWise Runtime;
  - Storm-metrics branch ------ Default Storm Runtime with metrics.

### How to Compile and Package
```sh
$ mvn install -DskipTests 
$ cd storm-dist/binary
$ mvn package -Dgpg.skip 
# Generated package will be here: storm-dist/binary/target/apache-storm-1.1.0.tar.gz
```

### How to Use
Set up the Storm Cluster as described [here](https://storm.apache.org/releases/1.0.6/Setting-up-a-Storm-cluster.html).
Develop you topology as described follow:
```java
// Build your topology here
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout(...);
builder.setBolt(...);

// Setup EdgeWise Config
Config conf = new Config();

// Metrics
conf.put("get_wait_time", true);
conf.put("get_empty_time", true);
conf.put("get_queue_time", true);
conf.put("queue_time_sample_freq", ...);
conf.put("info_path", "...");

// Choose the scheduler
// Using Congestion-Aware scheduler
conf.put("policy", "eda-dynamic");
// Using WP+Random
conf.put("policy", "eda-random");
// Using WP+MinMemory
conf.put("policy", "eda-chain");
conf.put("chain-bolt-ids", "...");
conf.put("chain-bolt-prios", "...");
// Using WP+MinLatency
conf.put("policy", "eda-min-lat");
conf.put("min-lat-bolt-ids", "...");
conf.put("min-lat-bolt-prios", "...");

// Choose the consumption policy
// consume all
conf.put("consume", "all");
// consume half
conf.put("consume", "half");
// consume at-most-constant
conf.put("consume", "constant");
conf.put("constant", ...);

// Default Storm Runtime doesn't support scheduler and consumption policy

// Submit your topology to a cluster
StormTopology stormTopology  =  builder.createTopology();
StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
```
Then submit you topology to the cluster as described [here](http://storm.apache.org/releases/1.0.6/Tutorial.html).
