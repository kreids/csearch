awiss

PageRank of graph in DynamoDB, implemented in spark.
This is running on java 1.8, the virtual machine is 1.7!

# How to run this job

## Install Spark
1. Download spark-1.5.2 prebuilt for hadoop 2.6 from: http://spark.apache.org/downloads.html
2. Unzip it somewhere /path/to/spark-1.5.2-bin-hadoop2.6
3. Set SPARK_HOME=/path/to/spark-1.5.2-bin-hadoop2.6 in bash
4. Add $SPARK_HOME/bin to your PATH

## Install Scala 
Do what it says [here](http://www.scala-lang.org/download/install.html) 
Make sure to set SCALA_HOME

## Install SBT
    brew install sbt
If you don't have brew, figure out how to install it [here](http://www.scala-sbt.org/release/tutorial/Setup.html)


## Build With SBT
In this folder, run `sbt` to start downloading and installing everything. This will open a command line for SBT, just quit out.

To build the final jar we're running on Spark, run `sbt assembly`

## Running on Spark
Then, you can run this locally with the following command:
    spark-submit --class "MyPageRank" --master local[4] target/scala-2.11/PageRank-assembly-1.0.jar

