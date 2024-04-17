# A graduation exam score management system using Apache Spark and Cassandra

The graduation score management project integrates Apache Spark with Cassandra to process large distributed datasets across the Cassandra cluster.

The focused project aims to develop using Apache Spark's distributed system for executing queries, processing, and computations on large datasets. It utilizes the Tkinter interface for efficient user interaction.

## Requirement

- Python 3.8 and lates
- Apache Spark: <https://spark.apache.org/>
- Docker: <https://www.docker.com/>
- Apache Cassandra: <https://cassandra.apache.org/_/index.html>

### Download and set up ApacheSpark

Download full package [Spark](https://spark.apache.org/downloads.html)

or run the following command:

```Bash
pip install pyspark
```
Read <https://spark.apache.org/docs/latest/api/python/getting_started/install.html> to installation.

### Docker install
Dowload docker: <https://docs.docker.com/get-docker/>

### Install cassandra
**Note:** *Cassandra not support for windows, using docker to run cassandra and install driver Cassandra.*
```bash
pip install cassandra-driver
```

## Example programs

### Run Cassandra with docker

- Pull image cassandra

```bash
docker pull cassandra:late
```
- Start Cassandra

```bash
docker run --name Cassandra -p 9042:9042 -d cassandra
```