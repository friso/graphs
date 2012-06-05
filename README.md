# Graph Partitioning and other fun with Cascading and Neo4j
*Note: initially this was just a demonstration of graph partitioning in MapReduce using Cascading. But now, there is also a Scoobi version of the same and a graph viewer which visualizes graphs taken from a local Neo4j instance and there will be some more stuff (like a Neo4j batch importer). Accompanying blog posts will also follow...*

This is a Hadoop MapReduce based implementation of graph partitioning. The MapReduce code is written using [Cascading](http://www.cascading.org/ "Cascading").

## Neo4j import job
Recently, I added a Neo4j+HTML5 based graph viewer and Neo4j importer to this sample code to accommodate a number of talks given at various conferences and hopefully a future blog post (when I find the time). The import takes a file containing nodes and a file containing edges and outputs a Neo4j readable database, that can be used to drop into a Neo4j server instance.
Node file layout (omit the header line in the actual file):
```
NodeID	attribute1	attribute2		...		...
123		friso		vollenhoven
456		other		dude			...		....
```
The first column is a node ID, which must be a positive integer that fits within the range of a Java long. The other attributes are arbitrary values, which are used as attributes. You can have as many attributes as you like, but each line has to have the same number of attributes (it's not sparse, although Neo4j does support that).

Edges file layout (omit the header line in the actual file):
```
FromNodeID	ToNodeId	attribute1	...	...
123			456			500
```
The edges files marks edges using the node IDs as found in the nodes file. Edges can also have any number of attributes, as long as each have has the same number of attributes.

Running the job:
```
hadoop jar neo4j-import nodes edges db attr1,attr2 attr1,attr2
```

In the above example the nodes and edges arguments point to a nodes and edges file respectively. These need to be local, not on HDFS, so use a hadoop fs -cat to grab them if they were produced in Hadoop. The db argument points to the directory where the graph DB needs to be created. The job will NOT overwrite this if it already exists. The two attribute lists are the attribute names for the nodes and edges respectively. The importer will use these names as property names as well as in the indexes. The importer tries to guess the data type of properties when inserting using some regex logic. All properties are indexed.

## Graph creation from BGP dump job
The job is in graphs/job/src/main/java/nl/waredingen/graphs/bgp/PrepareBgpGraphJob.java and is runnable from the command line using the prepare-bgp option:
```
hadoop jar prepare-bgp input-file nodes edges
```
In the above example, the input-file is a file containing BGP messages as available from the RIPE NCC RIS data project using the bgpdump tool available from RIPE NCC as well. The nodes en edges arguments are the output path where the job will produce the nodes and edges files respectively. Very likely when playing around with these examples, you will be using your own data to generate graphs, for which you only need some way of generating a nodes and edges file and run the Neo4j import job described above.

## Graph partitioning Input and output
The program takes a graph as input represented as a text file. Each line of the file must contain a single edge of the graph in the form of `source,target`, where source and target are numeric node IDs. Example:

```
0,1
0,2
1,3
2,3
```

The above represents the graph:

```
0 --- 1
|      \
|       \
2 ------ 3
```

The output of the program will the same graph represented as source nodes with adjacency lists prefixed with a partition ID seperated by tabs. The partition IDs are derived from the node IDs. Each partition gets assigned the largest node ID that exists within that partition. For the following input:

```
0,1
0,2
1,2
4,5
4,6
```

It will produce (columns are: partition ID \<tab\> source node ID \<tab\> adjacency list):

```
2	0	1,2
2	1	2
6	4	5,6
```

## Running
There are two partitioning implementations. One takes a disconnected graph and finds all partitions. The other (called 'with-flags'), will take a possibly connected graph and partition the graph by adding partition boundaries at nodes with a incoming edge count larger than a given threshold.

Both of the implementations consist of a prepare step and a iterative step. The Java code has a main (`nl.waredingen.graphs.Main`) class that is intented to be run using Hadoop (with `hadoop jar`). It takes one of six argument lists:

* prepare \[input\] \[output\]
* iterate \[input\] \[output\]
* iterate-once \[input\] \[output\]
* prepare-with-flags \[input\] \[output\] \[threshold\]
* iterate-with-flags \[input\] \[output\]
* iterate-once-with-flags \[input\] \[output\]

The prepare steps take an input file as described above and turn it into a file that can be fed to the iterative step. This file is the same format as the output file. The iterate-once arguments will perform a single iteration of the partitioning algorithm. The iterate variant will run the iterative step until it converges.

## More information
This code is companion to a blog post. First part is here: http://waredingen.nl/graph-partitioning-in-mapreduce-with-cascadin, second part is here: http://waredingen.nl/graph-partitioning-part-2-connected-graphs
