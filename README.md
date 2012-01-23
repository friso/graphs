# Graph Partitioning in Cascading
This is a Hadoop MapReduce based implementation of graph partitioning. The MapReduce code is written using [Cascading](http://www.cascading.org/ "Cascading").

## Input and output
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
This code is companion to a blog post. It's not written yet. The readme will be updated once it is...
