package com.agemooij.experiments.graphs

import scala.math.Ordering.{Int => IntOrdering}

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat._
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextOutput._


object Main {
  def main(originalArgs: Array[String]) = withHadoopArgs(originalArgs) { args =>
    // Read some config info from the command line arguments
    val (inputFile, outputPath) = args.toList match {
      case in :: out :: _ => (in, out)
      case _ => throw new IllegalArgumentException("Invalid arguments to main. Expected an input file and an outpur file.")
    }
    
    // read the original edges into a DList[(Int, Int)]
    val edges: DList[Edge] = extractFromDelimitedTextFile(",", inputFile) {
      case Int(source) :: Int(target) :: _ => Edge(source, target)
    }
    
    val prepared: DList[Node] =
      edges.groupBy(_.source)
           .map {
             case (source, edges) => {
               // no support in Scoobi for secondary sort, so we'll have to sort in memory
               val sortedTargets = edges.map(_.target).toList.sorted(IntOrdering.reverse)
               val partition = if (sortedTargets.head > source) sortedTargets.head else source
               
               Node(source, partition, sortedTargets)
             }
           }
    
    val iterated: DList[Node] = iterate(prepared, 6)
    
    DList.persist(
      toTextFile(prepared.map(node => node.asFormattedOutput), outputPath + "/prepared"),
      toTextFile(iterated.map(node => node.asFormattedOutput), outputPath + "/iterated")
    )
  }
  
  def iterate(nodes: DList[Node], maxIterations: Int): DList[Node] = {
    val nodesAfterOneIteration = 
      nodes
        .flatMap { node => 
          Edge(node.source, node.source, node.partition) :: node.targets.map(target => Edge(node.source, target, node.partition)).toList
        }
        .groupBy(_.target)
        .flatMap {
          case (target, edges) => {
            val largestPartition = edges.map(_.partition).toList.sorted(IntOrdering.reverse).head
            
            edges.map(edge => Edge(edge.source, edge.target, largestPartition))
          }
        }
        .groupBy(_.source)
        .map {
          case (source, edges) => {
            val largestPartition = edges.map(_.partition).toList.sorted(IntOrdering.reverse).head
            
            Node(source, largestPartition, edges.map(_.target).filterNot(_ == source).toList.distinct)
          }
        }
    
    if (maxIterations > 1) iterate(nodesAfterOneIteration, maxIterations - 1)
    else nodesAfterOneIteration
  }
  
  case class Edge(source: Int, target: Int, partition: Int = -1)
  case class Node(source: Int, partition: Int, targets: Iterable[Int]) {
    def asFormattedOutput = "%d\t%d\t%s".format(partition, source, targets.mkString(","))
  }
  
  implicit val edgeFormat = mkCaseWireFormat(Edge, Edge.unapply _)
  implicit val nodeFormat = mkCaseWireFormat(Node, Node.unapply _)
}



