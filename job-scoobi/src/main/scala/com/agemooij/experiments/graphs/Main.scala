package com.agemooij.experiments.graphs

import scala.math.Ordering.{Int => IntOrdering}

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextOutput._


object Main {
  def main(originalArgs: Array[String]) = withHadoopArgs(originalArgs) { args =>
    // Read some config info from the command line arguments
    val (inputFile, outputFile) = args.toList match {
      case in :: out :: _ => (in, out)
      case _ => throw new IllegalArgumentException("Invalid arguments to main. Expected an input file and an outpur file.")
    }
    
    // read the original edges into a DList[(Int, Int)]
    val edges: DList[(Int, Int)] = extractFromDelimitedTextFile(",", inputFile) {
      case Int(source) :: Int(target) :: _ => (source, target)
    }
    
    val prepared: DList[(Int, Int, List[Int])] =
      edges.groupByKey
           .map {
             case (source, targets) => {
               val sortedTargets = targets.toList.sorted(IntOrdering.reverse)
               
               (sortedTargets.head, source, sortedTargets)
             }
           }
    
    DList.persist(
      toTextFile(prepared.map{
        case (partition, source, targets) => "%d\t%d\t%s".format(partition, source, targets.mkString(","))
      }, outputFile)
    )
  }
}