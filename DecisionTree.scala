package com.suthirr.sparkProject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.control.Breaks._


object DecisionTree {
  var meanValues:List[Double]=List()
  var usedFeatures:List[Int]=List()
  var outputs:List[String]=List()
  //var classEntropy:Double=0

  def generateCountForEachAttribute(line: String):List[(String, Int)]={
    val columns=line.split(",")
    var output:List[(String,Int)]=List()
    if (columns.length>0) {

      var classValue:Int=0
      if (columns(1).equals("B"))
        classValue=0
      else
        classValue=1
      for(i <-0 to usedFeatures.length-1)
      {
        columns(usedFeatures(i)+2)="None"
      }
      for (i <- 2 to columns.length-1) {
        if (columns(i) != "None") {
          val data = "" + (i - 2) + '_' + columns(i) + '_' + classValue
          output = output :+ (data -> 1)
        }
      }
    }
    return output
  }

  def calcDataForEntropy(key:String,value:String):(String,String)={
    val keySplits=key.split("_")
    return (keySplits(0),keySplits(1)+"_"+keySplits(2)+"_"+value)
  }
  def reducingTheDataForEntropy(str: String, str1: String): String ={
    return str+":"+str1
  }


  def entropyCalcualation(attributeNo: String, value: String,classEntropy:Double):(Double,Int,Int,Int,Int,Int)={
    var yesData:List[String]=List()
    var noData:List[String]=List()
    val splitValue=meanValues(attributeNo.toInt)
    val valuesArr=value.split(":")
    for (i <- 0 to valuesArr.length-1) {
      if (valuesArr(i).split("_")(0).toDouble<=splitValue){
        yesData=yesData:+valuesArr(i)
      }
      else{
        noData=noData:+valuesArr(i)
      }
    }
    var posYesData:Int=0
    var negYesData:Int=0
    var posNoData:Int=0
    var negNoData:Int=0
    for (i <-0 to yesData.length-1){
     if(yesData(i).split("_")(1)=="1"){
       posYesData = posYesData + yesData(i).split("_")(2).toInt
     }
      else
     {
       negYesData =negYesData + yesData(i).split("_")(2).toInt
     }
    }
    for (i <-0 to noData.length-1){
      if(noData(i).split("_")(1)=="1"){
        posNoData += noData(i).split("_")(2).toInt
      }
      else
      {
        negNoData += noData(i).split("_")(2).toInt
      }
    }
    var totalYesData=posYesData+negYesData
    var totalNoData=posNoData+negNoData
//    Calculation of ENtropy should be done
    var yesEntropy=0.0
    var noEntropy=0.0
    var log2Neg=0.0
    var log2Pos=0.0
    var infoGain=0.0
    if(posYesData!=0 && negYesData!=0 ) {
      log2Pos = math.log10(posYesData.toDouble / totalYesData.toDouble) / math.log10(2)
      log2Neg = math.log10(negYesData.toDouble / totalYesData.toDouble) / math.log10(2)
      yesEntropy = -(((posYesData.toDouble / totalYesData.toDouble) * log2Pos) + ((negYesData.toDouble / totalYesData.toDouble) * log2Neg))
      //yesInfoGain=classEntropy-entropy

    }
    else{
      yesEntropy=0.0}
    if(posNoData!=0 && negNoData!=0 ) {
      log2Pos = math.log10(posNoData.toDouble / totalNoData.toDouble) / math.log10(2)
      log2Neg = math.log10(negNoData.toDouble / totalNoData.toDouble) / math.log10(2)
      noEntropy = -(((posNoData.toDouble / totalNoData.toDouble) * log2Pos) + ((negNoData.toDouble / totalNoData.toDouble) * log2Neg))
      //noInfoGain=classEntropy-entropy
    }
    else{
      noEntropy=0}
    val totalData=totalYesData+totalNoData
    val entropy=((totalYesData.toDouble/totalData.toDouble)*yesEntropy)+((totalNoData.toDouble/totalData.toDouble)*noEntropy)
    infoGain=classEntropy-entropy
    if(entropy==0)
      {
        infoGain=1.0
      }

    return (infoGain,attributeNo.toInt, posYesData,negYesData,posNoData,negNoData)
  }

  def splitPositiveRdd(line:String, attributeNo:Int):Option[String]={
    val splitValue=meanValues(attributeNo)
    val splittedData=line.split(",")
    var newdata:List[String]=List()
    if(splittedData(attributeNo+2).toDouble<=splitValue){

      for(i<-0 to splittedData.length-1)
      {
        if(i==attributeNo+2){
          newdata= newdata :+"None"
        }
        else{
          newdata = newdata:+splittedData(i)
        }
      }
    }
    if(newdata.length>0)
    {
      var outputStr:String=""
      for (i<- 0 to newdata.length-1){
        if(i==0)
        {
          outputStr =outputStr+""+newdata(i)
        }
        else {
          outputStr=outputStr+","+newdata(i)
        }
      }
      return Some(outputStr)
    }
    else
    return None
  }

  def splitNegativeRdd(line:String, attributeNo:Int):Option[String]={
    val splitValue=meanValues(attributeNo)
    val splittedData=line.split(",")
    var newdata:List[String]=List()
    if(splittedData(attributeNo+2).toDouble>splitValue){

      for(i<-0 to splittedData.length-1)
      {
        if(i==attributeNo+2){
          newdata= newdata :+"None"
        }
        else{
          newdata = newdata:+splittedData(i)
        }
      }
    }
    if(newdata.length>0)
    {
      var outputStr:String=""
      for (i<- 0 to newdata.length-1){
        if(i==0)
        {
          outputStr =outputStr+""+newdata(i)
        }
        else {
          outputStr=outputStr+","+newdata(i)
        }
      }
      return Some(outputStr)
    }
    else
      return None
  }

  def testingDecisionTree(line:String): Option[(String,Int)] ={
    val splittedFeatureValues=line.split(",")
    for (i<- 0 to outputs.length-1) {
      val output = outputs(i).split(":")
      val conditions = output(0).split(",")
      var checkCount=0
      for (j <- 0 to conditions.length - 1) {
        breakable {
          val attributeNo = conditions(j).split("-")(1).toInt
          if (conditions(j).split("-")(0) == "if") {
            if (splittedFeatureValues(attributeNo + 2).toDouble > meanValues(attributeNo)) {
              break()
            }
            else
              checkCount = checkCount+1

          }
          else{
            if(splittedFeatureValues(attributeNo+2).toDouble<=meanValues(attributeNo))
              break()
            else
              checkCount=checkCount+1
          }
        }
      }
      if(checkCount==3){
        var actualClass=0
        if(splittedFeatureValues(1)=="M") {
            actualClass=1
          }
        else
          actualClass=0
        if(actualClass==0 && output(1).toDouble==0.0)
          return Some("TP",1)
        else if(actualClass==1 && output(1).toDouble==1.0)
          return Some("TN",1)
        else if(actualClass==0 && output(1).toDouble==1.0)
          return Some("FP",1)
        else if(actualClass==1 && output(1).toDouble==0.0)
          return Some("FN",1)
      }
    }
    return None
  }
  def generateInfoGain(trainingData:RDD[String],iteration:Int,tree:String ): Unit ={
    var classEntropyRdd= trainingData.map(x=>(x.split(",")(1),1)).reduceByKey((v1,v2)=> v1+v2).collect()
    var posClassCount=0
    var negClassCount=0
    for (i <- 0 to classEntropyRdd.length-1)
    {
      if(i==0)
        posClassCount=classEntropyRdd(i)._2
      else
        negClassCount=classEntropyRdd(i)._2
    }

    var totClassCount=posClassCount+negClassCount
    val log2pos=math.log10(posClassCount.toDouble/totClassCount.toDouble)/math.log10(2)
    val log2neg=math.log10(negClassCount.toDouble/totClassCount.toDouble)/math.log10(2)


    val classEntropy= -(((posClassCount.toDouble/totClassCount.toDouble)*log2pos)+((negClassCount.toDouble/totClassCount.toDouble)*log2neg))



    val trainingRdd=trainingData.flatMap(generateCountForEachAttribute).reduceByKey((x,y)=>x+y)

    val dataForEntropy=trainingRdd.map(x=>calcDataForEntropy(x._1,x._2.toString)).reduceByKey((x,y)=>reducingTheDataForEntropy(x,y))
    //    dataForEntropy.foreach(println)
    val entropyforEachAttributeRDD= dataForEntropy.map(x=>entropyCalcualation(x._1,x._2,classEntropy)).max()
//    println(entropyforEachAttributeRDD)

//    if(entropyforEachAttributeRDD._1==NaN)
//      {
//        println("hii")
//      }
    usedFeatures=usedFeatures :+entropyforEachAttributeRDD._2.toInt
    val positiveRddData=trainingData.flatMap(x=>splitPositiveRdd(x,entropyforEachAttributeRDD._2))
    val negativeRddData=trainingData.flatMap(x=>splitNegativeRdd(x,entropyforEachAttributeRDD._2))

    if(iteration!=3) {
      var temptree=""
//      ifCond:String=entropyforEachAttributeRDD_2
      for (i<-1 to iteration){
        print(" ")
      }
      println("if (" +entropyforEachAttributeRDD._2.toString+"<="+meanValues(entropyforEachAttributeRDD._2)+")")
      if(iteration==1){
        temptree="if-"+entropyforEachAttributeRDD._2
      }
      else {
        temptree = tree + ",if-" + entropyforEachAttributeRDD._2
      }
      generateInfoGain(positiveRddData, iteration + 1,temptree)
      for (i<-1 to iteration){
        print(" ")
      }
      println("elif (" +entropyforEachAttributeRDD._2.toString+">"+meanValues(entropyforEachAttributeRDD._2)+")")
      if(iteration==1){
        temptree="else-"+entropyforEachAttributeRDD._2
      }
      else {
        temptree = tree + ",else-" + entropyforEachAttributeRDD._2
      }
      generateInfoGain(negativeRddData,iteration+1,temptree)
    }
    if(iteration==3)
      {
        var tempTree=tree+",if-"+entropyforEachAttributeRDD._2+":"
        if(entropyforEachAttributeRDD._3>entropyforEachAttributeRDD._4)
          {
            tempTree=tempTree+"1.0"
            for (i<-1 to iteration){
              print(" ")
            }
            println("if (" +entropyforEachAttributeRDD._2.toString+"<="+meanValues(entropyforEachAttributeRDD._2)+")")
            for (i<-1 to iteration){
              print(" ")
            }
            println("  predict=1.0")
//            println(entropyforEachAttributeRDD._2+" predict=1.0")
          }
        else
          {
            tempTree=tempTree+"0.0"
            for (i<-1 to iteration){
              print(" ")
            }
            println("if (" +entropyforEachAttributeRDD._2.toString+"<="+meanValues(entropyforEachAttributeRDD._2)+")")
            for (i<-1 to iteration){
              print(" ")
            }
            println("  predict=0.0")
//            println(entropyforEachAttributeRDD._2+" predict=0.0")
          }
//        println(tempTree)
        outputs= outputs :+tempTree
        tempTree=tree+",else-"+entropyforEachAttributeRDD._2+":"
        if(entropyforEachAttributeRDD._5>entropyforEachAttributeRDD._6)
        {
          tempTree=tempTree+"1.0"
          for (i<-1 to iteration){
            print(" ")
          }
          println("elif (" +entropyforEachAttributeRDD._2.toString+">"+meanValues(entropyforEachAttributeRDD._2)+")")
          for (i<-1 to iteration){
            print(" ")
          }
          println("  predict=1.0")
        }
        else
        {
          tempTree=tempTree+"0.0"
          for (i<-1 to iteration){
            print(" ")
          }
          println("elif (" +entropyforEachAttributeRDD._2.toString+">"+meanValues(entropyforEachAttributeRDD._2)+")")
          for (i<-1 to iteration){
            print(" ")
          }
          println("  predict=0.0")
        }
//        var tempTree=tree
//        println(tempTree)
        outputs= outputs :+tempTree
      }


//    positiveRddData.foreach(println)

//     println()
//    for(i <-0 to iteration-0){
//      print(" ")
//    }
//
//    print(entropyforEachAttributeRDD)
    //    entropyforEachAttributeRDD.foreach(println)

  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","DecisionTree")
    val inputData=sc.textFile("C:\\Users\\suthi\\Downloads\\wdbc.data")

    val trainTestSplit= inputData.randomSplit(Array(0.7,0.3))
    val trainingData=trainTestSplit(0)
    val testingData=trainTestSplit(1)
    for (i <- 2 to 31){
      val max=trainingData.map(x=>x.split(",")(i).toDouble).distinct().max()
      val min=trainingData.map(x=>x.split(",")(i).toDouble).distinct().min()
      meanValues =meanValues:+((max+min)/2)
    }
    generateInfoGain(trainingData,1,"")

//    println("\n\n\n")
//    outputs.foreach(println)
//    println()
    val confusionMatrix=testingData.flatMap(testingDecisionTree).reduceByKey((v1,v2)=>v1+v2).collect()
    //confusionMatrix.foreach(println)
    var truePredict=0
    var totalPredict=0
    for (i <- 0 to confusionMatrix.length-1){
      println(confusionMatrix(i)._1+","+confusionMatrix(i)._2)
      if(confusionMatrix(i)._1=="TP" || confusionMatrix(i)._1=="TN"){
        truePredict=truePredict+confusionMatrix(i)._2
      }
      totalPredict=totalPredict+confusionMatrix(i)._2
    }
    println("Accuracy: "+(truePredict/totalPredict.toFloat))
//    println(confusionMatrix)

  }
}
