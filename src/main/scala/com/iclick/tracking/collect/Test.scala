package com.iclick.tracking.collect
import com.iclick.spark.realtime.util.Config
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import java.util.Random


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorAssembler }
import org.apache.spark.ml.param.ParamMap

object Test {
  final val vertor_size = 100
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SMS Mess classification(Ham or Spm)").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    val parsedRDD = sc.textFile("d:\\wilson.zhou\\Desktop\\spark资料和计算广告相关\\SMSSpamCollection").map(_.split("\t")).map {
      eachRow =>
        {
          (eachRow(0), eachRow(1).split(" "))
        }
    }

    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")
    val labelInder = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(msgDF)

    val word2Vec = new Word2Vec().setInputCol("message").setOutputCol("features").setVectorSize(vertor_size).setMinCount(1)

    val layers = Array[Int](vertor_size, 6, 5, 2)
    val mlpc = new MultilayerPerceptronClassifier().setLayers(layers).
      setBlockSize(512).setSeed(1234L).setMaxIter(128).setFeaturesCol("features").setLabelCol("indexedLabel")

    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabel").
      setLabels(labelInder.labels)

    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    val pipeline = new Pipeline().setStages(Array(labelInder, word2Vec, mlpc, labelConverter))
    val model = pipeline.fit(trainingData)
 
    
    
   val  bp= model.stages(2).asInstanceOf[MultilayerPerceptronClassificationModel]
    println("bp神经网络训练完成")
   println(bp.layers)
   println(bp.numFeatures)
   println(bp.weights.toString)
   println(bp.getPredictionCol)
    
    val predictionResultDF = model.transform(testData)
    //below 2 lines are for debug use
    predictionResultDF.printSchema
    predictionResultDF.select("message", "label", "predictedLabel").show(30)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")

  }

}