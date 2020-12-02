/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.classification

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator._
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/** Params for linear SVM Classifier. */
// 定义trait 线性SVM分类器的参数
private[classification] trait LinearSVCParams extends ClassifierParams with HasRegParam
  with HasMaxIter with HasFitIntercept with HasTol with HasStandardization with HasWeightCol
  with HasAggregationDepth with HasThreshold with HasMaxBlockSizeInMB {

  /**
   * Param for threshold in binary classification prediction.
   * 二元分类预测阈值的参数
   * For LinearSVC, this threshold is applied to the rawPrediction, rather than a probability.
   * This threshold can be any real number, where Inf will make all predictions 0.0
   * and -Inf will make all predictions 1.0.
   * 对于LinearSVC，这个阈值被应用于rawPrediction（预测），而不是代表可能性。
   * 这个值可以是任何实数。 Inf可以预测为0.0，-In可以预测为1.0（其实就是label)。
   * Default: 0.0
   *
   * @group param
   */
  final override val threshold: DoubleParam = new DoubleParam(this, "threshold",
    "threshold in binary classification prediction applied to rawPrediction")

  // 设置默认值
  setDefault(regParam -> 0.0, maxIter -> 100, fitIntercept -> true, tol -> 1E-6,
    standardization -> true, threshold -> 0.0, aggregationDepth -> 2, maxBlockSizeInMB -> 0.0)
}

/**
 * <a href = "https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM">
 *   Linear SVM Classifier</a>
 *
 * This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
 * Only supports L2 regularization currently.
 * 这个二元分类器使用OWLQN优化器来优化Hinge Loss.
 * 当前仅支持L2正则。
 *
 * Since 3.1.0, it supports stacking instances into blocks and using GEMV for
 * better performance.
 * The block size will be 1.0 MB, if param maxBlockSizeInMB is set 0.0 by default.
 *
 */
@Since("2.2.0")
class LinearSVC @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Classifier[Vector, LinearSVC, LinearSVCModel]
  with LinearSVCParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("linearsvc"))

  /**
   * Set the regularization parameter. 正则化参数
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /**
   * Set the maximum number of iterations. 最多迭代次数
   * Default is 100.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Whether to fit an intercept term.  是否 fit 截距项
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Set the convergence tolerance of iterations. 迭代收敛容忍度
   * Smaller values will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Whether to standardize the training features before fitting the model.
   * 在训练模型前是否对训练特征进行标准化
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)

  /**
   * Set the value of param [[weightCol]].  权重
   * If this is not set or empty, we treat all instance weights as 1.0. 默认1.0
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Set threshold in binary classification.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  /**
   * Sets the value of param [[maxBlockSizeInMB]].
   * Default is 0.0, then 1.0 MB will be chosen.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVC = defaultCopy(extra)

  //训练
  override protected def train(dataset: Dataset[_]): LinearSVCModel = instrumented { instr =>
    // 日志
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, weightCol, featuresCol, predictionCol, rawPredictionCol,
      regParam, maxIter, fitIntercept, tol, standardization, threshold, aggregationDepth,
      maxBlockSizeInMB)

    if (dataset.storageLevel != StorageLevel.NONE) {
      instr.logWarning(s"Input instances will be standardized, blockified to blocks, and " +
        s"then cached during training. Be careful of double caching!")
    }

    // 提取数据，返回rdd  instance{label,weight,features}
    val instances = extractInstances(dataset)
      .setName("training instances")
    
    val (summarizer, labelSummarizer) = Summarizer
      .getClassificationSummarizers(instances, $(aggregationDepth), Seq("mean", "std", "count"))

    // 直方图，柱状图
    val histogram = labelSummarizer.histogram
    // 不合法数据个数
    val numInvalid = labelSummarizer.countInvalid
    // 特征维度
    val numFeatures = summarizer.mean.size

    // 日志
    instr.logNumExamples(summarizer.count)
    instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
    instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)
    instr.logSumOfWeights(summarizer.weightSum)

    var actualBlockSizeInMB = $(maxBlockSizeInMB)
    if (actualBlockSizeInMB == 0) {
      actualBlockSizeInMB = InstanceBlock.DefaultBlockSizeInMB
      require(actualBlockSizeInMB > 0, "inferred actual BlockSizeInMB must > 0")
      instr.logNamedValue("actualBlockSizeInMB", actualBlockSizeInMB.toString)
    }

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }

    // 只支持二分类
    require(numClasses == 2, s"LinearSVC only supports binary classification." +
      s" $numClasses classes detected in $labelCol")
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    // 有不合法的数据，抛出异常
    if (numInvalid != 0) {
      val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
        s"Found $numInvalid invalid labels."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val featuresStd = summarizer.std.toArray //每个特征值的标准差
    val getFeaturesStd = (j: Int) => featuresStd(j)
    val regularization = if ($(regParam) != 0.0) {
      val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures
      // L2正则
      Some(new L2Regularization($(regParam), shouldApply,
        if ($(standardization)) None else Some(getFeaturesStd)))
    } else None

    def regParamL1Fun = (index: Int) => 0.0
    // 优化器
    val optimizer = new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))

    /*
       The coefficients are trained in the scaled space; we're converting them back to
       the original space.
       coefficients系数在缩放空间中训练；我们将它们转换回原始空间
       Note that the intercept in scaled space and original space is the same;
       as a result, no scaling is needed.
       注意intercept在原始空间和缩放空间一样，不需要缩放
     */
    val (rawCoefficients, objectiveHistory) =
      trainImpl(instances, actualBlockSizeInMB, featuresStd, regularization, optimizer)

    if (rawCoefficients == null) {
      val msg = s"${optimizer.getClass.getName} failed."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val coefficientArray = Array.tabulate(numFeatures) { i =>
      if (featuresStd(i) != 0.0) rawCoefficients(i) / featuresStd(i) else 0.0
    }
    val intercept = if ($(fitIntercept)) rawCoefficients.last else 0.0
    createModel(dataset, Vectors.dense(coefficientArray), intercept, objectiveHistory)
  }

  private def createModel(
      dataset: Dataset[_],
      coefficients: Vector,
      intercept: Double,
      objectiveHistory: Array[Double]): LinearSVCModel = {
    val model = copyValues(new LinearSVCModel(uid, coefficients, intercept))
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)

    val (summaryModel, rawPredictionColName, predictionColName) = model.findSummaryModel()
    val summary = new LinearSVCTrainingSummaryImpl(
      summaryModel.transform(dataset),
      rawPredictionColName,
      predictionColName,
      $(labelCol),
      weightColName,
      objectiveHistory)
    model.setSummary(Some(summary))
  }

  private def trainImpl(
      instances: RDD[Instance],
      actualBlockSizeInMB: Double,
      featuresStd: Array[Double],
      regularization: Option[L2Regularization],
      optimizer: BreezeOWLQN[Int, BDV[Double]]): (Array[Double], Array[Double]) = {
    val numFeatures = featuresStd.length
    val numFeaturesPlusIntercept = if ($(fitIntercept)) numFeatures + 1 else numFeatures

    val bcFeaturesStd = instances.context.broadcast(featuresStd)

    val standardized = instances.mapPartitions { iter =>
      val inverseStd = bcFeaturesStd.value.map { std => if (std != 0) 1.0 / std else 0.0 }
      val func = StandardScalerModel.getTransformFunc(Array.empty, inverseStd, false, true)
      iter.map { case Instance(label, weight, vec) => Instance(label, weight, func(vec)) }
    }

    val maxMemUsage = (actualBlockSizeInMB * 1024L * 1024L).ceil.toLong
    val blocks = InstanceBlock.blokifyWithMaxMemUsage(standardized, maxMemUsage)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"training blocks (blockSizeInMB=$actualBlockSizeInMB)")

    val getAggregatorFunc = new BlockHingeAggregator($(fitIntercept))(_)
    val costFun = new RDDLossFunction(blocks, getAggregatorFunc,
      regularization, $(aggregationDepth))

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      Vectors.zeros(numFeaturesPlusIntercept).asBreeze.toDenseVector)

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    blocks.unpersist()
    bcFeaturesStd.destroy()

    (if (state != null) state.x.toArray else null, arrayBuilder.result)
  }
}

@Since("2.2.0")
object LinearSVC extends DefaultParamsReadable[LinearSVC] {

  @Since("2.2.0")
  override def load(path: String): LinearSVC = super.load(path)
}

/**
 * Linear SVM Model trained by [[LinearSVC]]
 */
@Since("2.2.0")
class LinearSVCModel private[classification] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val coefficients: Vector,
    @Since("2.2.0") val intercept: Double)
  extends ClassificationModel[Vector, LinearSVCModel]
  with LinearSVCParams with MLWritable with HasTrainingSummary[LinearSVCTrainingSummary] {

  @Since("2.2.0")
  override val numClasses: Int = 2

  @Since("2.2.0")
  override val numFeatures: Int = coefficients.size

  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, coefficients) + intercept
  }

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false.
   */
  @Since("3.1.0")
  override def summary: LinearSVCTrainingSummary = super.summary

  /**
   * Evaluates the model on a test dataset.
   *
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("3.1.0")
  def evaluate(dataset: Dataset[_]): LinearSVCSummary = {
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)
    // Handle possible missing or invalid rawPrediction or prediction columns
    val (summaryModel, rawPrediction, predictionColName) = findSummaryModel()
    new LinearSVCSummaryImpl(summaryModel.transform(dataset),
      rawPrediction, predictionColName, $(labelCol), weightColName)
  }

  override def predict(features: Vector): Double = {
    if (margin(features) > $(threshold)) 1.0 else 0.0
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    if (rawPrediction(1) > $(threshold)) 1.0 else 0.0
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVCModel = {
    copyValues(new LinearSVCModel(uid, coefficients, intercept), extra).setParent(parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new LinearSVCModel.LinearSVCWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"LinearSVCModel: uid=$uid, numClasses=$numClasses, numFeatures=$numFeatures"
  }
}


@Since("2.2.0")
object LinearSVCModel extends MLReadable[LinearSVCModel] {

  @Since("2.2.0")
  override def read: MLReader[LinearSVCModel] = new LinearSVCReader

  @Since("2.2.0")
  override def load(path: String): LinearSVCModel = super.load(path)

  /** [[MLWriter]] instance for [[LinearSVCModel]] */
  private[LinearSVCModel]
  class LinearSVCWriter(instance: LinearSVCModel)
    extends MLWriter with Logging {

    private case class Data(coefficients: Vector, intercept: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.coefficients, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LinearSVCReader extends MLReader[LinearSVCModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LinearSVCModel].getName

    override def load(path: String): LinearSVCModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
      val model = new LinearSVCModel(metadata.uid, coefficients, intercept)
      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * Abstraction for LinearSVC results for a given model.
 */
sealed trait LinearSVCSummary extends BinaryClassificationSummary

/**
 * Abstraction for LinearSVC training results.
 */
sealed trait LinearSVCTrainingSummary extends LinearSVCSummary with TrainingSummary

/**
 * LinearSVC results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the rawPrediction of each instance.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 */
private class LinearSVCSummaryImpl(
    @transient override val predictions: DataFrame,
    override val scoreCol: String,
    override val predictionCol: String,
    override val labelCol: String,
    override val weightCol: String)
  extends LinearSVCSummary

/**
 * LinearSVC training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the rawPrediction of each instance.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class LinearSVCTrainingSummaryImpl(
    predictions: DataFrame,
    scoreCol: String,
    predictionCol: String,
    labelCol: String,
    weightCol: String,
    override val objectiveHistory: Array[Double])
  extends LinearSVCSummaryImpl(
    predictions, scoreCol, predictionCol, labelCol, weightCol)
    with LinearSVCTrainingSummary
