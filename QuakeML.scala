import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vector

println(" Starting QuakeML (features: state, month, year only) ")

val spark = SparkSession.builder()
  .appName("QuakeML")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

println("Loading historical table anmolsandhu_quake_state_month_full ...")
val hist0 = spark.table("anmolsandhu_quake_state_month_full")

println("Schema of quake_state_month_full:")
hist0.printSchema()

println("Year range in historical data:")
hist0.select(min($"year"), max($"year")).show()

val hist = hist0.na.drop()

val statesDF = hist.select("state").distinct()

val maxYear = hist.agg(max($"year")).as[Int].first()
println(s"Max historical year = $maxYear")

val targetFutureYear = 2040  

val futureYearsRange =
  if (maxYear >= targetFutureYear) Seq.empty[Int]
  else (maxYear + 1 to targetFutureYear).toSeq

if (futureYearsRange.isEmpty) {
  println(s"No future years to generate (maxYear >= targetFutureYear=$targetFutureYear)")
} else {
  println(s"Generating future rows for years: ${futureYearsRange.head} .. ${futureYearsRange.last}")
}

val futureYearsDF = futureYearsRange.toDF("year")
val monthsDF = (1 to 12).toDF("month")

val future0 =
  if (futureYearsRange.isEmpty) {
    spark.emptyDataFrame
  } else {
    statesDF
      .crossJoin(futureYearsDF)
      .crossJoin(monthsDF)
      .select(
        $"state",
        $"year",
        $"month",
        lit(0L).as("quake_count"),
        lit("0.0").as("max_mag"),
        lit(0).as("label_quake_ge_4")
      )
  }

if (!future0.head(1).isEmpty) {
  println("Sample of generated future rows:")
  future0.orderBy($"state", $"year", $"month").show(20, truncate = false)
}

val includeFuture = futureYearsRange.nonEmpty

hist.cache()
if (includeFuture) future0.cache()

val histCount   = hist.count()
val futureCount = if (includeFuture) future0.count() else 0L

println(s"Historical rows: $histCount")
if (includeFuture) println(s"Future rows generated: $futureCount")
println(s"Total rows (historical + future): ${histCount + futureCount}")

val all0 =
  if (!includeFuture) hist
  else hist.unionByName(future0)

val all = all0.na.drop()

val stateIndexer = new StringIndexer()
  .setInputCol("state")
  .setOutputCol("stateIndex")
  .setHandleInvalid("skip")

val stateEncoder = new OneHotEncoder()
  .setInputCol("stateIndex")
  .setOutputCol("stateVec")

val assembler = new VectorAssembler()
  .setInputCols(Array("stateVec", "month", "year"))
  .setOutputCol("features")

val lr = new LogisticRegression()
  .setLabelCol("label_quake_ge_4")
  .setFeaturesCol("features")
  .setMaxIter(50)
  .setRegParam(0.01)

val pipeline = new Pipeline()
  .setStages(Array(stateIndexer, stateEncoder, assembler, lr))

val Array(trainSplit, testSplit) = hist.randomSplit(Array(0.8, 0.2), seed = 42L)
println(s"Train size = ${trainSplit.count()}, Test size = ${testSplit.count()}")

println("Training logistic regression model on historical data ...")
val model: PipelineModel = pipeline.fit(trainSplit)

println("Evaluating model on historical test set ...")
val testPreds = model.transform(testSplit)

val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label_quake_ge_4")
  .setRawPredictionCol("rawPrediction")

val auc = evaluator.evaluate(testPreds)
println(s" Historical Test AUC (state,month,year features only) = $auc ")

testPreds
  .select("state", "year", "month", "label_quake_ge_4", "probability", "prediction")
  .show(20, truncate = false)

val modelPath = "/home/hadoop/anmolsandhu/quake_models/lr_state_month_year_only"
println(s"Saving model to $modelPath ...")
model.write.overwrite().save(modelPath)
println("Model saved.")

val probUdf = udf((v: Vector) => if (v != null && v.size > 1) v(1) else 0.0)

println("Scoring ALL rows (historical + future) ...")
val allScored = model.transform(all)
  .withColumn("pred_prob_ge_4", probUdf(col("probability")))
  .select(
    col("state"),
    col("year"),
    col("month"),
    col("quake_count"),
    col("max_mag"),
    col("label_quake_ge_4"),
    col("pred_prob_ge_4")
  )

println("Sample of scored FUTURE rows (years > max historical):")
allScored
  .filter($"year" > maxYear)
  .orderBy($"year", $"state", $"month")
  .show(20, truncate = false)

println("Writing Hive table anmolsandhu_quake_state_month_scored (OVERWRITE) ...")
allScored.write.mode("overwrite").saveAsTable("anmolsandhu_quake_state_month_scored")
println("Finished writing quake_state_month_scored")

spark.stop()
println("DONE")
