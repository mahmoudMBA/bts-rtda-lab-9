import views.{AgeGenderView, DeveloperOpenSourcePercentageView}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame, spark: SparkSession) {

  def createDeveloperOpenSourcePercentageView() = {

    val modelEncoder = Encoders.product[DeveloperOpenSourcePercentageView]

    surveyDataFrame.groupBy("OpenSourcer")
      .count()
      .withColumn("percentage", (col("count")/
        sum("count").over())*100)
      .as[DeveloperOpenSourcePercentageView](modelEncoder)
  }


  def createAgeGenderView() = {

    val modelEncoder = Encoders.product[AgeGenderView]

    surveyDataFrame.createOrReplaceTempView("result")
    spark.sql("select Gender, avg(Age1stCode) as avg from result group by Gender order by avg desc")
    .as[AgeGenderView](modelEncoder)
  }


  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }

}
