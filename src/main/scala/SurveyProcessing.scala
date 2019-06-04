import views.DeveloperOpenSourcePercentageView
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame) {
  def createDeveloperOpenSourcePercentage() = {
    val modelEncoder = Encoders.product[DeveloperOpenSourcePercentageView]
    surveyDataFrame.groupBy("OpenSourcer")
      .count()
      .withColumn("percentage", (col("count")/ sum("count").over())*100)
  }



  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }

}
