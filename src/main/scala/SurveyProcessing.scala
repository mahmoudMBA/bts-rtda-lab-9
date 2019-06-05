import views.DeveloperOpenSourcePercentageView
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame) {
  def createDeveloperOpenSourcePercentageView() = {
    val modelEncoder = Encoders.product[DeveloperOpenSourcePercentageView]
    surveyDataFrame.groupBy("OpenSourcer")
      .count()
      .withColumn("percentage", (col("count")/ sum("count").over())*100)
      .as[DeveloperOpenSourcePercentageView](modelEncoder)
  }



  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }

}
