import views.{AgeGenderView, AvgProfessionalCodingExperienceView, DeveloperOpenSourcePercentageView, PercentageByEthnicityView, PercentageDevStudentsView, PercentageSocialMediaView}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame, spark: SparkSession) {

  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }

  def createDeveloperOpenSourcePercentageView(): Dataset[DeveloperOpenSourcePercentageView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("OpenSourcer")
      .count()
      .withColumn("percentage", (col("count")/
        sum("count").over())*100)
      .as[DeveloperOpenSourcePercentageView]
  }


  def createAgeGenderView(): Dataset[AgeGenderView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("Gender", explode(split($"Gender", ";")))
      .groupBy("Gender").agg(avg("Age1stCode").as("avg"))
      .orderBy(desc("avg"))
      .as[AgeGenderView]
  }

  def createPercentageDevStudentsView(): Dataset[PercentageDevStudentsView] = ???

  def createAvgProfessionalCodingExperienceView(): Dataset[AvgProfessionalCodingExperienceView] = ???

  def createPercentageByEthnicityView(): Dataset[PercentageByEthnicityView] = ???

  def createPercentageSocialMediaView(): Dataset[PercentageSocialMediaView] = ???
}
