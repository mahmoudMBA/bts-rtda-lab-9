import views.{AgeGenderView, AvgProfessionalCodingExperienceView, DeveloperOpenSourcePercentageView, PercentageByEthnicityView, PercentageDevStudentsView, PercentageSocialMediaView}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame, spark: SparkSession) {


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

  def createPercentageDevStudentsView(): Dataset[PercentageDevStudentsView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("Student")
      .count()
      .withColumn("percentage", (col("count")/ sum("count").over())*100)
      .orderBy(desc("count")).as[PercentageDevStudentsView]
  }

  def createAvgProfessionalCodingExperienceView(): Dataset[AvgProfessionalCodingExperienceView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("DevType", explode(split($"DevType", ";")))
      .groupBy("DevType")
      .agg(avg("YearsCodePro").as("avg")).orderBy(desc("avg")).as[AvgProfessionalCodingExperienceView]
  }

  def createPercentageByEthnicityView(): Dataset[PercentageByEthnicityView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("Ethnicity", explode(split($"Ethnicity", ";")))
      .groupBy("Ethnicity").count()
      .withColumn("percentage", (col("count")/ sum("count").over())*100)
      .orderBy(desc("percentage")).as[PercentageByEthnicityView]
  }

  def createPercentageSocialMediaView(): Dataset[PercentageSocialMediaView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("SocialMedia", explode(split($"SocialMedia", ";")))
      .groupBy("SocialMedia").count()
      .withColumn("percentage", (col("count")/ sum("count").over())*100)
      .orderBy(desc("percentage")).as[PercentageSocialMediaView]
  }

  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }
}
