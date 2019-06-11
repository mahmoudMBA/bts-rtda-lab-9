import org.apache.spark.sql.{Dataset, SparkSession}
import views.{AgeGenderView, DeveloperOpenSourcePercentageView}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.es.nodes", "elasticsearch")
      .config("spark.es.port", "9200")
      .config("spark.es.index.auto.create", "true")
      .getOrCreate()

    val surveyInputPath: String = args(0);

    val surveyDataFrame = spark.read.option("header", "true").csv(surveyInputPath)
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val developerOpenSourcePercentageView : Dataset[DeveloperOpenSourcePercentageView] =
      surveyProcessing.createDeveloperOpenSourcePercentageView()

    val ageGenderView: Dataset[AgeGenderView] = surveyProcessing.createAgeGenderView()

    val percentageDevStudentsView = surveyProcessing createPercentageDevStudentsView()

    val percentageByEthnicityView = surveyProcessing.createPercentageByEthnicityView()

    val percentageSocialMediaView  = surveyProcessing.createPercentageSocialMediaView()


    val ageCountryView  = surveyProcessing.createAvgAgeByCountry()
    val percentageByLanguageView  = surveyProcessing.createPercentageLanguageView()
    val percentageByPlatform  = surveyProcessing.createPercentagePlatformView()

    ElasticViewWriter.writeView(developerOpenSourcePercentageView, "DeveloperOpenSourcePercentageView")
    ElasticViewWriter.writeView(ageGenderView, "developerOpenSourcePercentageView")
    ElasticViewWriter.writeView(percentageDevStudentsView, "percentageDevStudentsView")
    ElasticViewWriter.writeView(percentageByEthnicityView, "percentageByEthnicityView")
    ElasticViewWriter.writeView(percentageSocialMediaView, "percentageSocialMediaView")

    ElasticViewWriter.writeView(ageCountryView, "ageCountryView")
    ElasticViewWriter.writeView(percentageByLanguageView, "percentageByLanguageView")
    ElasticViewWriter.writeView(percentageByPlatform, "percentageByPlatform")

    spark.stop();
  }


}
