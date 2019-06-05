import org.apache.spark.sql.{Dataset, SparkSession}
import views.DeveloperOpenSourcePercentageView

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.es.nodes", "elasticsearch")
      .config("spark.es.port", "9200")
      .config("spark.es.index.auto.create", "true")
      .getOrCreate()

    val surveyResultPath: String = args(0);

    val surveyDataFrame = spark.read.option("header", "true").csv(surveyResultPath)
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame);

    val developerOpenSourcePercentageView : Dataset[DeveloperOpenSourcePercentageView] =
      surveyProcessing.createDeveloperOpenSourcePercentageView()

    ElasticViewWriter
      .writeView[DeveloperOpenSourcePercentageView](
      developerOpenSourcePercentageView, "DeveloperOpenSourcePercentageView"
    )

    spark.stop();
  }


}
