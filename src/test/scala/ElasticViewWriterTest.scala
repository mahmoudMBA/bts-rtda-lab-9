import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.scalatest.FunSuite
import org.elasticsearch.spark.sql._
import views.DeveloperOpenSourcePercentageView

class ElasticViewWriterTest extends FunSuite with DatasetSuiteBase{

  def readFromTestFile(): DataFrame ={
    val spark_ = SparkSession.builder()
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .config("spark.es.index.auto.create", "true")
      .getOrCreate()
    val surveyDataFrame = spark_.read.option("header", "true").csv("data/survey_results_public.csv")
    surveyDataFrame
  }

  test("Write DeveloperOpenSourcePercentageView view to ES") {

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame);

    val developOpenSourcePercentage : DataFrame = surveyProcessing.createDeveloperOpenSourcePercentage()

    ElasticViewWriter.writeView(developOpenSourcePercentage, "DeveloperOpenSourcePercentageView")

    val retrivedView = spark.esDF("rtda/DeveloperOpenSourcePercentageView")

    assertDataFrameApproximateEquals(developOpenSourcePercentage, retrivedView, 0.0001)
  }

}
