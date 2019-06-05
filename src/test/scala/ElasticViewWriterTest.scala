import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.scalatest.FunSuite
import org.elasticsearch.spark.sql._
import views.DeveloperOpenSourcePercentageView

class ElasticViewWriterTest extends FunSuite {
 val spark = SparkSession.builder()
   .master("local")
   .config("spark.es.nodes", "elasticsearch")
   .config("spark.es.port", "9200")
   .config("spark.es.index.auto.create", "true")
   .getOrCreate();

  def readFromTestFile(): DataFrame ={
    val surveyDataFrame = spark.read.option("header", "true").csv("data/survey_results_public.csv")
    surveyDataFrame
  }

  test("Write DeveloperOpenSourcePercentageView view to ES") {

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val developOpenSourcePercentage : Dataset[DeveloperOpenSourcePercentageView] =
      surveyProcessing.createDeveloperOpenSourcePercentageView()

    ElasticViewWriter.writeView[DeveloperOpenSourcePercentageView](developOpenSourcePercentage, "DeveloperOpenSourcePercentageView")

    val modelEncoder = Encoders.product[DeveloperOpenSourcePercentageView]

    val retrievedView = spark.esDF("rtda/DeveloperOpenSourcePercentageView").as[DeveloperOpenSourcePercentageView](modelEncoder)

    assert(retrievedView.count() == developOpenSourcePercentage.count())
  }

}
