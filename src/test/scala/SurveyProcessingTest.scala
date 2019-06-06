import com.holdenkarau.spark.testing.DatasetSuiteBase
import views.DeveloperOpenSourcePercentageView
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite


class SurveyProcessingTest extends FunSuite with DatasetSuiteBase {
  def readFromTestFile(): DataFrame ={
    val surveyDataFrame = spark.read.option("header", "true").csv("data/survey_results_public.csv")
    surveyDataFrame
  }

  test("Test functionality: how many programmers participated in the survey?"){
    val surveyDataFrame = readFromTestFile();
    val  expectedDeveloperCount: Long =  88883;
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);
    val realDeveloperCount: Long = surveyProcessing.developerCount();
    assert(realDeveloperCount == expectedDeveloperCount)
  }

  test("Test functionality: Percentage of developers how do open source") {
    import spark.implicits._

    val expectedOpenSourcePercentageSeq = Seq(
      DeveloperOpenSourcePercentageView(
        "Less than once a month but more than once per year", 20561, 23.13265753856193
      ),
      DeveloperOpenSourcePercentageView(
        "Once a month or more often", 11055, 12.437698997558588
      ),
      DeveloperOpenSourcePercentageView(
        "Never", 32295, 36.33428214619219
      ),
      DeveloperOpenSourcePercentageView(
        "Less than once per year", 24972, 28.095361317687296
      )
    )

    val expectedOpenSourcePercentageView: Dataset[DeveloperOpenSourcePercentageView] =
      sc.parallelize(expectedOpenSourcePercentageSeq).toDS()

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);
    val realDevelopOpenSourcePercentageView: Dataset[DeveloperOpenSourcePercentageView] =
      surveyProcessing.createDeveloperOpenSourcePercentageView()

     assert(datasetsAreEquals(expectedOpenSourcePercentageView, realDevelopOpenSourcePercentageView))
  }


  def datasetsAreEquals[T](d1: Dataset[T], d2: Dataset[T]): Boolean ={
    val d1_prime = d1.groupBy().count()
    val d2_prime = d2.groupBy().count()
    d1_prime.intersect(d2_prime).count() == d2_prime.intersect(d1_prime).count()
  }
}
