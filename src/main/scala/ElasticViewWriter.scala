import org.apache.spark.sql.{DataFrame, Dataset}
import org.elasticsearch.spark.sql._

object ElasticViewWriter {
  def writeView(view: DataFrame, viewName : String): Unit = {
    view.saveToEs("rtda/" + viewName)
  }
}
