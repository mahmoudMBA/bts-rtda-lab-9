import org.apache.spark.sql.{DataFrame, Dataset}
import org.elasticsearch.spark.sql._

object ElasticViewWriter {
  def writeView[T](view: Dataset[T], viewName : String): Unit = {
    view.toDF().saveToEs("rtda" + viewName.toLowerCase + "/" + viewName.toLowerCase)
  }
}
