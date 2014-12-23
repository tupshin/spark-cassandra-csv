import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._
import com.datastax.spark.connector._

object CSV2Cassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")
    val sc = new SparkContext("local", "test", conf)
    val sqlContext = new SQLContext(sc)
    val cars = sqlContext.csvFile("cars.csv")
    val connector = CassandraConnector(conf)
    cars.saveToCassandra("spark_loader", "cars", SomeColumns("make", "model", "year", "comment"))
  }
}
