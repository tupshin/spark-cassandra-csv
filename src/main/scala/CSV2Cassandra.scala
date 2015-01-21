import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv.CsvContext
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.toRDDFunctions

object CSV2Cassandra {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("java -jar cvscass.jar") {
      head("CassCSVLoader", "0.1")

      opt[File]('f', "file") required () valueName ("<file>") action { 
         (x, c) => c.copy(file = x)
      } text ("the filename/path of the csv file to load")
      
      opt[String]('k', "keyspace") required() action { 
        (x, c) => c.copy(keyspace = x)
       } text ("the keyspace to write to")

       opt[String]('t', "table") required() action { 
        (x, c) => c.copy(table = x)
       } text ("the table to write to. it must already exist and have the columns specified in the 'cols' argument")

      opt[Seq[String]]('c', "cols") valueName ("col1,col2,...") action 
      { (x, c) => c.copy(cols = x)
      } text ("other arguments")

      opt[Unit]("verbose") action { (_, c) =>
        c.copy(verbose = true)
      } text ("verbose is a flag")

      opt[Unit]("debug") hidden () action { (_, c) =>
        c.copy(debug = true)
      } text ("this option is hidden in the usage text")

      note("some notes.\n")

      help("help") text ("prints this usage text")
      

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
      // do stuff

      case None =>
      // arguments are bad, error message will have been displayed
    }

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")
    val sc = new SparkContext("local", "test", conf)
    val sqlContext = new SQLContext(sc)
    val cars = sqlContext.csvFile("cars.csv")
    val connector = CassandraConnector(conf)
    val cols = SomeColumns("make", "model", "year", "comment")
    cars.saveToCassandra("spark_loader", "cars", cols)
  }
}
case class Config(
  file: File = new File("."),
  keyspace: String = "",
  table: String = "",
  verbose: Boolean = false,
  debug: Boolean = false,
  cols: Seq[String] = Seq()
)

