/**
 * Created by jhaddad on 9/18/14.


 This is my first attempt at doing anything with spark
 We're going to create a table, put some data into it, and aggregate the values
 We can then save the results to cassandra
 We'll build this out more to get more complex jobs
 This is only meant to be used in dev, since it's hard coded to "local" (no master)

  CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

  CREATE TABLE tutorial.demo (
      id int PRIMARY KEY,
      v int
  );

  insert into demo (id, v) values (1, 2);
  insert into demo (id, v) VALUES ( 3, 384);
  insert into demo (id, v) VALUES ( 4, 4);

  create table stats ( k timeuuid primary key, total int );


  After you run the job, select from the table:

 cqlsh:tutorial> select dateOf(k), total from stats ;

 dateOf(k)                | total
--------------------------+-------
 2014-09-18 13:22:37-0700 |   390

 */
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
