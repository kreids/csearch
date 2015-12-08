
import java.io.{File, PrintWriter}
import java.util.Calendar

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.spec.{BatchWriteItemSpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanResult}
import org.apache.log4j.{Level, BasicConfigurator, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.util.hashing.MurmurHash3



object MyPageRank {

  def hashToID(str: String) : Long = {
    return MurmurHash3.stringHash(str).toLong
  }

  def parallelScan(sc: SparkContext, accessKey: String, secretKey: String, tableName: String): RDD[(String, String)] = {
    val tableInfo = TableInfo(accessKey, secretKey, Regions.US_WEST_2, tableName)
    val itemCount = getTable(tableInfo).describe.getItemCount
    println(itemCount)
    val totalSegments = 5
    val scanInputs = 0 until totalSegments map {segment => (itemCount, totalSegments, segment, tableInfo)}

    sc.parallelize(scanInputs)
      .flatMap{tup => {
        val scanResult : ItemCollection[ScanOutcome] = getTable(tup._4).scan(new ScanSpec().withMaxResultSize(tup._1.toInt)
          .withTotalSegments(tup._2)
          .withSegment(tup._3)
          .withAttributesToGet("url", "links"))

        val iterator : java.util.Iterator[Item] = scanResult.iterator()
        iterator.flatMap { x => {
          val name = x.getString("url")
          x.getStringSet("links").map { s =>
            (name, s)
          }
        }}.toList
      }}
  }

  case class TableInfo(accessKey: String, secretKey: String, region: Regions, tableName: String)


  def getDB(accessKey: String, secretKey: String, regions: Regions) : DynamoDB = {
    val client = new AmazonDynamoDBClient(new BasicAWSCredentials(accessKey, secretKey))
    client.setRegion(Region.getRegion(regions))
    new DynamoDB(client)
  }

  def getTable(tableInfo: TableInfo) : Table = {
    getDB(tableInfo.accessKey, tableInfo.secretKey, tableInfo.region).getTable(tableInfo.tableName)
  }


  def main(args: Array[String]) {

    Logger.getRootLogger().setLevel(Level.INFO)
    BasicConfigurator.configure()

    val conf = new SparkConf().setAppName("DynamoDB PageRank")
    val sc = new SparkContext(conf)
    val edges = parallelScan(sc, "aws_access_key", "aws_secret_key", "455crawler-graph")

    val vertices = edges.map{ tup => tup._1 }.distinct()
      .map { x => (hashToID(x), x)}
    val edgeIDs = edges.map{ tup => (hashToID(tup._1), hashToID(tup._2))}
    val edgeRdd = edgeIDs.map(tup => (tup._2, tup._1)).join(vertices).map{tup => Edge[Int](tup._2._1, tup._1, 1)}

    val theGraph = Graph[String, Int](vertices, edgeRdd, "missing")

    val results = theGraph.pageRank(0.0001).vertices.join(vertices).map{x =>
      (x._2._2, x._2._1)}.collect()

    val dynamoDB = getDB("aws_access_key", "aws_secret_key", Regions.US_WEST_2)

    val items = results.map {tup => new Item().withPrimaryKey("url", tup._1).withDouble("rank", tup._2)}

    try {
      items.sliding(25).foreach { batch =>
        val writeItems = new TableWriteItems("pagerank-results").withItemsToPut(
          results.map { tup => new Item().withPrimaryKey("url", tup._1).withDouble("rank", tup._2) }.toList
        )

        dynamoDB.batchWriteItem(writeItems)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }


}

