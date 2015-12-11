import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, BasicConfigurator, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.util.hashing.MurmurHash3



object MyPageRank {

  val log = Logger.getLogger(MyPageRank.getClass)

  def hashToID(str: String) : Long = {
    return MurmurHash3.stringHash(str).toLong
  }

  def prepareGraphData(sc: SparkContext) = {
    val edges = sc.textFile("s3://cis455crawlerreal/links_db").flatMap { x => {
      val arr = x.split(",")
      if (arr.length == 2) Some((arr(0), arr(1))) else None
    }}
    val vertices = edges.map{ tup => tup._1 }.distinct(30)
      .map { x => (hashToID(x), x)}
    val edgeIDs = edges.map{ tup => (hashToID(tup._2), hashToID(tup._1))}
    val edgeRdd = edgeIDs.join(vertices, 100).map{tup => tup._2._1 + "," + tup._1}
    vertices.map{x => x._1.toString + "," + x._2}.saveAsTextFile("s3://cis455crawlerreal/vertex_db")
    edgeRdd.saveAsTextFile("s3://cis455crawlerreal/edge_db")
  }

  def doPageRank(sc: SparkContext) = {
    val vertices = sc.textFile("s3://cis455crawlerreal/vertex_db").map {
      line => {
        val arr = line.split(",")
        (arr(0).toLong, arr(1))
      }}.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val edges = sc.textFile("s3://cis455crawlerreal/edge_db")
      .map{ line => {
        val arr = line.split(",")
        Edge[Int](arr(0).toLong, arr(1).toLong, 1)
      }}.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val theGraph = Graph[String, Int](vertices, edges, "missing")
    theGraph.staticPageRank(1000).vertices
      .join(vertices, 30)
      .map{x => x._2._2 + "," + x._2._1}
      .saveAsTextFile("s3a://cis455crawlerreal/pagerank-1000-out")
  }


  def main(args: Array[String]) {

    Logger.getRootLogger().setLevel(Level.INFO)
    BasicConfigurator.configure()

    //val (accessKey, secretKey) = getCreds
    val conf = new SparkConf().setAppName("DynamoDB PageRank")
      .set("spark.rdd.compress", "true")
      .set("spark.executor.extraJavaOptions", "XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIAJAORMEQ7SPZCHL7Q")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    doPageRank(sc)



  }


}

