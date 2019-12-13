import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class Spell(name:String)
case class Line(creatureName : String, spells : Array[Spell])


object MainClass extends App {

  val conf = new SparkConf()
    .setAppName("Devoir2")
    .setMaster("local[*]");

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR");

  val spark = SparkSession
    .builder()
    .appName("Spark Scala Json")
    .config("spark.some.config.option", "some-value")
    .getOrCreate();

  import spark.implicits._

  println("Hello World")

  val jsonPath = "C:\\Users\\MR\\IdeaProjects\\Devoir2\\src\\main\\scala\\Spells_TestBig.json";
  val dataFrame = spark.read.option("multiline", true).json(jsonPath);

  val data: RDD[(String, String)] = dataFrame.flatMap(row => {
    val spellList = row.getAs[Seq[String]]("spells")
    spellList.map(sp => Tuple2(sp.toString(), row.getAs[String]("nameCreature")))
}).rdd.reduceByKey(_+','+_)

  val r: RDD[(String, Array[String])] = data.map(elem => Tuple2(elem._1, elem._2.split(",")))

  r.foreach(x=>{
    print(x._1 + " : [")
    x._2.foreach(x => print(x + ", "))
    println("]")
  })

}

