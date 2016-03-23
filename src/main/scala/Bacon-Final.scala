/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import java.io._

object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	
	def main(args: Array[String]) 
	{
		val cores = args(0)
		val inputFile = args(1)
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		val sc = new SparkContext(conf)
		
		println("Number of cores: " + args(0))
		println("Input file: " + inputFile)
		
		var t0 = System.currentTimeMillis

		// Main code here ////////////////////////////////////////////////////
		// ...
		//////////////////////////////////////////////////////////////////////
		val conf1 = new Configuration(sc.hadoopConfiguration)
		conf1.set("textinputformat.record.delimiter", "\n\n")
		val input = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf1)

		val actorAndData = input.map(text=>text._2.toString).map(text=>text.split("\t")).map(text=>(text(0),text.drop(1).mkString.split("\n"))).zipWithIndex()

		val index = actorAndData.map(text=>(text._2,text._1._1))

		val KevinBaconID = index.map(a=>(a._2,a._1)).lookup(KevinBacon)(0)

		val actorAndMovies = actorAndData.map(text=>(text._2,text._1._2))/*.map(a=>(a._1,a._2.map(text=>if(text.contains("(as")) text.substring(0,text.indexOf("(as")) else text).map(text=>if(text.contains('[')) text.substring(0,text.indexOf('[')) else text).map(text=>if(text.contains('<')) text.substring(0,text.indexOf('<')) else text).map(text=>text.split(' ').filter(_!="").mkString(" "))))*/
		
		val actorAndMovie = actorAndMovies.flatMap(data=>data._2.map(movie=>(data._1,movie)))

		val movieAndactors = actorAndMovie.map(a=>(a._2,Array(a._1))).reduceByKey(_++_).map(actors=>actors._2)

		val actorAndCollaboratingActors = movieAndactors.flatMap(actors=>actors.map(actor=>(actor,actors.diff(Array(actor))))).reduceByKey(_++_).map(actors=>(actors._1,actors._2.distinct))

		val actorWithDistanceUpto0 = actorAndCollaboratingActors.keys.filter(_==KevinBaconID)

		val actorWithDistance1 = actorAndCollaboratingActors.filter(actors=>actors._1==KevinBaconID).flatMap(actors=>actors._2)

		val actorWithDistanceUpto1 = actorWithDistanceUpto0++actorWithDistance1

		val actorWithDistance2 = actorWithDistance1.map(a=>(a,1)).join(actorAndCollaboratingActors).filter(a=>a._2._1==1).flatMap(actors=>actors._2._2).distinct.subtract(actorWithDistanceUpto1)

		val actorWithDistanceUpto2 = actorWithDistanceUpto1++actorWithDistance2

		val actorWithDistance3 = actorWithDistance2.map(a=>(a,1)).join(actorAndCollaboratingActors).filter(a=>a._2._1==1).flatMap(actors=>actors._2._2).distinct.subtract(actorWithDistanceUpto2)

		val actorWithDistanceUpto3 = actorWithDistanceUpto2++actorWithDistance3

		val actorWithDistance4 = actorWithDistance3.map(a=>(a,1)).join(actorAndCollaboratingActors).filter(a=>a._2._1==1).flatMap(actors=>actors._2._2).distinct.subtract(actorWithDistanceUpto3)

		val actorWithDistanceUpto4 = actorWithDistanceUpto3++actorWithDistance4

		val actorWithDistance5 = actorWithDistance4.map(a=>(a,1)).join(actorAndCollaboratingActors).filter(a=>a._2._1==1).flatMap(actors=>actors._2._2).distinct.subtract(actorWithDistanceUpto4)

		val actorWithDistanceUpto5 = actorWithDistanceUpto4++actorWithDistance5

		val actorWithDistance6 = actorWithDistance5.map(a=>(a,1)).join(actorAndCollaboratingActors).filter(a=>a._2._1==1).flatMap(actors=>actors._2._2).distinct.subtract(actorWithDistanceUpto5)

		// Write to "actors.txt", the following:
		//	Total number of actors
		//	Total number of movies (and TV shows etc)
		//	Total number of actors at distances 1 to 6 (Each distance information on new line)
		//	The name of actors at distance 6 sorted alphabetically (ascending order), with each actor's name on new line
		val file = new PrintWriter("actors.txt", "UTF-8")
		file.print  ("Total number of actors is: ")
		file.println(actorAndCollaboratingActors.count())
		file.print  ("Total number of movies is: ")
		file.println(movieAndactors.count())
		file.print  ("\n")
		file.print  ("Total number of actors at distance 1 is: ")
		file.println(actorWithDistance1.count)
		file.print  ("Total number of actors at distance 2 is: ")
		file.println(actorWithDistance2.count)
		file.print  ("Total number of actors at distance 3 is: ")
		file.println(actorWithDistance3.count)
		file.print  ("Total number of actors at distance 4 is: ")
		file.println(actorWithDistance4.count)
		file.print  ("Total number of actors at distance 5 is: ")
		file.println(actorWithDistance5.count)
		file.print  ("Total number of actors at distance 6 is: ")
		file.println(actorWithDistance6.count)
		file.print  ("\n")
		file.println("The name of all actors at distance 6: ")
		file.print  (actorWithDistance6.map(a=>(a,1)).join(index).filter(a=>a._2._1==1).map(a=>(a._2._2,a._2._1)).sortByKey(numPartitions = 1).keys.reduce(_+"\n"+_))
		file.close();

		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	} 
}

