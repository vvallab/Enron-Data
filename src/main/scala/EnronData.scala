import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.xml
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs._
import java.io._;
case class FileLength(length:Double, sNo:Int)
object EnronData {

  def main(args: Array[String]) {
if (args(0) !=null && args(1) !=null && args(2) !=null)
{
val sparkConf = new SparkConf().setAppName("enron");
val sc = new SparkContext(sparkConf);
//val rootLogger = Logger.getRootLogger()
//rootLogger.setLevel(Level.ERROR)
val source_Path = args(0);
val inter_Path = args(1)
val destination_Path = args(2);

val hadoopConf = sc.hadoopConfiguration;
val sqlContext = new SQLContext(sc);
import sqlContext.implicits._;
def hasPath(path: String) = Try(sqlContext.read.json(path)).isSuccess;
def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess;
val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
val format1 = new java.text.SimpleDateFormat("dd/MM/yyyy");

val temp_Folder = "tmp"+destination_Path;
val temp_file_Folder = "tmpFile"+destination_Path;
val full_inter_path = inter_Path+"/"+"*"

//Retrive the list of xml zip files
val zip_List = new File(source_Path).listFiles.map(_.getName).filter(_.endsWith("xml.zip"))
zip_List.foreach{t => 
Unzip.unZip(t,inter_Path);


val mail_DF = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "Documents").load(full_inter_path);

val mail_DF_Documents = mail_DF.select(explode($"Document").as("docs"))

val mail_DF_Tags = mail_DF_Documents.select(explode($"docs.Tags.Tag").as("tags"))

val mail_DF_To_List = mail_DF_Tags.filter($"tags.@TagName".contains("To"))

val mail_DF_Cc_List = mail_DF_Tags.filter($"tags.@TagName".contains("CC"))

val mail_DF_To_List_Ref = mail_DF_To_List.select(($"tags.@TagName").as("To"),($"tags.@TagValue").as("mailAddress"))

val mail_DF_Cc_List_Ref = mail_DF_Cc_List.select(($"tags.@TagName").as("CC"),($"tags.@TagValue").as("mailAddress"))

val mail_DF_To_Values = mail_DF_To_List_Ref.select($"mailAddress").withColumn("instances",lit(1))

val mail_DF_Cc_Values = mail_DF_Cc_List_Ref.select($"mailAddress").withColumn("instances",lit(0.5))

val mail_DF_Combined = mail_DF_To_Values.unionAll(mail_DF_Cc_Values);

val mail_DF_Aggregate = mail_DF_Combined.groupBy($"mailAddress").agg(sum($"instances").alias("instances"));

/**
A reasonable approximation made with the assumption that the final top 100 mail recipients will not differ from top 100 recipients derived at individual level 
*/

val mail_DF_Aggr_Top_100 = mail_DF_Aggregate.orderBy(desc("instances")).limit(100);

//Results from individual zip folder are written and appended to json. 

mail_DF_Aggr_Top_100.write.mode("append").json(temp_Folder);


//Start of the Mail body length solution

val mail_DF_Docs = mail_DF.select(explode($"Document").as("docs"));

val mail_DF_Files = mail_DF_Docs.select(explode($"docs.Files.File").as("files"));

val mail_DF_Files_Filtered = mail_DF_Files.filter($"files.@FileType".contains("Text"))

val mail_DF_Files_Refined = mail_DF_Files_Filtered.select(($"files.ExternalFile.@FileName").as("fileName"),($"files.ExternalFile.@FilePath").as("filePath"))

val mail_DF_Path = mail_DF_Files_Refined.select(concat($"filePath",lit("/"),$"fileName").as("fullFileName"));


val mail_DF_Paths = mail_DF_Path.select($"fullFileName").map(_.toString()).collect

val total_Files = mail_DF_Paths.length;
var file_Length = 0
mail_DF_Paths.foreach{a => val ss = a.dropRight(1);val sx = ss.slice(1,ss.length); val str1 = inter_Path+s"$sx"; val sy = sc.textFile(str1); val splitline:Int = sy.flatMap(_.split(" ")).count().toInt; file_Length = file_Length+ splitline;} 

val average_File_Length = file_Length/total_Files;

val fileLength_List = List(average_File_Length)
val inter_RDD = sqlContext.sparkContext.parallelize(fileLength_List);

val inter_DF = inter_RDD.map{case(a)=>FileLength(a.toDouble,1)}.toDF();

inter_DF.write.mode("append").json(temp_file_Folder);

}

//All the individual jsons are loaded and top 100 are filtered out

val tmpPath = temp_Folder+"/"+"*"

val Aggregate_Mail_List = sqlContext.read.json(tmpPath)

val Aggregate_DF = Aggregate_Mail_List.groupBy($"mailAddress").agg(sum($"instances").alias("instances"));

val Agg_Top_100_Mail_List = Aggregate_DF.orderBy(desc("instances")).limit(100);

//Final Top 100 List is written into a Json
Agg_Top_100_Mail_List.write.mode("append").json(destination_Path);

//All the individual File sizes are loaded (for each zip)

val tmpFilePath = temp_file_Folder+"/"+"*";

val Aggregate_File_Size = sqlContext.read.json(tmpFilePath);

val Agr_Size_DF = Aggregate_File_Size.select(avg($"length"));

Agr_Size_DF.show;

Agr_Size_DF.write.mode("append").json(destination_Path);
}

}
}


