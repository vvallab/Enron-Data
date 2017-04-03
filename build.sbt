name := "beef-app"

version := "1.3"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "com.databricks" % "spark-xml_2.10" % "0.4.1"

resolvers += "OSS Sonatype" at "https://repo1.maven.org/maven2/"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

    
