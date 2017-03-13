import AssemblyKeys._

//javaOptions ++= Seq("-server", "-Xms1536M", "-Xmx1536M", "-XX:+CMSClassUnloadingEnabled")
//javacOptions ++= Seq("-server", "-Xms1536M", "-Xmx1536M", "-XX:+CMSClassUnloadingEnabled")

javacOptions ++= Seq("-encoding", "UTF-8")

lazy val root = (project in file(".")).
  settings(
    name := "my-spark_sentiment",
    version := "1.0",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("ch.uzh.sentiment.Sentiment"),
    mergeStrategy in assembly := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case _ => MergeStrategy.first
    }
  )

val configVersion = "1.3.0"
val sparkVersion = "2.1.0"
val hadoopVersion = "2.7.3"
val protobufVersion = "2.6.1"
val coreNlpVersion = "3.7.0"
val scoptVersion = "3.5.0"
val sparkAvroVersion = "1.5.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % configVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion  % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion classifier "models",
  "com.github.scopt" %% "scopt" % scoptVersion,
  "com.databricks" %% "spark-csv" % sparkAvroVersion
)

lazy val defaultSettings = Defaults.coreDefaultSettings ++ Seq(
  resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)

