name := "spark-top-ten-genres"

version := "1.0"

scalaVersion := "2.10.6"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

val spark_version = "1.6.0"
val spark_csv_version = "1.5.0"
val spark_testing_base_version = "1.6.0_0.7.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.apache.spark" %% "spark-hive" % spark_version,
  "com.databricks" %% "spark-csv" % spark_csv_version,
  "com.holdenkarau" %% "spark-testing-base" % spark_testing_base_version % "test"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M",
                    "-Xmx2048M",
                    "-XX:MaxPermSize=2048M",
                    "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

def latestScalafmt = "1.3.0"
commands += Command.args("scalafmt", "Run scalafmt cli.") {
  case (state, args) =>
    val Right(scalafmt) =
      org.scalafmt.bootstrap.ScalafmtBootstrap.fromVersion(latestScalafmt)
    scalafmt.main("--non-interactive" +: args.toArray)
    state
}
