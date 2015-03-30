import AssemblyKeys._

name := "spark-examples"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.2.1"
)

resolvers += Resolver.sonatypeRepo("public")

assemblySettings

jarName in assembly := s"spark-examples-assembly-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)