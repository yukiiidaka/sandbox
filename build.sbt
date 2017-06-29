organization := "org.mvrck"

name := "sandbox"

version := "0.1.0-SNAPSHOT"

lazy val sandbox = project in file(".")

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "2.12.0",
//  "commons-daemon" % "commons-daemon" % "1.0.15",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12" % "test",
  "org.apache.spark" %% "spark-mllib" % "2.1.0"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in run := true
javaOptions ++= Seq(
  "-Xms16G",
  "-Xmx16G",
  "-XX:NewSize=8G",
  "-XX:MaxNewSize=8G",
  "-XX:PermSize=8G",
  "-XX:MaxPermSize=8G"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfuture",
  "-Xlint",
  "-Yinline-warnings",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-unused-import",
  "-Ywarn-value-discard"
)
scalacOptions in (Compile, console) ~= {_.filterNot(_ == "-Ywarn-unused-import")}
scalacOptions in (Test, console) <<= (scalacOptions in (Compile, console))

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.size > 1 && ps(1) == "joda" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
