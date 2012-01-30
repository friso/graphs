
name := "job-scoobi"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq("snapshots" at "http://scala-tools.org/repo-snapshots",
                  "releases"  at "http://scala-tools.org/repo-releases")

libraryDependencies ++= Seq(
    "com.nicta" %% "scoobi" % "0.2.0" % "provided"
)
