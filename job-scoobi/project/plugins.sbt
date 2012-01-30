
resolvers ++= Seq(
    Classpaths.typesafeResolver,
    "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
)

// Eclipse - use command 'eclipse' in sbt to create Eclipse project files.
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.5.0")

// IDEA - use command 'gen-idea' in sbt to create idea project files.
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "0.11.0")
