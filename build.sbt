
scalaVersion := "2.11.9"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// https://mvnrepository.com/artifact/com.amazon.deequ/deequ
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.2"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test


libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "." + artifact.extension
}
crossTarget := baseDirectory.value / "extra/"