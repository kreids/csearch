name := "PageRank"

version := "1.0"

scalaVersion := "2.11.4"


//libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.9.40"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.2"


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("org", "jets3t", xs @ _*) => MergeStrategy.last
    case PathList("com", "fasterxml", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x => old(x)
  }
}

packageOptions in assembly ~= { pos =>
  pos.filterNot { po =>
      po.isInstanceOf[Package.MainClass]
  }
}
