lazy val akkaHttpVersion = "10.2.3"
lazy val akkaVersion    = "2.6.12"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization    := "com.example",
      scalaVersion    := "2.12.12"
    )),
    name := "akka-htt-quick",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"                % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json"     % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
      "com.typesafe.akka" %% "akka-stream"              % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"               % akkaVersion,
      "ch.qos.logback"    % "logback-classic"           % "1.2.3",
      "org.apache.httpcomponents" % "httpcore"          % "4.4.9",
      "org.apache.httpcomponents" % "httpclient"        % "4.3.3",

      "org.apache.hadoop" % "hadoop-client"             % "3.0.3",
      "org.apache.hadoop" % "hadoop-common" % "3.0.3",
      "org.apache.hadoop" % "hadoop-hdfs"   % "3.0.3",

      "com.typesafe.akka" %% "akka-http-testkit"        % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion     % Test,
      "org.scalatest"     %% "scalatest"                % "3.1.4"         % Test
    )
  )

