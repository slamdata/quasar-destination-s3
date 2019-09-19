import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-s3"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-s3"),
  "scm:git@github.com:slamdata/quasar-destination-s3.git"))

val ArgonautVersion = "6.2.3"
val AwsSdkVersion = "2.9.1"
val AwsV1SdkVersion = "1.11.634"
val Fs2Version = "1.0.5"
val MonixVersion = "3.0.0"
val SpecsVersion = "4.7.0"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true)

lazy val QuasarVersion = IO.read(file("./quasar-version")).trim

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-s3")
  .settings(
    performMavenCentralSync := false,
    quasarPluginName := "s3-dest",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.s3.S3DestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "io.argonaut"  %% "argonaut" % ArgonautVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "co.fs2" %% "fs2-reactive-streams" % Fs2Version,
      "io.monix" %% "monix-catnap" % MonixVersion,
      "software.amazon.awssdk" % "netty-nio-client" % AwsSdkVersion,
      // We depend on both v1 and v2 S3 SDKs because of this ticket:
      // https://github.com/aws/aws-sdk-java-v2/issues/272
      "software.amazon.awssdk" % "s3" % AwsSdkVersion,
      "com.amazonaws" % "aws-java-sdk-s3" % AwsV1SdkVersion),
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % SpecsVersion % Test),
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
