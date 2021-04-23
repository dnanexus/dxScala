import Merging.customMergeStrategy
import sbt.Keys.{maxErrors, _}
import sbtassembly.AssemblyPlugin.autoImport.{assemblyMergeStrategy, _}
import com.typesafe.config._
import sbt.ThisBuild

name := "dxScala"

// build-wide settings
ThisBuild / organization := "com.dnanexus"
ThisBuild / scalaVersion := "2.13.2"
ThisBuild / developers := List(
    Developer("jdidion", "jdidion", "jdidion@dnanexus.com", url("https://github.com/dnanexus-rnd"))
)
ThisBuild / homepage := Some(url("https://github.com/dnanexus/dxScala"))
ThisBuild / scmInfo := Some(
    ScmInfo(url("https://github.com/dnanexus/dxScala"), "git@github.com:dnanexus/dxScala.git")
)
ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// PROJECTS

lazy val root = project.in(file("."))
lazy val global = root
  .settings(settings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
      common,
      api,
      protocols
  )

def getVersion(subproject: String, subprojectName: String): String = {
  val confPath = s"${subproject}/src/main/resources/application.conf"
  val conf = ConfigFactory.parseFile(new File(confPath)).resolve()
  conf.getString(s"${subprojectName}.version")
}

val common = project
  .in(file("common"))
  .settings(
      name := "dxCommon",
      version := getVersion("common", "dxCommon"),
      settings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.typesafe
      ),
      assemblyJarName in assembly := "dxCommon.jar"
  )

val api = project
  .in(file("api"))
  .settings(
      name := "dxApi",
      version := getVersion("api", "dxApi"),
      settings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.dxCommon,
          dependencies.jackson,
          dependencies.guava,
          dependencies.commonsHttp
      ),
      assemblyJarName in assembly := "dxApi.jar"
  )

val protocols = project
  .in(file("protocols"))
  .settings(
      name := "dxFileAccessProtocols",
      version := getVersion("protocols", "dxFileAccessProtocols"),
      settings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.dxCommon,
          dependencies.dxApi,
          dependencies.awssdk,
          dependencies.nettyHandler,
          dependencies.nettyCodecHttp2
      ),
      assemblyJarName in assembly := "dxFileAccessProtocols.jar"
  )

// DEPENDENCIES

lazy val dependencies =
  new {
    val dxCommonVersion = "0.2.14-SNAPSHOT"
    val dxApiVersion = "0.2.0-SNAPSHOT"
    val typesafeVersion = "1.3.3"
    val sprayVersion = "1.3.5"
    val scalatestVersion = "3.1.1"
    val jacksonVersion = "2.11.0"
    val guavaVersion = "18.0"
    val httpClientVersion = "4.5.9"
    val logbackVersion = "1.2.3"
    val awsVersion = "2.15.1"
    val nettyVersion = "4.1.46.Final"

    val dxCommon = "com.dnanexus" % "dxcommon" % dxCommonVersion
    val dxApi = "com.dnanexus" % "dxapi" % dxApiVersion
    val typesafe = "com.typesafe" % "config" % typesafeVersion
    val spray = "io.spray" %% "spray-json" % sprayVersion
    val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
    val guava = "com.google.guava" % "guava" % guavaVersion
    val commonsHttp = "org.apache.httpcomponents" % "httpclient" % httpClientVersion
    val logback = "ch.qos.logback" % "logback-classic" % logbackVersion
    val awssdk = "software.amazon.awssdk" % "s3" % awsVersion
    val scalatest = "org.scalatest" % "scalatest_2.13" % scalatestVersion
    // dependencies of dependencies - specified here to resolve version conflicts
    val nettyHandler = "io.netty" % "netty-handler" % nettyVersion
    val nettyCodecHttp2 = "io.netty" % "netty-codec-http" % nettyVersion
  }

lazy val commonDependencies = Seq(
    dependencies.logback,
    dependencies.spray,
    dependencies.scalatest % Test
)

// SETTINGS

val githubResolver = Resolver.githubPackages("dnanexus", "dxScala")
resolvers += githubResolver

lazy val settings = Seq(
    scalacOptions ++= compilerOptions,
    // javac
    javacOptions ++= Seq("-Xlint:deprecation"),
    // reduce the maximum number of errors shown by the Scala compiler
    maxErrors := 20,
    // scalafmt
    scalafmtConfig := root.base / ".scalafmt.conf",
    // Publishing
    // disable publish with scala version, otherwise artifact name will include scala version
    // e.g dxScala_2.11
    crossPaths := false,
    // add repository settings
    // snapshot versions publish to GitHub repository
    // release versions publish to sonatype staging repository
    publishTo := Some(
        if (isSnapshot.value) {
          githubResolver
        } else {
          Opts.resolver.sonatypeStaging
        }
    ),
    githubOwner := "dnanexus",
    githubRepository := "dxScala",
    publishMavenStyle := true,
    // Tests
    // If an exception is thrown during tests, show the full
    // stack trace, by adding the "-oF" option to the list.
    Test / testOptions += Tests.Argument("-oF")
    // Coverage
    //
    // sbt clean coverage test
    // sbt coverageReport
    //coverageEnabled := true
    // To turn it off do:
    // sbt coverageOff
    // Ignore code parts that cannot be checked in the unit
    // test environment
    //coverageExcludedPackages := "dxScala.Main"
)

// Show deprecation warnings
val compilerOptions = Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-explaintypes",
    "-encoding",
    "UTF-8",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:doc-detached",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:nullary-override",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:type-parameter-shadow",
    "-Ywarn-dead-code",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:privates",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:imports", // warns about every unused import on every command.
    "-Xfatal-warnings" // makes those warnings fatal.
)

// Assembly
lazy val assemblySettings = Seq(
    logLevel in assembly := Level.Info,
    // comment out this line to enable tests in assembly
    test in assembly := {},
    assemblyMergeStrategy in assembly := customMergeStrategy.value
)
