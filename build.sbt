import Merging.customMergeStrategy
import sbt.Keys.{maxErrors, _}
import sbtassembly.AssemblyPlugin.autoImport.{assemblyMergeStrategy, _}
import com.typesafe.config._
import sbt.ThisBuild

name := "dxScala"

// build-wide settings
ThisBuild / organization := "com.dnanexus"
ThisBuild / scalaVersion := "2.13.7"
ThisBuild / developers := List(
    Developer("commandlinegirl",
      "Ola Zalcman",
      "azalcman@dnanexus.com",
      url("https://github.com/dnanexus")),
    Developer("Gvaihir", "Gvaihir", "aogrodnikov@dnanexus.com", url("https://github.com/dnanexus")),
    Developer("mhrvol", "Marek Hrvol", "mhrvol@dnanexus.com", url("https://github.com/dnanexus")),
    Developer("r-i-v-a",
      "Riva Nathans",
      "rnathans@dnanexus.com",
      url("https://github.com/dnanexus")),
    Developer("YuxinShi0423", "Yuxin Shi", "yshi@dnanexus.com", url("https://github.com/dnanexus")),
)
ThisBuild / homepage := Some(url("https://github.com/dnanexus/dxScala"))
ThisBuild / scmInfo := Some(
    ScmInfo(url("https://github.com/dnanexus/dxScala"), "git@github.com:dnanexus/dxScala.git")
)
ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// PROJECTS

lazy val root = project
  .in(file("."))
  .settings(
      globalSettings,
      publish / skip := true
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(
      common,
      api,
      protocols,
      yaml
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
      globalSettings,
      publishSettings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.typesafe
      ),
      assembly / assemblyJarName := "dxCommon.jar"
  )

val api = project
  .in(file("api"))
  .settings(
      name := "dxApi",
      version := getVersion("api", "dxApi"),
      globalSettings,
      publishSettings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.dxCommon,
          dependencies.jackson,
          dependencies.guava,
          dependencies.commonsHttp
      ),
      assembly / assemblyJarName := "dxApi.jar"
  )

val protocols = project
  .in(file("protocols"))
  .settings(
      name := "dxFileAccessProtocols",
      version := getVersion("protocols", "dxFileAccessProtocols"),
      globalSettings,
      publishSettings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.dxCommon,
          dependencies.dxApi,
          dependencies.awssdk,
          dependencies.nettyHandler,
          dependencies.nettyCodecHttp2
      ),
      assembly / assemblyJarName := "dxFileAccessProtocols.jar"
  )

val yaml = project
  .in(file("yaml"))
  .settings(
      name := "dxYaml",
      version := getVersion("yaml", "dxYaml"),
      globalSettings,
      publishSettings,
      assemblySettings,
      libraryDependencies ++= commonDependencies ++ Seq(
          dependencies.snakeyaml
      ),
      assembly / assemblyJarName := "dxYaml.jar"
  )

// DEPENDENCIES

lazy val dependencies =
  new {
    val dxCommonVersion = "0.11.4"
    val dxApiVersion = "0.13.4"
    val typesafeVersion = "1.4.1"
    val sprayVersion = "1.3.6"
    val snakeyamlVersion = "2.3"
    val scalatestVersion = "3.2.13"
    val mockitoVersion = "1.17.12"
    val jacksonVersion = "2.13.1"
    val guavaVersion = "23.0"
    val httpClientVersion = "4.5.13"
    val logbackVersion = "1.2.10"
    val awsVersion = "2.17.102"
    val nettyVersion = "4.1.46.Final"

    val dxCommon = "com.dnanexus" % "dxcommon" % dxCommonVersion
    val dxApi = "com.dnanexus" % "dxapi" % dxApiVersion
    val typesafe = "com.typesafe" % "config" % typesafeVersion
    val spray = "io.spray" %% "spray-json" % sprayVersion
    val snakeyaml = "org.snakeyaml" % "snakeyaml-engine" % snakeyamlVersion
    val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
    val guava = "com.google.guava" % "guava" % guavaVersion
    val commonsHttp = "org.apache.httpcomponents" % "httpclient" % httpClientVersion
    val logback = "ch.qos.logback" % "logback-classic" % logbackVersion
    val awssdk = "software.amazon.awssdk" % "s3" % awsVersion
    val scalatest = "org.scalatest" % "scalatest_2.13" % scalatestVersion
    val mockitoScala = "org.mockito" %% "mockito-scala" % mockitoVersion % Test
    // dependencies of dependencies - specified here to resolve version conflicts
    val nettyHandler = "io.netty" % "netty-handler" % nettyVersion
    val nettyCodecHttp2 = "io.netty" % "netty-codec-http" % nettyVersion
  }

lazy val commonDependencies = Seq(
    dependencies.logback,
    dependencies.spray,
    dependencies.scalatest % Test,
    dependencies.mockitoScala
)

// SETTINGS

val githubResolver = Resolver.githubPackages("dnanexus", "dxScala")
resolvers += githubResolver

val releaseTarget = Option(System.getProperty("releaseTarget")).getOrElse("github")

lazy val globalSettings = Seq(
    scalacOptions ++= compilerOptions,
    // javac
    javacOptions ++= Seq("-Xlint:deprecation", "-source", "1.8", "-target", "1.8"),
    // reduce the maximum number of errors shown by the Scala compiler
    maxErrors := 20,
    // scalafmt
    scalafmtConfig := file(".") / ".scalafmt.conf",
    // Publishing
    // disable publish with scala version, otherwise artifact name will include scala version
    // e.g dxScala_2.11
    crossPaths := false,
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

lazy val publishSettings = Seq(
    // snapshot versions publish to GitHub repository
    // release versions publish to sonatype staging repository
    publishTo := Some(
        if (isSnapshot.value || releaseTarget == "github") {
          githubResolver
        } else {
          Opts.resolver.sonatypeStaging
        }
    ),
    githubOwner := "dnanexus",
    githubRepository := "dxScala",
    publishMavenStyle := true
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
    assembly / logLevel := Level.Info,
    // comment out this line to enable tests in assembly
    assembly / test := {},
    assembly / assemblyMergeStrategy := customMergeStrategy.value
)
