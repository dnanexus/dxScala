addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
// sbt-sonatype plugin used to publish artifact to maven central via sonatype nexus
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
// sbt-pgp plugin used to sign the artifcat with pgp keys
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
// sbt-github-packages plugin used to publish snapshot versions to github packages
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "0.5.2")

// This prevents the SLF dialog from appearing when starting an SBT session
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.24"
// only load git plugin if we're actually in a git repo
libraryDependencies ++= {
  if (baseDirectory.value / "../.git" isDirectory)
    Seq(
        Defaults.sbtPluginExtra("com.typesafe.sbt" % "sbt-git" % "1.0.0",
                                (update / sbtBinaryVersion).value,
                                (update / scalaBinaryVersion).value)
    )
  else {
    println("sbt-git plugin not loaded")
    Seq.empty
  }
}
