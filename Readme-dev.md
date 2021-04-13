# How to build local version for testing

Set custom version name in `<project>/src/main/resources/application.conf`.

Run `sbt publishLocal`, which will [publish to your Ivy local file repository](https://www.scala-sbt.org/1.x/docs/Publishing.html).
