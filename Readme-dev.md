# Building a local version for testing

- Set version name in `<project>/src/main/resources/application.conf` to `X.Y.Z-SNAPSHOT`.
- Run `sbt publishLocal`, which will [publish to your Ivy local file repository](https://www.scala-sbt.org/1.x/docs/Publishing.html).
- In the downstream project, set the dependency version in `build.sbt` to `X.Y.Z-SNAPSHOT`.

# Making a pull request

- During development cycle, make pull requests to `develop` branch.

# Releasing

(WIP, update as needed)

Release coordinator: John Didion

- Ensure that `develop` branches are in a compatible state.
- Ensure that release notes are up-to-date.
- Merge `develop` branches to `main`. Set actual (non-SNAPSHOT) release versions.
- Tag commit and publist release from `main`.
- Bump -SNAPSHOT version in `develop` branch for next development cycle.
