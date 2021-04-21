## Setup

You will need to create a GitHub personal access token (this is required by the sbt-github-packages plugin). In GitHub settings, go to "Developer settings > Personal access token" and create a new token with "write:packages" and "read:packages" scopes only. Then, export the `GITHUB_TOKEN` environment variable with this token as the value. For example, in your `.profile`:

```bash
export GITHUB_TOKEN=<your personal access token>
```

On macOS, you may also want to add this token into your global environment so it is visible to your IDE:

```bash
launchctl setenv GITHUB_TOKEN $GITHUB_TOKEN
```

## Making a change

If you want to make a change to dxScala, do the following:

1. Checkout the `develop` branch.
2. Create a new branch with your changes. Name it something meaningful, like `APPS-123-download-bug`.
3. Make your changes. Test locally using `sbt test`.
4. When you are done, create a pull request against the `develop` branch.

When a PR is merged into `develop`, SNAPSHOT packages are automatically published to GitHub packages. Although unlikely, it is possible that if two people merge into `develop` at around the same time, the older SNAPSHOT will overwrite the newer one, so you should announce when you are going to merge your PR before doing so.

## Releasing

There are two parts to the release process: releasing to GitHub and releasing to the Maven repository.

### Releasing to GitHub

1. Checkout out the develop branch (either HEAD or the specific commit you want to release)
2. Create a release branch named with the version number, e.g. `release-2.4.2`
3. Update the version numbers in application.conf files
    - For the libraries you will release, remove "-SNAPSHOT"
    - For the libraries not being released, reset the version to the released version
4. Update the release notes
    - Change the top header from "in develop" to "<version> (<date>)"
5. Push the branch to GitHub
6. Run the release actions for the libraries you want to release
7. Go to the "Releases" page on GitHub and publish each draft release

### Releasing to Maven

Note: this process is currently coordinated by John Didion - please request from him a release of the updated library(ies).

1. From the release branch, run `sbt publishSigned`. You will need to have the SonaType PGP private key on your machine, and you will need the password.
2. Go to [nexus repository manager](https://oss.sonatype.org/#stagingRepositories), log in, and go to "Staging Repositories".
3. Check the repository(ies) to release (there should only be one), but if there are more, check the contents to find yours).
4. Click the "Close" button. After a few minutes, hit "Refresh". The "Release" button should become un-grayed. If not, wait a few more minutes and referesh again.
5. Click the "Release" button.

### Completing the release

If you encounter any additional issues while creating the release, you will need to make the fixes in `develop` and then merge them into the release branch.

To complete the release, open a PR to merge the release branch into main. 