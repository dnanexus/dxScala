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
3. If the current snapshot version matches the release version, increment the snapshot version.
   - For example, if the current release is `1.0.0` and the current snapshot version is `1.0.0-SNAPSHOT`, increment the snapshot version to `1.0.1-SNAPSHOT`.
4. Make your changes. Test locally using `sbt test`.
5. Update the release notes under the top-most header (which should be "in develop").
6. If the current snapshot version only differs from the release version by a patch, and you added any new functionality (vs just fixing a bug), increment the minor version instead.
   - For example, when you first created the branch you set the version to `1.0.1-SNAPSHOT`, but then you realized you needed to add a new function to the public API, change the version to `1.1.0-SNAPSHOT`. 
7. When you are done, create a pull request against the `develop` branch.

### Building a local version for testing

- Set version name in `<project>/src/main/resources/application.conf` to `X.Y.Z-SNAPSHOT`.
- Run `sbt publishLocal`, which will [publish to your Ivy local file repository](https://www.scala-sbt.org/1.x/docs/Publishing.html).
- In any downstream project that will depend on the changes you are making, set the dependency version in `build.sbt` to `X.Y.Z-SNAPSHOT`.

### Merging the PR

When a PR is merged into `develop`, SNAPSHOT packages are automatically published to GitHub packages. When you push to `develop` (including merging a PR), you should announce that you are doing so (e.g. via GChat) for two reasons:

* Publishing a new snapshot requires deleting the existing one. If someone is trying to fetch the snapshot from the repository at the time when the snapshot workflow is running, they will get a "package not found" error.
* Although unlikely, it is possible that if two people merge into `develop` at around the same time, the older SNAPSHOT will overwrite the newer one.

## Releasing

### Sonatype Nexus publishing setup

We use [Sonatype Nexus repository manager](https://oss.sonatype.org/#stagingRepositories) to release artifacts. In order to set it up:

- Have `dnanexus`'s password for Sonatype. Riva Nathans and John Didion have the password.
- Install gnupg `brew install gnupg`.
- Generate a key `gpg --quick-gen-key <your email> rsa2048`. Remember the passphrase.
- Get the key's identifier.

```
% gpg --list-keys

pub   rsa2048 2021-09-09 [SC] [expires: 2023-09-09]
      <key identifier is this string>
uid           [ultimate] <your email>
```

- Distribute the key `gpg --keyserver keyserver.ubuntu.com --send-keys <key identifier>`.
- Export secret key for sbt plugin `gpg --armor --export-secret-key > ~/.sbt/gpg/secring.asc`. You will need to enter the passphrase.
- Add the following to `~/.sbt/1.0/plugins/gpg.sbt`.

```
credentials += Credentials(
  "GnuPG Key ID",
  "gpg",
  "<key identifer>", // key identifier
  "ignored" // this field is ignored; passwords are supplied by pinentry
)
```

- Add the following to `~/.sbt/1.0/sonatype.sbt`.

```
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
```

- Add the following to `~/.sbt/sonatype_credentials`.

```
realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user=dnanexus
password=<dnanexus's password>
```

Resources
- https://formulae.brew.sh/formula/gnupg
- https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html

### Beginning the release

1. Checkout the develop branch (either HEAD or the specific commit you want to release)
2. Create a release branch named with the version number, e.g. `release-2.4.2`, or if you are releasing multiple projects with different versions, with the current date, e.g. `release-2021-05.07`
3. Update the version numbers in application.conf files
   - For the projects you will release, remove "-SNAPSHOT"
   - For the projects not being released, reset the version to the current release version
4. Also update the version numbers in the dependency section of build.sbt
5. Update the release notes for each projects being released
   - Change the top header from "in develop" to "<version> (<date>)"

### Releasing to GitHub

1. Push the release branch to GitHub.
2. Run the release actions for the libraries you want to release.
3. Go to the "Releases" page on GitHub and publish each draft release.

### Releasing to Maven

Note: this process is currently coordinated by Riva Nathans & John Didion -- please contact them to request a release of the updated library(ies).

1. From the release branch, run `sbt 'project <name>' publishSigned -DreleaseTarget=sonatype` for each project being released. You will need to have completed the "Sonatype Nexus publishing setup" instructions above. You will be prompted to enter your key's passphrase.
2. Go to [Sonatype Nexus repository manager](https://oss.sonatype.org/#stagingRepositories), log in as `dnanexus`, and go to "Staging Repositories".
3. Check the repository(ies) to release; there should only be one, but if there are more check the contents to find yours.
4. Click the "Close" button. After a few minutes, hit "Refresh". The "Release" button should become un-grayed. If not, wait a few more minutes and referesh again.
5. Click the "Release" button.

### Completing the release

If you encounter any additional issues while creating the release, you will need to make the fixes in `develop` and then merge them into the release branch.

To complete the release, open a PR to merge the release branch into main. The easiest way to do this is to run `git merge main -X ours` from the release branch, commit, and push. Then set the base to `main` on the PR and merge the PR. You can then delete the release branch.

Unfortunately, the tags that are created on the release branch are not merged into `main` when merging the PR. Thus, after merging the PR, you must manually tag the `main` branch with the release, e.g.

```
$ git tag common-0.3.0 -am "release dxCommon 0.3.0"
$ git push origin common-0.3.0
```