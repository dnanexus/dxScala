# dxCommon

## 0.11.5 (2023-03-07)

* Implemented immutable linked list to parse instance names

## 0.11.4 (2022-08-23)

* Fix for handling 503 error in dx CLI when API requests are throttled

## 0.11.3 (2022-05-11)

* Fix `JsUtils.makeDeterministic` to handle JsArrays sorting

## 0.11.2 (2022-02-25)

* Minor changes to JSON formatting

## 0.11.1 (2022-01-07)

* Fix `SysUtils.runCommand` forwarding of stderr

## 0.11.0 (2022-01-05)

* Adds `SysUtils.runCommand`, which exposes options for how to handle stdin/stdout/stderr.
* Adds `Paths.BaseEvalPaths.isLocal` attribute to differentiate local from remote paths.
* **Breaking** removes `SysUtils.execScript`. Use `runCommand` with the script path as the argument instead.
* Adds validation of path characters to `FileUtils.getUriScheme`
* Updates code to compile with JDK11
* Updates build environment to JDK11, Scala 2.13.7, and SBT 1.5.7

## 0.10.1 (2021-12-13)

* Prettify truncated log messages

## 0.10.0 (2021-12-09)

* Adds `PosixPath` class for working with POSIX-style paths
* **Breaking**: changes `Paths` to use `PosixPath` rather than `java.nio.Path`

## 0.9.0 (2021-11-15)

* Adds option to `Logger.trace*` to show beginning and/or end of log when limiting trace length

## 0.8.0 (2021-08-31)

* Adds `getTargetDir` methods to `LocalizationDisambiguator`
* Fixes use of `localizationDir` together with `force` in `SafeLocalizationDisambiguator`
* Adds `FileSource.resolveDirectory` as a separate method from `resolve`

## 0.7.0 (2021-07-27)

* Removes deprecated constructors from `Logger`
* Adds converstion to/from JSON to `Logger` (implicit) and `LocalizationDisambiguator` (explicit)
* Adds `SysUtils.availableCores` for getting number of available CPU cores
* Adds `SysUtils.time` for timing long-running operations
* `DockerUtils.pullImage` now handles the case where a manifest file is not available for an image tarball 

## 0.6.0 (2021-07-12)

* Adds `getLocalPathForSource` function to `LocalizationDisambiguator` for handling non-addressable `FileSource`s 
* Adds `linkFrom` method to `LocalFileSource`

## 0.5.2 (2021-06-29)

* No longer uses `--quiet` option with `docker pull` in `DockerUtils.pullImage`

## 0.5.1 (2021-06-28)

* Adds `FileUtils.normalizePath`
* Makes `DockerUtil.pullImage` compatible with earlier Docker versions by only using `--quiet` option with versions >= 19.03

## 0.5.0 (2021-06-23)

* Adds optional `localizationDir` parameter to `LocalizationDisambiguator` methods for specifying the localization directory that must be used
* Fixes `prettyFormat` function to handle case clases with public fields in the second parameter list
* Adds `recursive` parameter (default=`false`) to `FileSource.listing`
* Caches parent `LocalFileSource` when resolving children

## 0.4.1 (2021-06-08)

* Fixes issues with local and http path relativization

## 0.4.0 (2021-05-26)

* Adds `relativize` method to `AddressableFileSource`
* Adds `Logger.hideStackTraces` parameter, which controls whether stack traces are shown for warning/error messages, and defaults to false unless `traceLevel >= Verbose`

## 0.3.0 (2021-05-07)

* Fixes implementations of `AddressableFileSource.folder` for cases where the file source represents a root directory
* Adds `listing` method to `FileSource`
* Adds `container` and `version` fields to `AddressableFileSource` 
* Updates `LocalizationDisambiguator` to use `container` and `version` for disambiguation of identical paths in different systems

## 0.2.14 (2021-04-23)

* Adds assertion in `LocalizationDisambiguator.getLocalPath` that the `FileSource` name is not an absolute path

## 0.2.13 (2021-04-20)

* Eliminate possibility of file name collisions in `LocalizationDisambiguator` when `separateDirsBySource` is `false`
* Adds `LocalizationDisambiguator.getLocalPaths`, which can leverage a common disambiguation directory without the chance of collision

## 0.2.12 (2021-04-19)

* Adds `FileSource.exists` method

## 0.2.11 (2021-03-28)

* Fix truncation of trace messages
* Adds `getParent` and `resolve` methods to `AddressableFileSource`

## 0.2.10 (2021-03-10)

* Adds `FileUtils.changeFirstFileExt` with multiple drop extensions
* Add option to `LocalizationDisambiguator` to not create directories

## 0.2.9 (2021-02-25)

* Breaking change: `Bindings.update` renamed to `Bindings.addAll`

## 0.2.8 (2021-02-17)

* Change logger to use `Level` rather than `quiet` - add `Logger.apply(quiet, ...)` for backward compatibility
* Breaking change: remove `logger` parameter from `SysUtils.execCommand` and instead use custom Exception classes

## 0.2.7 (2021-02-17)

* Add `FileUtils.sanitizeFileName`

## 0.2.6 (2021-02-10)

* Additional JsUtils

## 0.2.5 (2021-01-27)

* Add `FileSourceResolver.localSearchPath` accessor

## 0.2.4 (2020-12-15)

* Add DockerUtils

## 0.2.3 (2020-11-25)

* Relocate LocalizationDisambiguator here from wdlTools

## 0.2.2 (2020-11-12)

* Add `canResolve` method to FileSourceResolver

## 0.2.1 (2020-11-06)

* Move variable attributes to secondary parameters in `FileSource` subclasses

## 0.2.0 (2020-10-26)

* Add ExecPaths and EvalPaths traits

## 0.1.0 (2020-10-23)

* First release
