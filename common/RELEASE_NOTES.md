# dxCommon

## 0.2.14 (2021-04-23)

* Adds assertion in `LocalizationDisambiguator.getLocalPath` that the `FileSource` name is not an absolute path

## 0.2.13

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
