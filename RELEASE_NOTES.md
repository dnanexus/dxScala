# dxScala release notes

## dxCommon 

### 0.2.8 (2021-02-17)

* Change logger to use `Level` rather than `quiet` - add `Logger.apply(quiet, ...)` for backward compatibility
* Remove `logger` parameter from `SysUtils.execCommand` and instead use custom Exception classes

### 0.2.7 (2021-02-17)

* Add `FileUtils.sanitizeFileName`

### 0.2.6 (2021-02-10)

* Additional JsUtils

### 0.2.5 (2021-01-27)

* Add `FileSourceResolver.localSearchPath` accessor

### 0.2.4 (2020-12-15)

* Add DockerUtils

### 0.2.3 (2020-11-25)

* Relocate LocalizationDisambiguator here from wdlTools

### 0.2.2 (2020-11-12)

* Add `canResolve` method to FileSourceResolver

### 0.2.1 (2020-11-06)

* Move variable attributes to secondary parameters in `FileSource` subclasses

### 0.2.0 (2020-10-26)

* Add ExecPaths and EvalPaths traits

### 0.1.0 (2020-10-23)

* First release

## dxApi

### 0.1.12 (2020-02-24)

* Add functions related to EBORs in `DxUtils`
* bugfixes

### 0.1.11 (2020-02-18)

* bugfixes

### 0.1.10 (2020-02-16)

* Add option to uploadFile to specify a destination
* Add `DxApi.uploadString`
* Add `folder` parameter to `(App|Applet|Workflow).newRun`

### 0.1.9 (2020-02-10)

* Fix `DxUtils.parseObjectId` to recognize named objects (apps and globalworkflows)

### 0.1.8 (2020-02-02)

* Support execution priority on app/applet/workflow newRun
* Allow for optional instance type requests

### 0.1.7 (2020-12-03)

* Support max requirements in InstanceTypeRequest

### 0.1.6 (2020-11-30)

* Handle null inputSpec/outputSpec when parsing workflow/describe
 
### 0.1.5 (2020-11-23)

* Fix DxFindDataObjects parsing of workflow results

### 0.1.4 (2020-11-16)

* Allow caching file name independenly of `describe()`

### 0.1.3 (2020-11-13)
 
* Add `DxInstanceType.matchesOrExceedes function`

### 0.1.2 (2020-11-12)

* Eliminate indeterminate comparisons by resource in DxInstanceType

### 0.1.1 (2020-11-06)

* Fix test
* Set number of API retries to 10
* Make `dxApi` a secondary parameter in `DxObject` subclasses

### 0.1.0 (2020-10-23)

* First release

## dxFileAccessProtocols

### 0.1.2 (2020-11-16)

* DxFileSource caches file name when parsing an extended dx:// URI

### 0.1.1 (2020-11-06)

* Move variable attributes to secondary parameters in `FileSource` subclasses

### 0.1.0 (2020-10-23)

* First release