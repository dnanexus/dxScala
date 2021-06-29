# dxApi

## in develop

* Fixes `DxApi.getWorkingDir`

## 0.5.1 (2021-06-28)

* Normalizes any object/folder paths

## 0.5.0 (2021-06-23)

* Adds `folder` attribute to job description
* Checks that all necessary fields are available when trying to used cached object descriptions
* Creates `DxFile` with destination project when uploading to a destination project
* Modifies `DxFile.uploadDirectory` signature:
    * Optional `filter` parameter to upload only certain files in the directory
    * Also returns mapping of local path to `DxFile`
* Caches projects by ID as well as by name for later reuse

## 0.4.1 (2021-06-08)

* Adds warning when `findDataObjects` is called without a project

## 0.4.0 (2021-05-26)

* Adds `tags` and `properties` parameters to the `DxApi.upload*` functions.

## 0.3.0 (2021-05-07)

* Adds functions to create `dx://` URIs from components
* URL-encodes project names, paths, and file names when creating `dx://` URIs, and decodes them when parsing 
* Adds `state` parameter to `DxFindDataObjects`
* Adds `DxApi.uploadDirectory` function

## 0.2.0 (2021-04-23)

* Adds option to wait for upload to `DxApi.uploadFile`
* Allow caching file folder independenly of `describe()`

## 0.1.16 (2021-04-19)

* Adds 'state' field to `DxFileDescribe`

## 0.1.15 (2021-04-16)

* Adds `overwrite` option to `DxApi.downloadFile`
* Adds `dependsOn` field to `DxAnalysis.describe`

## 0.1.14 (2021-03-28)

* Adds `DxPath.split` function
* Adds option to ignore maximum resource bounds in `InstanceTypeRequest`
* bugfixes

## 0.1.13 (2021-02-25)

* Improves `DxApi.downloadFile`

## 0.1.12 (2021-02-24)

* Add functions related to EBORs in `DxUtils`
* bugfixes

## 0.1.11 (2021-02-18)

* bugfixes

## 0.1.10 (2021-02-16)

* Add option to uploadFile to specify a destination
* Add `DxApi.uploadString`
* Add `folder` parameter to `(App|Applet|Workflow).newRun`

## 0.1.9 (2021-02-10)

* Fix `DxUtils.parseObjectId` to recognize named objects (apps and globalworkflows)

## 0.1.8 (2021-02-02)

* Support execution priority on app/applet/workflow newRun
* Allow for optional instance type requests

## 0.1.7 (2020-12-03)

* Support max requirements in InstanceTypeRequest

## 0.1.6 (2020-11-30)

* Handle null inputSpec/outputSpec when parsing workflow/describe

## 0.1.5 (2020-11-23)

* Fix DxFindDataObjects parsing of workflow results

## 0.1.4 (2020-11-16)

* Allow caching file name independenly of `describe()`

## 0.1.3 (2020-11-13)

* Add `DxInstanceType.matchesOrExceedes function`

## 0.1.2 (2020-11-12)

* Eliminate indeterminate comparisons by resource in DxInstanceType

## 0.1.1 (2020-11-06)

* Fix test
* Set number of API retries to 10
* Make `dxApi` a secondary parameter in `DxObject` subclasses

## 0.1.0 (2020-10-23)

* First release
