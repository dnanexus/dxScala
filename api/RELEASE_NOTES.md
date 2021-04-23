# dxApi

## in develop

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
