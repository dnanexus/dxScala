# dxApi

## unreleased

## 0.13.10 (2024-03-25)

* Change to make the user-agent string for dxScala more distinctive.
* Support for collecting data objects / files nested inside JSON objects.

## 0.13.9 (2024-02-29)

* adds `headJobOnDemand` attribute to jobNew call

## 0.13.8 (2023-07-21)

* changes to allow compiling with `treeTurnaroundTimeThreshold` attribute which facilitates platform to send the email 
notifications for the root jobs/analyses with a run time (aka `treeTurnatoundTime`) exceeding the specified threshold. 
This feature is not exclusive for dxCompiler and more information is available in the platform documentation.

## 0.13.6 (2023-06-08)

* changes to facilitate optimizations of the number of `file-xxx/describe` API calls upon (de)localization of input/output files.

## 0.13.5 (2023-03-07)

* Upgrades instance type to V2 in AWS regions when available. The upgrade happens only if the user specified system 
requirements for a task/process. 

## 0.13.4 (2023-02-23)

* Fixes handling of `suggestions` field for IO specs when only project ID is specified. Now instead of exception
a warning is thrown.

## 0.13.3 (2022-08-23)

* Handling of non-fully qualified file IDs for bulk search/describe. Now for the files provided without the project ID, 
the `describe` response will be returned only for current workspace/project. If a file was cloned to other projects, they 
will be ignored. Non-fully qualified file IDs are not allowed when searching files in other projects.
* Regression tests for API calls to platform

## 0.13.2 (2022-03-15)

* Added API methods for describing dbcluster and database objects

## 0.13.1 (2022-01-05)

* Updates code to compile with JDK11
* Updates build environment to JDK11, Scala 2.13.7, and SBT 1.5.7

## 0.13.0 (2021-12-09)

* Adds `DxApi.addTags` method
* Fixes `DxFindDataObjects` when used with `tags` constraint
* Handles record results in `DxFindDataObjects`
* Adds `systemRequirements` to `DxWorkflowStageDesc`

## 0.12.0 (2021-11-18)

* Enables `retryLimit` to be set for `DxApi.uploadFile` and `DxApi.downloadFile`

## 0.11.0 (2021-11-15)

* Fixes `DxApi.resolveApp` to handle app name with with version (e.g. `bwa_mem/1.0.0`)
* Adds `version` field to `DxAppDescribe`

## 0.10.1 (2021-11-10)

* Fixes `resolveProject` to handle `container-` objects
* Improves error message when API call failes due to connection error

## 0.10.0 (2021-09-08)

* Removes price-based selection of instance types in favor of rank-based selection
* Fixes parsing of non-file-type default values that are reference-type links
* Fixes parsing of parameter defaults/suggestions/choices that are of type `Hash`

## 0.9.0 (2021-08-31)

* Make including the project optional (default true) in `DxUtils.dxDataObjectToUri`
* Adds `force` option to `DxProject.removeObjects`
* Adds `cloneDataObject` method to `dxApi`
* Fixes parsing of file-type default values that are reference-type links 

## 0.8.0 (2021-07-27)

* Fixes parsing of File-type default values that are a DNAnexus link with a project ID or field reference  
* Adds `DxApi.uploadFiles`, `DxApi.uploadStrings`, and `DxApi.uploadDirectories`, which support parallel uploading

## 0.7.0 (2021-07-16)

* Unifies parsing of `choices`, `suggestions`, and `default` in `DxIOParameter`
* No longer attempts to resolve files/projects/folders when parsing a `DxIOParameter`
* Adds tags field to `DxFileDescribe`
* Adds hidden field to `DxWorkflowDescribe`, `DxAppletDescribe`
* `DxProject` now extends `DxObject` rather than `DxDataObject`
* Adds `DxApi.dataObjectFromJson` method

## 0.6.0 (2021-07-12)

* Adds option to `DxApi.describeFilesBulk` to search first in the workspace container
* `DxApi.resolveDataObject` now searches in the current workspace and/or project if the project is not specified explicitly
* Refactors `DxFindDataObjects` to use separate `DxFindDataObjectsConstraints` class for specifying constraints
* Uses the currently select project ID as the workspace ID when not running in a job.
* Better handles insufficient permissions when requesting instance type price list

## 0.5.2 (2021-06-29)

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
