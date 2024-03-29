# dxFileAccessProtocols

## in develop

...

## 0.5.6 (2023-06-08)

* changes to facilitate optimizations of the number of `file-xxx/describe` API calls upon (de)localization of input/output files.

## 0.5.5 (2022-08-23)

* Dependency update

## 0.5.4 (2022-03-15)

* Minor fixes to DxFileAccessProtocol to ensure directory path starts with a forward slash

## 0.5.3 (2022-01-05)

* Handles `ResourceNotFoundException` in `DxFileSource.exists`
* Updates code to compile with JDK11
* Updates build environment to JDK11, Scala 2.13.7, and SBT 1.5.7

## 0.5.2 (2021-12-09)

* Uses `PosixPath` rather than `java.nio.Path` for manipulating remote paths

## 0.5.1 (2021-11-18)

* Update dxApi version

## 0.5.0 (2021-08-31)

* Implements `resolveDirectory` method

## 0.4.1 (2021-06-28)

* Normalizes any file/folder paths

## 0.4.0 (2021-06-23)

* Exposes dx folder in `DxFolderSource`
* Implements recursive listing in `S3FileAccessProtocol` and `DxFileAccessProtocol`

## 0.3.1 (2021-06-08)

* Update dependencies

## 0.3.0 (2021-05-26)

* Implements `listing` and `relativize` methods for dx and S3 protocols

## 0.2.0 (2021-05-07)

* Fixes handling of `DxFolderSource` for root folders
* Implements `container` and `version` fields for dx and S3 protocols

## 0.1.6 (2021-04-23)

* Fixes parsing of dx:// URIs that include a file path

## 0.1.5 (2021-04-19)

* Implements `FileSource.exists` for dx and s3 protocols

## 0.1.4 (2021-04-16)

* Fixes `DxFileAccessProtocol` sources to ovewrite existing paths in `localizeTo`

## 0.1.3 (2021-03-28)

* DxFileAccessProtocol supports directories

## 0.1.2 (2020-11-16)

* DxFileSource caches file name when parsing an extended dx:// URI

## 0.1.1 (2020-11-06)

* Move variable attributes to secondary parameters in `FileSource` subclasses

## 0.1.0 (2020-10-23)

* First release
