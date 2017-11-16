[![Build Status](https://snap-ci.com/indix/dfs-datastores/branch/master/build_image)](https://snap-ci.com/indix/dfs-datastores/branch/master)

# dfs-datastores

A dramatically simpler and more powerful way to store records on a distributed filesystem.

To run the tests:

```scala
sbt test
```

To publish the changes
* Update the minor / majar version in version.sbt
* `sbt publish` to publish the artifact to artifactory

### Using Incremental PailConsolidate

Please make sure to add this argument to the PailConsolidate when you want to run the incremental version. The reason for doing so is because by default the S3N implementation does full recursive directory and file fetch without an ability to control the listing limit.

```
-Dfs.s3n.impl=com.indix.hadoop.fs.s3native.LimitedListingS3NFileSystem
```
