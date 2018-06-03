<img src="https://cdn.worldvectorlogo.com/logos/couchbase.svg" width="300" />

# TestContainers Couchbase Module
Testcontainers module for Couchbase. [Couchbase](https://www.couchbase.com/) is a Document oriented NoSQL database.

See [testcontainers.org](https://www.testcontainers.org) for more information about Testcontainers.

## Usage example

Running Couchbase as a stand-in in a test:

### Create you own bucket

```java
public class SomeTest {

    @Rule
    public CouchbaseContainer couchbase = new CouchbaseContainer()
             .withNewBucket(DefaultBucketSettings.builder()
                        .enableFlush(true)
                        .name('bucket-name')
                        .quota(100)
                        .type(BucketType.COUCHBASE)
                        .build());
    
    @Test
    public void someTestMethod() {
        Bucket bucket = couchbase.getCouchbaseCluster().openBucket('bucket-name')
        
        ... interact with client as if using Couchbase normally
```

### Use preconfigured default bucket

Bucket is cleared after each test

```java
public class SomeTest extends AbstractCouchbaseTest {

    @Test
    public void someTestMethod() {
        Bucket bucket = getBucket();
        
        ... interact with client as if using Couchbase normally
```

### Special consideration

Begining from version [1.2](https://github.com/differentway/testcontainers-java-module-couchbase/releases/tag/1.2), Couchbase testContainer is configured to use random available ports for all [ports](https://developer.couchbase.com/documentation/server/current/install/install-ports.html) : 
- **8091** : REST/HTTP traffic ([bootstrapHttpDirectPort](http://docs.couchbase.com/sdk-api/couchbase-java-client-2.4.6/com/couchbase/client/java/env/DefaultCouchbaseEnvironment.Builder.html#bootstrapCarrierDirectPort-int-))
- **18091** : REST/HTTP traffic with SSL ([bootstrapHttpSslPort](http://docs.couchbase.com/sdk-api/couchbase-java-client-2.4.6/com/couchbase/client/java/env/DefaultCouchbaseEnvironment.Builder.html#bootstrapCarrierSslPort-int-))
- **11210** : memcached ([bootstrapCarrierDirectPort](http://docs.couchbase.com/sdk-api/couchbase-java-client-2.4.6/com/couchbase/client/java/env/DefaultCouchbaseEnvironment.Builder.html#bootstrapCarrierDirectPort-int-))
- **11207** : memcached SSL ([bootstrapCarrierSslPort](http://docs.couchbase.com/sdk-api/couchbase-java-client-2.4.6/com/couchbase/client/java/env/DefaultCouchbaseEnvironment.Builder.html#bootstrapCarrierSslPort-int-))
- **8092** : Queries, views, XDCR
- **8093** : REST/HTTP Query service
- **8094** : REST/HTTP Search Service
- **8095** : REST/HTTP Analytic service

---
[![Build Status](https://travis-ci.org/differentway/testcontainers-java-module-couchbase.svg?branch=master)](https://travis-ci.org/differentway/testcontainers-java-module-couchbase) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.differentway/couchbase-testcontainer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.differentway/couchbase-testcontainer) [![Licence](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/differentway/testcontainers-java-module-couchbase/blob/master/LICENSE)
