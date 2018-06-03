/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.testcontainers.couchbase;

import com.couchbase.client.core.config.DefaultPortInfo;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.Base64;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.Index;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.Wither;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.HttpWaitStrategy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

/**
 * Based on Laurent Doguin version
 * <p>
 * Optimized by ctayeb
 */
@AllArgsConstructor
public class CouchbaseContainer<SELF extends CouchbaseContainer<SELF>> extends GenericContainer<SELF> {

    //<editor-fold desc="Ports">
    private static final int BINARY_PORT = 11210;
    private static final int BINARY_SSL_PORT = 11207;
    private static final int CONFIG_PORT = 8091;
    private static final int CONFIG_SSL_PORT = 18091;
    private static final int VIEW_PORT = 8092;
    private static final int VIEW_SSL_PORT = 18092;
    private static final int QUERY_PORT = 8093;
    private static final int QUERY_SSL_PORT = 18093;
    private static final int SEARCH_PORT = 8094;
    private static final int SEARCH_SSL_PORT = 18094;
    private static final int ANALYTICS_PORT = 8095;
    private static final int ANALYTICS_SSL_PORT = 18095;
    //</editor-fold>
    public static final String DELIMITER = ",";

    @Getter
    @Wither
    private boolean ssl = false;

    @Wither
    private String memoryQuota = "300";

    @Wither
    private String indexMemoryQuota = "300";

    @Wither
    private String clusterUsername = "Administrator";

    @Wither
    private String clusterPassword = "password";

    @Wither
    private boolean keyValue = true;

    @Getter
    @Wither
    private boolean query = true;

    @Getter
    @Wither
    private boolean index = true;

    @Getter
    @Wither
    private boolean primaryIndex = true;

    @Getter
    @Wither
    private boolean fts = false;

    @Getter
    @Wither
    private boolean analytics = false;

    @Wither
    private boolean beerSample = false;

    @Wither
    private boolean travelSample = false;

    @Wither
    private boolean gamesIMSample = false;

    @Getter(lazy = true)
    private final CouchbaseEnvironment couchbaseEnvironment = createCouchbaseEnvironment();

    @Getter(lazy = true)
    private final CouchbaseCluster couchbaseCluster = createCouchbaseCluster();

    @Getter(lazy = true)
    private final CouchbaseNodeWaitStrategy couchbaseNodeWaitStrategy = createCouchbaseWaitStrategy();

    @Getter
    private static final Collection<CouchbaseContainer> containers = new HashSet<>();

    @Getter(lazy = true)
    private final PortInfo portInfo = createPortInfo();

    private List<BucketSettings> newBuckets = new ArrayList<>();

    @Getter(lazy = true)
    private final String urlBase = createUrlBase();

    public CouchbaseContainer() {
        this("couchbase/server:latest");
    }

    public CouchbaseContainer(String containerName) {
        super(containerName);
        containers.add(this);
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return isSsl() ? getMappedPort(CONFIG_SSL_PORT) : getMappedPort(CONFIG_PORT);
    }

    @Override
    protected void configure() {
        // Configurable ports
        if (isSsl()) {
            addExposedPorts(CONFIG_SSL_PORT, VIEW_SSL_PORT, BINARY_SSL_PORT);
        } else {
            addExposedPorts(CONFIG_PORT, VIEW_PORT, BINARY_PORT);
        }
        if (isQuery()) {
            addExposedPort(QUERY_PORT);
            if (isSsl()) {
                addExposedPort(QUERY_SSL_PORT);
            }
        }
        if (isFts()) {
            addExposedPort(SEARCH_PORT);
            if (isSsl()) {
                addExposedPort(SEARCH_SSL_PORT);
            }
        }
        if (isAnalytics()) {
            addExposedPort(ANALYTICS_PORT);
            if (isSsl()) {
                addExposedPort(ANALYTICS_SSL_PORT);
            }
        }
        HttpWaitStrategy waitStrategy = new HttpWaitStrategy().forPath("/ui/index.html#/");
        setWaitStrategy(ssl ? waitStrategy.usingTls() : waitStrategy);
    }

    public SELF withNewBucket(BucketSettings bucketSettings) {
        newBuckets.add(bucketSettings);
        return self();
    }

    @SneakyThrows
    public void init() {
        initCluster();
        initServices();
        initAdminUser();
        initSampleBuckets();
        this.getCouchbaseNodeWaitStrategy().waitUntilReady(this);
        initIndexes();
        this.getCouchbaseNodeWaitStrategy().waitUntilReady(this);
    }

    private void initCluster() throws IOException {
        logger().debug("Initializing couchbase cluster");
        String poolURL = "/pools/default";
        String poolPayload = "memoryQuota=" + URLEncoder.encode(memoryQuota, "UTF-8") + "&indexMemoryQuota=" + URLEncoder.encode(indexMemoryQuota, "UTF-8");
        callCouchbaseRestAPI(poolURL, poolPayload);
    }

    private void initServices() throws IOException {
        StringJoiner services = new StringJoiner(DELIMITER);
        if (keyValue) {
            services.add("kv");
        }
        if (query) {
            services.add("n1ql");
        }
        if (index) {
            services.add("index");
        }
        if (fts) {
            services.add("fts");
        }
        if (analytics) {
            services.add("cbas");
        }
        logger().debug("Initializing services : {}", services.toString());
        callCouchbaseRestAPI("/node/controller/setupServices", "services=" + URLEncoder.encode(services.toString(), "UTF-8"));
    }

    private void initAdminUser() throws IOException {
        logger().debug("Creating cluster admin user '{}'", clusterUsername);
        callCouchbaseRestAPI("/settings/web",
                "username=" + URLEncoder.encode(clusterUsername, "UTF-8") + "&password=" + URLEncoder.encode(clusterPassword, "UTF-8") + "&port=8091");
    }

    private void initSampleBuckets() throws IOException {
        StringJoiner sampleBucketPayload = new StringJoiner(DELIMITER);
        if (travelSample) {
            sampleBucketPayload.add("\"travel-sample\"");
        }
        if (beerSample) {
            sampleBucketPayload.add("\"beer-sample\"");
        }
        if (gamesIMSample) {
            sampleBucketPayload.add("\"gamesim-sample\"");
        }
        if (sampleBucketPayload.length() != 0) {
            logger().debug("Initialize sample buckets {}", sampleBucketPayload.toString());
            callCouchbaseRestAPI("/sampleBuckets/install", "[" + sampleBucketPayload.toString() + "]");
        }
    }

    private void initIndexes() throws IOException {
        logger().debug("Activate memory optimized index");
        callCouchbaseRestAPI("/settings/indexes", "indexerThreads=0&logLevel=info&maxRollbackPoints=5&storageMode=memory_optimized");
    }

    private String createUrlBase() {
        return String.format((ssl ? "https" : "http") + "://%s:%s", getContainerIpAddress(), getMappedPort(CONFIG_PORT));
    }

    @NotNull
    private CouchbaseNodeWaitStrategy createCouchbaseWaitStrategy() {
        return new CouchbaseNodeWaitStrategy()
                .withUsername(clusterUsername)
                .withPassword(clusterPassword)
                .withSsl(ssl);
    }

    public void createBucket(BucketSettings bucketSetting, boolean primaryIndex) {
        logger().debug("Creating bucket {}", bucketSetting.name());
        ClusterManager clusterManager = getCouchbaseCluster().clusterManager(clusterUsername, clusterPassword);
        // Insert Bucket
        BucketSettings bucketSettings = clusterManager.insertBucket(bucketSetting);
        // Insert Bucket admin user
        logger().debug("Creating bucket admin user '{}'", bucketSetting.name());
        UserSettings userSettings = UserSettings.build()
                .password(bucketSetting.password())
                .roles(Collections.singletonList(new UserRole("bucket_admin", bucketSetting.name())));
        try {
            clusterManager.upsertUser(AuthDomain.LOCAL, bucketSetting.name(), userSettings);
        } catch (Exception e) {
            logger().warn("Unable to insert user '" + bucketSetting.name() + "', maybe you are using older version");
        }
        this.getCouchbaseNodeWaitStrategy().waitUntilReady(this);
        if (index) {
            Bucket bucket = getCouchbaseCluster().openBucket(bucketSettings.name(), bucketSettings.password());
            new CouchbaseQueryServiceWaitStrategy(bucket).waitUntilReady(this);
            if (primaryIndex) {
                logger().debug("Creating primary index");
                bucket.query(Index.createPrimaryIndex().on(bucketSetting.name()));
            }
        }
    }

    public void callCouchbaseRestAPI(String url, String payload) throws IOException {
        String fullUrl = getUrlBase() + url;
        @Cleanup(value = "disconnect")
        HttpURLConnection httpConnection = (HttpURLConnection) ((new URL(fullUrl).openConnection()));
        httpConnection.setDoOutput(true);
        httpConnection.setRequestMethod("POST");
        httpConnection.setRequestProperty("Content-Type",
                "application/x-www-form-urlencoded");
        String encoded = Base64.encode((clusterUsername + ":" + clusterPassword).getBytes("UTF-8"));
        httpConnection.setRequestProperty("Authorization", "Basic " + encoded);
        @Cleanup
        DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
        out.writeBytes(payload);
        out.flush();
        httpConnection.getResponseCode();
    }

    @Override
    public void start() {
        super.start();
        init();
        if (!newBuckets.isEmpty()) {
            for (BucketSettings bucketSetting : newBuckets) {
                createBucket(bucketSetting, primaryIndex);
            }
        }
    }

    private CouchbaseCluster createCouchbaseCluster() {
        return CouchbaseCluster.create(getCouchbaseEnvironment(), getContainerIpAddress());
    }

    private DefaultCouchbaseEnvironment createCouchbaseEnvironment() {
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder()
                .sslEnabled(ssl);
        if (isSsl()) {
            builder
                    .bootstrapCarrierSslPort(getMappedPort(BINARY_SSL_PORT))
                    .bootstrapHttpSslPort(getMappedPort(CONFIG_SSL_PORT));
        } else {
            builder
                    .bootstrapCarrierDirectPort(getMappedPort(BINARY_PORT))
                    .bootstrapHttpDirectPort(getMappedPort(CONFIG_PORT));
        }
        return builder.build();
    }

    private PortInfo createPortInfo() {
        DefaultPortInfo portInfo = new DefaultPortInfo(new HashMap<>(), null);
        try {
            portInfo.ports().put(ServiceType.VIEW, getMappedPort(VIEW_PORT));
            portInfo.ports().put(ServiceType.CONFIG, getMappedPort(CONFIG_PORT));
            portInfo.ports().put(ServiceType.BINARY, getMappedPort(BINARY_PORT));
            if (isSsl()) {
                portInfo.sslPorts().put(ServiceType.BINARY, getMappedPort(BINARY_SSL_PORT));
                portInfo.sslPorts().put(ServiceType.CONFIG, getMappedPort(CONFIG_SSL_PORT));
                portInfo.sslPorts().put(ServiceType.VIEW, getMappedPort(VIEW_SSL_PORT));
            }
            if (isQuery()) {
                portInfo.ports().put(ServiceType.QUERY, getMappedPort(QUERY_PORT));
                if (isSsl()) {
                    portInfo.sslPorts().put(ServiceType.QUERY, getMappedPort(QUERY_SSL_PORT));
                }
            }
            if (isFts()) {
                portInfo.ports().put(ServiceType.SEARCH, getMappedPort(SEARCH_PORT));
                if (isSsl()) {
                    portInfo.sslPorts().put(ServiceType.SEARCH, getMappedPort(SEARCH_SSL_PORT));
                }
            }
            if (isAnalytics()) {
                portInfo.ports().put(ServiceType.ANALYTICS, getMappedPort(ANALYTICS_PORT));
                if (isSsl()) {
                    portInfo.sslPorts().put(ServiceType.ANALYTICS, getMappedPort(ANALYTICS_SSL_PORT));
                }
            }
        } catch (IllegalStateException e) {
            logger().warn(e.getMessage());
        }
        return portInfo;
    }
}
