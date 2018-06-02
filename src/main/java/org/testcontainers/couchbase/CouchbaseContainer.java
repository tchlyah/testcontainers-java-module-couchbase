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
import lombok.Getter;
import lombok.experimental.Wither;
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

    @Getter
    private static final Collection<CouchbaseContainer> containers = new HashSet<>();

    @Getter(lazy = true)
    private final PortInfo portInfo = createPortInfo();

    private List<BucketSettings> newBuckets = new ArrayList<>();

    private String urlBase;

    public CouchbaseContainer() {
        this("couchbase/server:latest");
    }

    public CouchbaseContainer(String containerName) {
        super(containerName);
        containers.add(this);
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(CONFIG_PORT);
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
        setWaitStrategy(new HttpWaitStrategy().forPath("/ui/index.html#/"));
    }

    public SELF withNewBucket(BucketSettings bucketSettings) {
        newBuckets.add(bucketSettings);
        return self();
    }

    public void initCluster() {
        urlBase = String.format("http://%s:%s", getContainerIpAddress(), getMappedPort(CONFIG_PORT));
        try {
            String poolURL = "/pools/default";
            String poolPayload = "memoryQuota=" + URLEncoder.encode(memoryQuota, "UTF-8") + "&indexMemoryQuota=" + URLEncoder.encode(indexMemoryQuota, "UTF-8");

            String setupServicesURL = "/node/controller/setupServices";
            StringBuilder servicePayloadBuilder = new StringBuilder();
            if (keyValue) {
                servicePayloadBuilder.append("kv,");
            }
            if (query) {
                servicePayloadBuilder.append("n1ql,");
            }
            if (index) {
                servicePayloadBuilder.append("index,");
            }
            if (fts) {
                servicePayloadBuilder.append("fts,");
            }
            if (analytics) {
                servicePayloadBuilder.append("cbas,");
            }
            String setupServiceContent = "services=" + URLEncoder.encode(servicePayloadBuilder.toString(), "UTF-8");

            String webSettingsURL = "/settings/web";
            String webSettingsContent = "username=" + URLEncoder.encode(clusterUsername, "UTF-8") + "&password=" + URLEncoder.encode(clusterPassword, "UTF-8") + "&port=8091";

            String bucketURL = "/sampleBuckets/install";

            StringBuilder sampleBucketPayloadBuilder = new StringBuilder();
            sampleBucketPayloadBuilder.append('[');
            if (travelSample) {
                sampleBucketPayloadBuilder.append("\"travel-sample\",");
            }
            if (beerSample) {
                sampleBucketPayloadBuilder.append("\"beer-sample\",");
            }
            if (gamesIMSample) {
                sampleBucketPayloadBuilder.append("\"gamesim-sample\",");
            }
            sampleBucketPayloadBuilder.append(']');

            callCouchbaseRestAPI(poolURL, poolPayload);
            callCouchbaseRestAPI(setupServicesURL, setupServiceContent);
            callCouchbaseRestAPI(webSettingsURL, webSettingsContent);
            callCouchbaseRestAPI(bucketURL, sampleBucketPayloadBuilder.toString());

            CouchbaseWaitStrategy s = new CouchbaseWaitStrategy();
            s.withBasicCredentials(clusterUsername, clusterPassword);
            s.waitUntilReady(this);
            callCouchbaseRestAPI("/settings/indexes", "indexerThreads=0&logLevel=info&maxRollbackPoints=5&storageMode=memory_optimized");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createBucket(BucketSettings bucketSetting, boolean primaryIndex) {
        ClusterManager clusterManager = getCouchbaseCluster().clusterManager(clusterUsername, clusterPassword);
        // Insert Bucket
        BucketSettings bucketSettings = clusterManager.insertBucket(bucketSetting);
        // Insert Bucket admin user
        UserSettings userSettings = UserSettings.build()
                .password(bucketSetting.password())
                .roles(Collections.singletonList(new UserRole("bucket_admin", bucketSetting.name())));
        try {
            clusterManager.upsertUser(AuthDomain.LOCAL, bucketSetting.name(), userSettings);
        } catch (Exception e) {
            logger().warn("Unable to insert user '" + bucketSetting.name() + "', maybe you are using older version");
        }
        if (index) {
            Bucket bucket = getCouchbaseCluster().openBucket(bucketSettings.name(), bucketSettings.password());
            new CouchbaseQueryServiceWaitStrategy(bucket).waitUntilReady(this);
            if (primaryIndex) {
                bucket.query(Index.createPrimaryIndex().on(bucketSetting.name()));
            }
        }
    }

    public void callCouchbaseRestAPI(String url, String payload) throws IOException {
        String fullUrl = urlBase + url;
        HttpURLConnection httpConnection = (HttpURLConnection) ((new URL(fullUrl).openConnection()));
        httpConnection.setDoOutput(true);
        httpConnection.setRequestMethod("POST");
        httpConnection.setRequestProperty("Content-Type",
                "application/x-www-form-urlencoded");
        String encoded = Base64.encode((clusterUsername + ":" + clusterPassword).getBytes("UTF-8"));
        httpConnection.setRequestProperty("Authorization", "Basic " + encoded);
        DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
        out.writeBytes(payload);
        out.flush();
        out.close();
        httpConnection.getResponseCode();
        httpConnection.disconnect();
    }

    @Override
    public void start() {
        super.start();
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
        initCluster();
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
