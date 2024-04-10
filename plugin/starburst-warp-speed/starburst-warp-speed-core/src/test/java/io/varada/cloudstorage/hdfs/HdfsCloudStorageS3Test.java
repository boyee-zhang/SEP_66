/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.varada.cloudstorage.hdfs;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.spi.NodeManager;
import io.varada.cloudstorage.CloudStorageAbstractTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

@Disabled
public class HdfsCloudStorageS3Test
        extends CloudStorageAbstractTest
{
//    public static final String ACCESS_KEY = null;
//    public static final String SECRET_KEY = null;
    public static final String IAM_ROLE = "";
    public static final String EXTERNAL_ID = "";
    public static final String REGION = "us-east-1";
    public static final String BUCKET_NAME = "varadaio-us-east1-unit-tests";
    public static final String EMPTY_BUCKET_NAME = "varadaio-us-east1-mount-point-try1";

    private static Injector injector;

    @BeforeAll
    static void beforeAll()
    {
        System.setProperty("aws.profile", "varada");
        System.setProperty("aws.region", REGION);

        injector = Guice.createInjector(new HdfsTestModule());
    }

    @BeforeEach
    void setUp()
    {
        TrinoFileSystemFactory fileSystemFactory = injector.getInstance(TrinoFileSystemFactory.class);

        cloudStorage = new HdfsCloudStorage(fileSystemFactory);
    }

    @Override
    protected String getBucket()
    {
        return "s3://" + BUCKET_NAME + "/";
    }

    @Override
    protected String getEmptyBucket()
    {
        return "s3://" + EMPTY_BUCKET_NAME + "/";
    }

    @Override
    protected String getNotExistBucket()
    {
        return "s3://" + BUCKET_NAME + "-not-exist/";
    }

    static class HdfsTestModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            String catalogName = "catalogName";
            NodeManager nodeManager = Mockito.mock(NodeManager.class);
            OpenTelemetry openTelemetry = OpenTelemetry.noop();

            Map<String, String> properties = new HashMap<>();
            properties.put("hive.s3.iam-role", IAM_ROLE);
            properties.put("hive.s3.external-id", EXTERNAL_ID);

            ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
            binder.bind(ConfigurationFactory.class).toInstance(configurationFactory);

            FileSystemModule fileSystemModule = new FileSystemModule(catalogName, nodeManager, openTelemetry);
            fileSystemModule.setConfigurationFactory(configurationFactory);
            binder.install(fileSystemModule);

            Tracer tracer = openTelemetry.getTracer("warp.cloud-vendor");
            binder.bind(Tracer.class).toInstance(tracer);

            LifeCycleManager lifeCycleManager = Mockito.mock(LifeCycleManager.class);
            binder.bind(LifeCycleManager.class).toInstance(lifeCycleManager);
        }
    }
}