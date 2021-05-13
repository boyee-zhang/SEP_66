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
package io.trino.plugin.hive;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.MetadataSource;
import io.trino.parquet.reader.cache.CachingMetadataSource;
import io.trino.parquet.reader.cache.MetadataCacheConfig;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.plugin.hive.s3select.S3SelectRecordCursorProvider;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(SINGLETON);
        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);

        binder.bind(HiveSessionProperties.class).in(SINGLETON);
        binder.bind(HiveTableProperties.class).in(SINGLETON);
        binder.bind(HiveAnalyzeProperties.class).in(SINGLETON);

        binder.bind(TrinoS3ClientFactory.class).in(SINGLETON);

        binder.bind(CachingDirectoryLister.class).in(SINGLETON);
        newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();

        binder.bind(HiveWriterStats.class).in(SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).withGeneratedName();

        newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class).in(SINGLETON);
        binder.bind(HivePartitionManager.class).in(SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(SINGLETON);
        newOptionalBinder(binder, HiveRedirectionsProvider.class)
                .setDefault().to(NoneHiveRedirectionsProvider.class).in(SINGLETON);
        newOptionalBinder(binder, HiveMaterializedViewMetadata.class)
                .setDefault().to(NoneHiveMaterializedViewMetadata.class).in(SINGLETON);
        newOptionalBinder(binder, TransactionalMetadataFactory.class)
                .setDefault().to(HiveMetadataFactory.class).in(SINGLETON);
        binder.bind(HiveTransactionManager.class).in(SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitManager.class).in(SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generator -> generator.generatedNameOf(HiveSplitManager.class));
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class).in(SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

        binder.bind(FileFormatDataSourceStats.class).in(SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(SINGLETON);

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(S3SelectRecordCursorProvider.class).in(SINGLETON);

        binder.bind(GenericHiveRecordCursorProvider.class).in(SINGLETON);

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        binder.bind(OrcFileWriterFactory.class).in(SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).withGeneratedName();
        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(ParquetFileWriterFactory.class).in(SINGLETON);
        configBinder(binder).bindConfig(MetadataCacheConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);

        newSetBinder(binder, SystemTable.class);
    }

    @Singleton
    @Provides
    public MetadataSource createMetadataSource(MetadataCacheConfig config, MBeanExporter exporter, CatalogName catalogName)
    {
        CachingMetadataSource metadataSource = new CachingMetadataSource(config, new MetadataReader());
        exporter.exportWithGeneratedName(metadataSource, CachingMetadataSource.class, catalogName.toString());
        return metadataSource;
    }

    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("hive-" + catalogName + "-%s"));
    }

    @ForHiveTransactionHeartbeats
    @Singleton
    @Provides
    public ScheduledExecutorService createHiveTransactionHeartbeatExecutor(CatalogName catalogName, HiveConfig hiveConfig)
    {
        return newScheduledThreadPool(
                hiveConfig.getHiveTransactionHeartbeatThreads(),
                daemonThreadsNamed("hive-heartbeat-" + catalogName + "-%s"));
    }

    @Singleton
    @Provides
    public Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> createMetastoreGetter(HiveTransactionManager transactionManager)
    {
        return transactionHandle -> ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore();
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(TypeId.of(value));
        }
    }
}
