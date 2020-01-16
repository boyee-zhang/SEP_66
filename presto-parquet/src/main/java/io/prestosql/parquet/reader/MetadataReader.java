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
package io.prestosql.parquet.reader;

import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.parquet.ParquetValidationUtils.validateParquet;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.parquet.format.Util.readFileMetaData;

public final class MetadataReader
{
    private static final int PARQUET_METADATA_LENGTH = 4;
    private static final byte[] MAGIC = "PAR1".getBytes(US_ASCII);
    private static final ParquetMetadataConverter PARQUET_METADATA_CONVERTER = new ParquetMetadataConverter();
    //  Size of a no-data, no metadata empty parquet file
    private static final int MINIMUM_VALID_PARQUET_FILE_SIZE = MAGIC.length + PARQUET_METADATA_LENGTH + MAGIC.length;
    //  Prefetch at most this much data from the end of the file
    private static final int PARQUET_FOOTER_INITIAL_PREFETCH_LENGTH = 8 * 1024;

    private MetadataReader() {}

    public static int footerInitialPrefetchLength(Path file, long fileSize)
            throws IOException
    {
        validateParquet(fileSize >= MINIMUM_VALID_PARQUET_FILE_SIZE, "%s is not a valid Parquet File", file);
        return toIntExact(min(fileSize, PARQUET_FOOTER_INITIAL_PREFETCH_LENGTH));
    }

    private static Slice ensureTailSliceSize(FSDataInputStream inputStream, long fileSize, Slice tailSlice, int newSize)
            throws IOException
    {
        if (tailSlice.length() >= newSize) {
            return tailSlice;
        }
        checkArgument(newSize <= fileSize, "newSize %s is larger than file size %s", newSize, fileSize);
        byte[] buffer = new byte[newSize - tailSlice.length()];
        inputStream.readFully(fileSize - newSize, buffer);
        Slice result = Slices.allocate(newSize);
        result.setBytes(0, buffer);
        result.setBytes(buffer.length, tailSlice);
        return result;
    }

    private static InputStream readFooterToMetadataStream(FSDataInputStream inputStream, Path file, long fileSize, Slice tailSlice)
            throws IOException
    {
        // Parquet File Layout:
        //
        // MAGIC
        // variable: Data
        // variable: Metadata
        // 4 bytes: MetadataLength
        // MAGIC

        validateParquet(fileSize >= MINIMUM_VALID_PARQUET_FILE_SIZE, "%s is not a valid Parquet File", file);
        // Ensure the tail has enough data for length and magic section
        tailSlice = ensureTailSliceSize(inputStream, fileSize, tailSlice, MAGIC.length + PARQUET_METADATA_LENGTH);

        byte[] magic = tailSlice.getBytes(tailSlice.length() - MAGIC.length, MAGIC.length);
        validateParquet(Arrays.equals(MAGIC, magic), "Not valid Parquet file: %s expected magic number: %s got: %s", file, Arrays.toString(MAGIC), Arrays.toString(magic));

        int metadataLength = bytesAsLittleEndianInt(tailSlice.getBytes(tailSlice.length() - (PARQUET_METADATA_LENGTH + MAGIC.length), PARQUET_METADATA_LENGTH));
        int combinedTrailerSize = metadataLength + MAGIC.length + PARQUET_METADATA_LENGTH;
        long metadataFileOffset = fileSize - combinedTrailerSize;
        validateParquet(metadataFileOffset >= MAGIC.length && metadataFileOffset + MINIMUM_VALID_PARQUET_FILE_SIZE < fileSize,
                "Corrupted Parquet file: %s metadata index: %s out of range",
                file,
                metadataFileOffset);

        // Read any metadata bytes not already in the slice buffer
        tailSlice = ensureTailSliceSize(inputStream, fileSize, tailSlice, combinedTrailerSize);

        return new ByteArrayInputStream(tailSlice.getBytes(tailSlice.length() - combinedTrailerSize, metadataLength));
    }

    public static ParquetMetadata readFooter(FSDataInputStream inputStream, Path file, long fileSize, Slice tailSlice)
            throws IOException
    {
        InputStream metadataStream = readFooterToMetadataStream(inputStream, file, fileSize, tailSlice);
        FileMetaData fileMetaData = readFileMetaData(metadataStream);
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), "Empty Parquet schema in file: %s", file);

        MessageType messageType = readParquetSchema(schema);
        List<BlockMetaData> blocks = new ArrayList<>();
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups != null) {
            for (RowGroup rowGroup : rowGroups) {
                BlockMetaData blockMetaData = new BlockMetaData();
                blockMetaData.setRowCount(rowGroup.getNum_rows());
                blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), "No columns in row group: %s", rowGroup);
                String filePath = columns.get(0).getFile_path();
                for (ColumnChunk columnChunk : columns) {
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            "all column chunks of the same row group must be in the same file");
                    ColumnMetaData metaData = columnChunk.meta_data;
                    String[] path = metaData.path_in_schema.stream()
                            .map(value -> value.toLowerCase(Locale.ENGLISH))
                            .toArray(String[]::new);
                    ColumnPath columnPath = ColumnPath.get(path);
                    PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
                    ColumnChunkMetaData column = ColumnChunkMetaData.get(
                            columnPath,
                            primitiveType,
                            CompressionCodecName.fromParquet(metaData.codec),
                            PARQUET_METADATA_CONVERTER.convertEncodingStats(metaData.encoding_stats),
                            readEncodings(metaData.encodings),
                            readStats(Optional.ofNullable(fileMetaData.getCreated_by()), Optional.ofNullable(metaData.statistics), primitiveType),
                            metaData.data_page_offset,
                            metaData.dictionary_page_offset,
                            metaData.num_values,
                            metaData.total_compressed_size,
                            metaData.total_uncompressed_size);
                    blockMetaData.addColumn(column);
                }
                blockMetaData.setPath(filePath);
                blocks.add(blockMetaData);
            }
        }

        Map<String, String> keyValueMetaData = new HashMap<>();
        List<KeyValue> keyValueList = fileMetaData.getKey_value_metadata();
        if (keyValueList != null) {
            for (KeyValue keyValue : keyValueList) {
                keyValueMetaData.put(keyValue.key, keyValue.value);
            }
        }
        return new ParquetMetadata(new org.apache.parquet.hadoop.metadata.FileMetaData(messageType, keyValueMetaData, fileMetaData.getCreated_by()), blocks);
    }

    private static MessageType readParquetSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getTypeName(element.type), Repetition.valueOf(element.repetition_type.name()));
                if (element.isSetType_length()) {
                    primitiveBuilder.length(element.type_length);
                }
                if (element.isSetPrecision()) {
                    primitiveBuilder.precision(element.precision);
                }
                if (element.isSetScale()) {
                    primitiveBuilder.scale(element.scale);
                }
                typeBuilder = primitiveBuilder;
            }

            if (element.isSetConverted_type()) {
                typeBuilder.as(getOriginalType(element.converted_type));
            }
            if (element.isSetField_id()) {
                typeBuilder.id(element.field_id);
            }
            typeBuilder.named(element.name.toLowerCase(Locale.ENGLISH));
        }
    }

    public static org.apache.parquet.column.statistics.Statistics<?> readStats(Optional<String> fileCreatedBy, Optional<Statistics> statisticsFromFile, PrimitiveType type)
    {
        Statistics statistics = statisticsFromFile.orElse(null);
        org.apache.parquet.column.statistics.Statistics<?> columnStatistics = new ParquetMetadataConverter().fromParquetStatistics(fileCreatedBy.orElse(null), statistics, type);

        if (type.getOriginalType() == OriginalType.UTF8
                && statistics != null
                && !statistics.isSetMin_value() && !statistics.isSetMax_value() // the min,max fields used for UTF8 since Parquet PARQUET-1025
                && statistics.isSetMin() && statistics.isSetMax()  // the min,max fields used for UTF8 before Parquet PARQUET-1025
                && columnStatistics.genericGetMin() == null && columnStatistics.genericGetMax() == null
                && !CorruptStatistics.shouldIgnoreStatistics(fileCreatedBy.orElse(null), type.getPrimitiveTypeName())) {
            tryReadOldUtf8Stats(statistics, (BinaryStatistics) columnStatistics);
        }

        return columnStatistics;
    }

    private static void tryReadOldUtf8Stats(Statistics statistics, BinaryStatistics columnStatistics)
    {
        byte[] min = statistics.getMin();
        byte[] max = statistics.getMax();

        if (Arrays.equals(min, max)) {
            // If min=max, then there is single value only
            min = min.clone();
            max = min;
        }
        else {
            // For min it's enough to retain leading all-ASCII, because this produces a strictly lower value.
            int minFirstNonAsciiOffset = firstOutsideRange(min, 0, 128);
            min = Arrays.copyOf(min, minFirstNonAsciiOffset);

            // For max we chop away everything at the first non-ASCII, then increment last character.
            int maxFirstBadCharacter = firstOutsideRange(max, 0, 127); // last ASCII is also bad because we can't increment it
            if (maxFirstBadCharacter == 0) {
                // We can't help.
                return;
            }
            max[maxFirstBadCharacter - 1]++;
            max = Arrays.copyOf(max, maxFirstBadCharacter);
        }

        columnStatistics.setMinMaxFromBytes(min, max);
        if (!columnStatistics.isNumNullsSet() && statistics.isSetNull_count()) {
            columnStatistics.setNumNulls(statistics.getNull_count());
        }
    }

    private static int firstOutsideRange(byte[] bytes, int rangeStartInclusive, int rangeEndExclusive)
    {
        int offset = 0;
        while (offset < bytes.length && rangeStartInclusive <= bytes[offset] && bytes[offset] < rangeEndExclusive) {
            offset++;
        }
        return offset;
    }

    private static Set<org.apache.parquet.column.Encoding> readEncodings(List<Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (Encoding encoding : encodings) {
            columnEncodings.add(org.apache.parquet.column.Encoding.valueOf(encoding.name()));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static PrimitiveTypeName getTypeName(Type type)
    {
        switch (type) {
            case BYTE_ARRAY:
                return PrimitiveTypeName.BINARY;
            case INT64:
                return PrimitiveTypeName.INT64;
            case INT32:
                return PrimitiveTypeName.INT32;
            case BOOLEAN:
                return PrimitiveTypeName.BOOLEAN;
            case FLOAT:
                return PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveTypeName.DOUBLE;
            case INT96:
                return PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private static OriginalType getOriginalType(ConvertedType type)
    {
        switch (type) {
            case UTF8:
                return OriginalType.UTF8;
            case MAP:
                return OriginalType.MAP;
            case MAP_KEY_VALUE:
                return OriginalType.MAP_KEY_VALUE;
            case LIST:
                return OriginalType.LIST;
            case ENUM:
                return OriginalType.ENUM;
            case DECIMAL:
                return OriginalType.DECIMAL;
            case DATE:
                return OriginalType.DATE;
            case TIME_MILLIS:
                return OriginalType.TIME_MILLIS;
            case TIMESTAMP_MILLIS:
                return OriginalType.TIMESTAMP_MILLIS;
            case INTERVAL:
                return OriginalType.INTERVAL;
            case INT_8:
                return OriginalType.INT_8;
            case INT_16:
                return OriginalType.INT_16;
            case INT_32:
                return OriginalType.INT_32;
            case INT_64:
                return OriginalType.INT_64;
            case UINT_8:
                return OriginalType.UINT_8;
            case UINT_16:
                return OriginalType.UINT_16;
            case UINT_32:
                return OriginalType.UINT_32;
            case UINT_64:
                return OriginalType.UINT_64;
            case JSON:
                return OriginalType.JSON;
            case BSON:
                return OriginalType.BSON;
            case TIMESTAMP_MICROS:
                return OriginalType.TIMESTAMP_MICROS;
            case TIME_MICROS:
                return OriginalType.TIME_MICROS;
            default:
                throw new IllegalArgumentException("Unknown converted type " + type);
        }
    }

    private static int bytesAsLittleEndianInt(byte[] buffer)
    {
        checkArgument(buffer.length == 4, "buffer must be 4 bytes long");
        return Ints.fromBytes(buffer[3], buffer[2], buffer[1], buffer[0]);
    }
}
