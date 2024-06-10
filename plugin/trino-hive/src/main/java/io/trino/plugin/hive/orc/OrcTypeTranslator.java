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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.trino.orc.OrcColumn;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.hive.coercions.BooleanCoercer.BooleanToVarcharCoercer;
import io.trino.plugin.hive.coercions.BooleanCoercer.OrcVarcharToBooleanCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.ListCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.MapCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.StructCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.DateToVarcharCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.VarcharToDateCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToDateCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToLongTimestampCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToShortTimestampCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.coercions.VarcharToDoubleCoercer;
import io.trino.plugin.hive.coercions.VarcharToFloatCoercer;
import io.trino.plugin.hive.coercions.VarcharToIntegralNumericCoercers.OrcVarcharToIntegralNumericCoercer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BINARY;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BOOLEAN;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BYTE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DATE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DECIMAL;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DOUBLE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.FLOAT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LIST;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.MAP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.SHORT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRING;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.VARCHAR;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToInteger;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createIntegerNumberToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DoubleToVarcharCoercers.createDoubleToVarcharCoercer;
import static io.trino.plugin.hive.coercions.FloatToVarcharCoercers.createFloatToVarcharCoercer;
import static io.trino.plugin.hive.coercions.VarbinaryToVarcharCoercers.createVarbinaryToVarcharCoercer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(OrcType fromOrcType, List<OrcColumn> nestedColumns, Type toTrinoType)
    {
        OrcTypeKind fromOrcTypeKind = fromOrcType.getOrcTypeKind();
        if (fromOrcTypeKind == TIMESTAMP) {
            if (toTrinoType instanceof VarcharType varcharType) {
                return Optional.of(new LongTimestampToVarcharCoercer(TIMESTAMP_NANOS, varcharType));
            }
            if (toTrinoType instanceof DateType toDateType) {
                return Optional.of(new LongTimestampToDateCoercer(TIMESTAMP_NANOS, toDateType));
            }
            return Optional.empty();
        }
        if (fromOrcTypeKind == DATE && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(new DateToVarcharCoercer(varcharType));
        }
        if (isVarcharType(fromOrcTypeKind)) {
            if (toTrinoType instanceof BooleanType) {
                return Optional.of(new OrcVarcharToBooleanCoercer(createUnboundedVarcharType()));
            }
            if (toTrinoType instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return Optional.of(new VarcharToShortTimestampCoercer(createUnboundedVarcharType(), timestampType));
                }
                return Optional.of(new VarcharToLongTimestampCoercer(createUnboundedVarcharType(), timestampType));
            }
            if (toTrinoType instanceof DateType toDateType) {
                return Optional.of(new VarcharToDateCoercer(createUnboundedVarcharType(), toDateType));
            }
            if (toTrinoType instanceof RealType) {
                return Optional.of(new VarcharToFloatCoercer(createUnboundedVarcharType(), true));
            }
            if (toTrinoType instanceof DoubleType) {
                return Optional.of(new VarcharToDoubleCoercer(createUnboundedVarcharType(), true));
            }
            if (toTrinoType instanceof TinyintType tinyintType) {
                return Optional.of(new OrcVarcharToIntegralNumericCoercer<>(createUnboundedVarcharType(), tinyintType));
            }
            if (toTrinoType instanceof SmallintType smallintType) {
                return Optional.of(new OrcVarcharToIntegralNumericCoercer<>(createUnboundedVarcharType(), smallintType));
            }
            if (toTrinoType instanceof IntegerType integerType) {
                return Optional.of(new OrcVarcharToIntegralNumericCoercer<>(createUnboundedVarcharType(), integerType));
            }
            if (toTrinoType instanceof BigintType bigintType) {
                return Optional.of(new OrcVarcharToIntegralNumericCoercer<>(createUnboundedVarcharType(), bigintType));
            }
            return Optional.empty();
        }
        if (fromOrcTypeKind == FLOAT && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(createFloatToVarcharCoercer(varcharType, true));
        }
        if (fromOrcType.getOrcTypeKind() == FLOAT && toTrinoType instanceof DecimalType decimalType) {
            return Optional.of(createRealToDecimalCoercer(decimalType));
        }
        if (fromOrcTypeKind == DOUBLE && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(createDoubleToVarcharCoercer(varcharType, true));
        }
        if (fromOrcTypeKind == DOUBLE && toTrinoType instanceof DecimalType decimalType) {
            return Optional.of(createDoubleToDecimalCoercer(decimalType));
        }
        if (fromOrcType.getOrcTypeKind() == BOOLEAN && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(new BooleanToVarcharCoercer(varcharType));
        }
        if (toTrinoType instanceof DoubleType) {
            if (fromOrcTypeKind == BYTE) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(TINYINT));
            }
            if (fromOrcTypeKind == SHORT) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(SMALLINT));
            }
            if (fromOrcTypeKind == INT) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(INTEGER));
            }
            if (fromOrcTypeKind == LONG) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(BIGINT));
            }
        }
        if (toTrinoType instanceof DecimalType decimalType) {
            if (fromOrcTypeKind == BYTE) {
                return Optional.of(createIntegerNumberToDecimalCoercer(TINYINT, decimalType));
            }
            if (fromOrcTypeKind == SHORT) {
                return Optional.of(createIntegerNumberToDecimalCoercer(SMALLINT, decimalType));
            }
            if (fromOrcTypeKind == INT) {
                return Optional.of(createIntegerNumberToDecimalCoercer(INTEGER, decimalType));
            }
            if (fromOrcTypeKind == LONG) {
                return Optional.of(createIntegerNumberToDecimalCoercer(BIGINT, decimalType));
            }
        }
        if ((fromOrcTypeKind == BYTE || fromOrcTypeKind == SHORT || fromOrcTypeKind == INT || fromOrcTypeKind == LONG) && toTrinoType instanceof VarcharType varcharType) {
            Type fromType = switch (fromOrcTypeKind) {
                case BYTE -> TINYINT;
                case SHORT -> SMALLINT;
                case INT -> INTEGER;
                case LONG -> BIGINT;
                default -> throw new UnsupportedOperationException("Unsupported ORC type: " + fromOrcType);
            };
            return Optional.of(new IntegerNumberToVarcharCoercer<>(fromType, varcharType));
        }
        if (fromOrcTypeKind == DECIMAL && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(createDecimalToVarcharCoercer(
                    DecimalType.createDecimalType(fromOrcType.getPrecision().orElseThrow(), fromOrcType.getScale().orElseThrow()),
                    varcharType));
        }
        if (fromOrcType.getOrcTypeKind() == BYTE) {
            return switch (toTrinoType) {
                case SmallintType smallintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, smallintType));
                case IntegerType integerType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, integerType));
                case BigintType bigintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, bigintType));
                default -> Optional.empty();
            };
        }
        if (fromOrcType.getOrcTypeKind() == SHORT) {
            return switch (toTrinoType) {
                case IntegerType integerType -> Optional.of(new IntegerNumberUpscaleCoercer<>(SMALLINT, integerType));
                case BigintType bigintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(SMALLINT, bigintType));
                default -> Optional.empty();
            };
        }
        if (fromOrcType.getOrcTypeKind() == INT && toTrinoType instanceof BigintType) {
            return switch (toTrinoType) {
                case BigintType ignored -> Optional.of(new IntegerNumberUpscaleCoercer<>(SMALLINT, BIGINT));
                default -> Optional.empty();
            };
        }

        if (fromOrcType.getOrcTypeKind() == DECIMAL) {
            DecimalType decimalType = DecimalType.createDecimalType(fromOrcType.getPrecision().orElseThrow(), fromOrcType.getScale().orElseThrow());
            return switch (toTrinoType) {
                case TinyintType tinyintType -> Optional.of(createDecimalToInteger(decimalType, tinyintType));
                case SmallintType smallintType -> Optional.of(createDecimalToInteger(decimalType, smallintType));
                case IntegerType integerType -> Optional.of(createDecimalToInteger(decimalType, integerType));
                case BigintType bigintType -> Optional.of(createDecimalToInteger(decimalType, bigintType));
                case RealType ignored -> Optional.of(createDecimalToRealCoercer(decimalType));
                case DoubleType ignored -> Optional.of(createDecimalToDoubleCoercer(decimalType));
                case DecimalType toDecimalType -> Optional.of(createDecimalToDecimalCoercer(decimalType, toDecimalType));
                default -> Optional.empty();
            };
        }
        if (fromOrcTypeKind == BINARY && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(createVarbinaryToVarcharCoercer(varcharType, true));
        }
        if (fromOrcType.getOrcTypeKind() == STRUCT && toTrinoType instanceof RowType rowType) {
            ImmutableList.Builder<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers = ImmutableList.builder();
            ImmutableList.Builder<RowType.Field> fromField = ImmutableList.builder();
            ImmutableList.Builder<RowType.Field> toField = ImmutableList.builder();

            List<String> fromStructFieldName = fromOrcType.getFieldNames();
            List<String> toStructFieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            for (int i = 0; i < toStructFieldNames.size(); i++) {
                if (i >= fromStructFieldName.size()) {
                    toField.add(new RowType.Field(
                            Optional.of(toStructFieldNames.get(i)),
                            rowType.getFields().get(i).getType()));
                    coercers.add(Optional.empty());
                }
                else {
                    Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = createCoercer(nestedColumns.get(i).getColumnType(), nestedColumns.get(i).getNestedColumns(), rowType.getFields().get(i).getType());

                    RowType.Field field = rowType.getFields().get(i);

                    fromField.add(new RowType.Field(
                            Optional.of(fromStructFieldName.get(i)),
                            coercer.map(TypeCoercer::getFromType).orElseGet(field::getType)));
                    toField.add(new RowType.Field(
                            Optional.of(toStructFieldNames.get(i)),
                            coercer.map(TypeCoercer::getToType).orElseGet(field::getType)));

                    coercers.add(coercer);
                }
            }

            return Optional.of(new StructCoercer(RowType.from(fromField.build()), RowType.from(toField.build()), coercers.build()));
        }

        if (fromOrcType.getOrcTypeKind() == LIST && toTrinoType instanceof ArrayType arrayType) {
            return createCoercer(getOnlyElement(nestedColumns).getColumnType(), getOnlyElement(nestedColumns).getNestedColumns(), arrayType.getElementType())
                    .map(elementCoercer -> new ListCoercer(new ArrayType(elementCoercer.getFromType()), new ArrayType(elementCoercer.getToType()), elementCoercer));
        }

        if (fromOrcType.getOrcTypeKind() == MAP && toTrinoType instanceof MapType mapType) {
            Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer = createCoercer(nestedColumns.get(0).getColumnType(), nestedColumns.get(0).getNestedColumns(), mapType.getKeyType());
            Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer = createCoercer(nestedColumns.get(1).getColumnType(), nestedColumns.get(1).getNestedColumns(), mapType.getValueType());
            MapType fromType = new MapType(
                    keyCoercer.map(TypeCoercer::getFromType).orElseGet(mapType::getKeyType),
                    valueCoercer.map(TypeCoercer::getFromType).orElseGet(mapType::getValueType),
                    new TypeOperators());

            MapType toType = new MapType(
                    keyCoercer.map(TypeCoercer::getToType).orElseGet(mapType::getKeyType),
                    valueCoercer.map(TypeCoercer::getToType).orElseGet(mapType::getKeyType),
                    new TypeOperators());

            return Optional.of(new MapCoercer(fromType, toType, keyCoercer, valueCoercer));
        }
        return Optional.empty();
    }

    private static boolean isVarcharType(OrcTypeKind orcTypeKind)
    {
        return orcTypeKind == STRING || orcTypeKind == VARCHAR;
    }
}
