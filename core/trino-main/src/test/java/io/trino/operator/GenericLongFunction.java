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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;

import java.lang.invoke.MethodHandle;
import java.util.function.LongUnaryOperator;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public final class GenericLongFunction
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(GenericLongFunction.class, "apply", LongUnaryOperator.class, long.class);

    private final LongUnaryOperator longUnaryOperator;

    GenericLongFunction(String suffix, LongUnaryOperator longUnaryOperator)
    {
        super(new FunctionMetadata(
                new Signature(
                        "generic_long_" + requireNonNull(suffix, "suffix is null"),
                        BIGINT.getTypeSignature(),
                        BIGINT.getTypeSignature()),
                new FunctionNullability(false, ImmutableList.of(false)),
                true,
                true,
                "generic long function for test",
                SCALAR));
        this.longUnaryOperator = longUnaryOperator;
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(longUnaryOperator);
        return new ChoicesScalarFunctionImplementation(functionBinding, FAIL_ON_NULL, ImmutableList.of(NEVER_NULL), methodHandle);
    }

    public static long apply(LongUnaryOperator longUnaryOperator, long value)
    {
        return longUnaryOperator.applyAsLong(value);
    }
}
