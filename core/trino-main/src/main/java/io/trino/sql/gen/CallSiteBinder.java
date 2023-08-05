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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.annotation.UsedByGeneratedCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantDynamic;
import static java.util.Objects.requireNonNull;

public final class CallSiteBinder
{
    private static final Method BOOTSTRAP_METHOD;

    static {
        try {
            BOOTSTRAP_METHOD = CallSiteBinder.class.getMethod("bootstrap", Lookup.class, String.class, Class.class, int.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private int nextId;

    private final List<Object> bindings = new ArrayList<>();

    public BytecodeExpression loadConstant(Object constant, Class<?> type)
    {
        int binding = bind(constant);
        return constantDynamic(
                "_",
                type,
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding));
    }

    public BytecodeExpression invoke(MethodHandle method, String name, BytecodeExpression... parameters)
    {
        return invoke(method, name, ImmutableList.copyOf(parameters));
    }

    public BytecodeExpression invoke(MethodHandle method, String name, List<? extends BytecodeExpression> parameters)
    {
        checkArgument(
                method.type().parameterCount() == parameters.size(),
                "Method requires %s parameters, but only %s supplied",
                method.type().parameterCount(),
                parameters.size());

        return loadConstant(method, MethodHandle.class).invoke("invoke", method.type().returnType(), parameters);
    }

    private int bind(Object constant)
    {
        requireNonNull(constant, "constant is null");

        int bindingId = nextId++;

        verify(bindingId == bindings.size());
        bindings.add(constant);

        return bindingId;
    }

    public List<Object> getBindings()
    {
        return ImmutableList.copyOf(bindings);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }

    @UsedByGeneratedCode
    public static Object bootstrap(MethodHandles.Lookup callerLookup, String name, Class<?> type, int bindingId)
    {
        try {
            Object data = MethodHandles.classDataAt(callerLookup, name, type, bindingId);
            if (data != null) {
                return data;
            }
        }
        catch (IllegalAccessException ignored) {
        }

        ClassLoader classLoader = callerLookup.lookupClass().getClassLoader();
        checkArgument(classLoader instanceof DynamicClassLoader, "Expected %s's classloader to be of type %s", callerLookup.lookupClass().getName(), DynamicClassLoader.class.getName());

        DynamicClassLoader dynamicClassLoader = (DynamicClassLoader) classLoader;
        MethodHandle target = dynamicClassLoader.getCallSiteBindings().get((long) bindingId);
        checkArgument(target != null, "Binding %s for constant %s with type %s not found", bindingId, name, type);

        try {
            return target.invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Error loading value for constant %s with type %s not found".formatted(name, type));
        }
    }
}
