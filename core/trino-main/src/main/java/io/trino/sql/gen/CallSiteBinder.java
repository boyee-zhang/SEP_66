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
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.annotation.UsedByGeneratedCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantDynamic;

public final class CallSiteBinder
{
    private static final Method CONSTANT_BOOTSTRAP_METHOD;
    private static final Method LEGACY_BOOTSTRAP_METHOD;

    static {
        try {
            CONSTANT_BOOTSTRAP_METHOD = MethodHandles.class.getMethod("classDataAt", Lookup.class, String.class, Class.class, int.class);
            LEGACY_BOOTSTRAP_METHOD = CallSiteBinder.class.getMethod("legacyBootstrap", Lookup.class, String.class, Class.class, int.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private final boolean legacy;
    private int nextId;

    private final List<Object> bindingList = new ArrayList<>();
    private final Map<Long, MethodHandle> bindings = new HashMap<>();

    public CallSiteBinder()
    {
        this(true);
    }

    public CallSiteBinder(boolean legacy)
    {
        this.legacy = legacy;
    }

    public BytecodeExpression loadConstant(Object constant, Class<?> type)
    {
        int binding = bind(constant, type);
        return constantDynamic(
                legacy ? "constant_" + binding : "_",
                type,
                legacy ? LEGACY_BOOTSTRAP_METHOD : CONSTANT_BOOTSTRAP_METHOD,
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

    private int bind(Object constant, Class<?> type)
    {
        int bindingId = nextId++;

        verify(bindingId == bindingList.size());
        bindingList.add(constant);

        // DynamicClassLoader only supports MethodHandles, so wrapper constants in a method handle
        bindings.put((long) bindingId, MethodHandles.constant(type, constant));

        return bindingId;
    }

    public Map<Long, MethodHandle> getBindings()
    {
        checkState(legacy, "getBindings is only allowed in legacy mode");
        return ImmutableMap.copyOf(bindings);
    }

    public List<Object> getBindingList()
    {
        checkState(!legacy, "getBindings is only allowed in non-legacy mode");
        return ImmutableList.copyOf(bindingList);
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
    public static Object legacyBootstrap(MethodHandles.Lookup callerLookup, String name, Class<?> type, int bindingId)
    {
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
