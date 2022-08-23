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
package io.trino.spi;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import io.trino.spi.connector.ConnectorContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.lang.ClassLoader.getPlatformClassLoader;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpiBackwardCompatibility
{
    private static final SetMultimap<String, String> BACKWARD_INCOMPATIBLE_CHANGES = ImmutableSetMultimap.<String, String>builder()
            // When updating this map, please try to remove backward incompatible changes for old versions.
            // Also consider mentioning backward incompatible changes in release notes.
            // We try to be backward compatible with at least the last released version.
            // example
            .put("123", "Class: public static class io.trino.spi.predicate.BenchmarkSortedRangeSet$Data")
            // example
            .put("123", "Constructor: public io.trino.spi.predicate.BenchmarkSortedRangeSet$Data()")
            // example
            .put("123", "Method: public void io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.init()")
            // example
            .put("123", "Field: public java.util.List<io.trino.spi.predicate.Range> io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.ranges")
            // changes
            .put("393", "Method: public io.trino.spi.connector.ConnectorBucketNodeMap io.trino.spi.connector.ConnectorNodePartitioningProvider.getBucketNodeMap(io.trino.spi.connector.ConnectorTransactionHandle,io.trino.spi.connector.ConnectorSession,io.trino.spi.connector.ConnectorPartitioningHandle)")
            .put("393", "Method: public java.util.function.ToIntFunction<io.trino.spi.connector.ConnectorSplit> io.trino.spi.connector.ConnectorNodePartitioningProvider.getSplitBucketFunction(io.trino.spi.connector.ConnectorTransactionHandle,io.trino.spi.connector.ConnectorSession,io.trino.spi.connector.ConnectorPartitioningHandle)")
            .put("394", "Method: public abstract io.trino.spi.exchange.Exchange io.trino.spi.exchange.ExchangeManager.createExchange(io.trino.spi.exchange.ExchangeContext,int)")
            .put("394", "Method: public abstract io.trino.spi.exchange.ExchangeSink io.trino.spi.exchange.ExchangeManager.createSink(io.trino.spi.exchange.ExchangeSinkInstanceHandle,boolean)")
            .build();

    @Test
    public void testSpiSingleVersionBackwardCompatibility()
            throws Exception
    {
        assertThat(getCurrentSpi()).containsAll(difference(getPreviousSpi(), getBackwardIncompatibleChanges()));
    }

    @Test
    public void testBackwardIncompatibleEntitiesAreInPreviousSpi()
            throws Exception
    {
        assertThat(getPreviousSpi()).containsAll(getBackwardIncompatibleChanges());
    }

    private static Set<String> getBackwardIncompatibleChanges()
    {
        String version = new ConnectorContext() {}.getSpiVersion().replace("-SNAPSHOT", "");
        return BACKWARD_INCOMPATIBLE_CHANGES.get(version);
    }

    private static Set<String> getCurrentSpi()
            throws IOException
    {
        return getSpiEntities(getSystemClassLoader(), true);
    }

    private static Set<String> getPreviousSpi()
            throws Exception
    {
        try (Stream<Path> list = Files.list(Path.of("target", "released-artifacts"))) {
            URL[] jars = list.map(TestSpiBackwardCompatibility::getUrl)
                    .toArray(URL[]::new);
            try (URLClassLoader urlClassLoader = new URLClassLoader(jars, getPlatformClassLoader())) {
                return getSpiEntities(urlClassLoader, false);
            }
        }
    }

    private static URL getUrl(Path path)
    {
        try {
            return path.toUri().toURL();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> getSpiEntities(ClassLoader classLoader, boolean includeDeprecated)
            throws IOException
    {
        ImmutableSet.Builder<String> entities = ImmutableSet.builder();
        for (ClassInfo classInfo : ClassPath.from(classLoader).getTopLevelClassesRecursive("io.trino.spi")) {
            Class<?> clazz = classInfo.load();
            addClassEntities(entities, clazz, includeDeprecated);
        }
        return entities.build().stream()
                // Ignore `final` so that we can e.g. remove final from a SPI method.
                // While adding `final` can be a breaking change, we currently ignore such breakages.
                .map(entity -> entity.replace(" final ", " "))
                .collect(toImmutableSet());
    }

    private static void addClassEntities(ImmutableSet.Builder<String> entities, Class<?> clazz, boolean includeDeprecated)
    {
        if (isExperimental(clazz, "class " + clazz.getName())) {
            return;
        }

        if (!isPublic(clazz.getModifiers())) {
            return;
        }

        // TODO remove this after Experimental is released
        if (isOriginalPtfClass(clazz, includeDeprecated)) {
            return;
        }

        for (Class<?> nestedClass : clazz.getDeclaredClasses()) {
            addClassEntities(entities, nestedClass, includeDeprecated);
        }
        if (!includeDeprecated && clazz.isAnnotationPresent(Deprecated.class)) {
            return;
        }
        entities.add("Class: " + clazz.toGenericString());
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (isExperimental(constructor, "constructor " + constructor)) {
                continue;
            }
            if (!includeDeprecated && constructor.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Constructor: " + constructor.toGenericString());
        }
        for (Method method : clazz.getMethods()) {
            if (isExperimental(method, "method " + method)) {
                continue;
            }
            if (!isPublic(method.getModifiers())) {
                continue;
            }
            if (!includeDeprecated && method.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Method: " + new MethodPrinter(clazz, method));
        }
        for (Field field : clazz.getDeclaredFields()) {
            if (isExperimental(field, "field " + field)) {
                continue;
            }
            if (!isPublic(field.getModifiers())) {
                continue;
            }
            if (!includeDeprecated && field.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Field: " + field.toGenericString());
        }
    }

    private static boolean isExperimental(AnnotatedElement element, String description)
    {
        if (!element.isAnnotationPresent(Experimental.class)) {
            return false;
        }

        // validate the annotation while we have access to the annotation
        String date = element.getAnnotation(Experimental.class).eta();
        try {
            LocalDate.parse(date);
        }
        catch (DateTimeParseException e) {
            throw new AssertionError(format("Invalid date '%s' in Experimental annotation on %s", date, description));
        }
        return true;
    }

    // TODO remove this after Experimental is released
    private static boolean isOriginalPtfClass(Class<?> clazz, boolean includeDeprecated)
    {
        return !includeDeprecated && clazz.getName().startsWith("io.trino.spi.ptf.");
    }

    private static class MethodPrinter
    {
        private static final int RELEVANT_METHOD_MODIFIERS = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.PRIVATE | Modifier.STATIC;
        private final Class<?> clazz;
        private final Method method;

        private MethodPrinter(Class<?> clazz, Method method)
        {
            this.clazz = clazz;
            this.method = method;
        }

        // Based on java.lang.reflect.Executable.sharedToGenericString
        // Two changes. Outputs only modifiers relevant to the compatibility check
        // and uses specified class name instead of class where the method was declared in.
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            printModifiersIfNonzero(sb);

            TypeVariable<?>[] typeparms = method.getTypeParameters();
            if (typeparms.length > 0) {
                sb.append(Arrays.stream(typeparms)
                        .map(MethodPrinter::typeVarBounds)
                        .collect(Collectors.joining(",", "<", "> ")));
            }

            specificToGenericStringHeader(sb);

            sb.append('(');
            StringJoiner sj = new StringJoiner(",");
            Type[] params = method.getGenericParameterTypes();
            for (int j = 0; j < params.length; j++) {
                String param = params[j].getTypeName();
                if (method.isVarArgs() && (j == params.length - 1)) { // replace T[] with T...
                    param = param.replaceFirst("\\[\\]$", "...");
                }
                sj.add(param);
            }
            sb.append(sj);
            sb.append(')');

            Type[] exceptionTypes = method.getGenericExceptionTypes();
            if (exceptionTypes.length > 0) {
                sb.append(Arrays.stream(exceptionTypes)
                        .map(Type::getTypeName)
                        .collect(Collectors.joining(",", " throws ", "")));
            }
            return sb.toString();
        }

        void specificToGenericStringHeader(StringBuilder sb)
        {
            Type genRetType = method.getGenericReturnType();
            sb.append(genRetType.getTypeName()).append(' ');
            sb.append(clazz.getTypeName()).append('.');
            sb.append(method.getName());
        }

        static String typeVarBounds(TypeVariable<?> typeVar)
        {
            Type[] bounds = typeVar.getBounds();
            if (bounds.length == 1 && bounds[0].equals(Object.class)) {
                return typeVar.getName();
            }
            else {
                return typeVar.getName() + " extends " +
                        Arrays.stream(bounds)
                                .map(Type::getTypeName)
                                .collect(Collectors.joining(" & "));
            }
        }

        void printModifiersIfNonzero(StringBuilder sb)
        {
            int mod = method.getModifiers() & RELEVANT_METHOD_MODIFIERS;

            if (mod != 0) {
                sb.append(Modifier.toString(mod)).append(' ');
            }
        }
    }
}
