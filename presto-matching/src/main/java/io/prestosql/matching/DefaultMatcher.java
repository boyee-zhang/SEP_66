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
package io.prestosql.matching;

import io.prestosql.matching.pattern.CapturePattern;
import io.prestosql.matching.pattern.EqualsPattern;
import io.prestosql.matching.pattern.FilterPattern;
import io.prestosql.matching.pattern.TypeOfPattern;
import io.prestosql.matching.pattern.WithPattern;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class DefaultMatcher
        implements Matcher
{
    public static final Matcher DEFAULT_MATCHER = new DefaultMatcher();

    @Override
    public <T> Stream<Match<T>> match(Pattern<T> pattern, Object object, Captures captures)
    {
        if (pattern.previous().isPresent()) {
            return match(pattern.previous().get(), object, captures)
                    .flatMap(match -> pattern.accept(this, match.value(), match.captures()));
        }
        else {
            return pattern.accept(this, object, captures);
        }
    }

    @Override
    public <T> Stream<Match<T>> matchTypeOf(TypeOfPattern<T> typeOfPattern, Object object, Captures captures)
    {
        Class<T> expectedClass = typeOfPattern.expectedClass();
        if (expectedClass.isInstance(object)) {
            return Stream.of(Match.of(expectedClass.cast(object), captures));
        }
        return Stream.of();
    }

    @Override
    public <T> Stream<Match<T>> matchWith(WithPattern<T> withPattern, Object object, Captures captures)
    {
        Function<? super T, Optional<?>> property = withPattern.getProperty().getFunction();
        Optional<?> propertyValue = property.apply((T) object);
        return propertyValue.map(value -> match(withPattern.getPattern(), value, captures))
                .map(matchStream -> matchStream.map(match -> Match.of((T) object, match.captures())))
                .orElse(Stream.of());
    }

    @Override
    public <T> Stream<Match<T>> matchCapture(CapturePattern<T> capturePattern, Object object, Captures captures)
    {
        Captures newCaptures = captures.addAll(Captures.ofNullable(capturePattern.capture(), (T) object));
        return Stream.of(Match.of((T) object, newCaptures));
    }

    @Override
    public <T> Stream<Match<T>> matchEquals(EqualsPattern<T> equalsPattern, Object object, Captures captures)
    {
        return Stream.of(Match.of((T) object, captures))
                .filter(match -> equalsPattern.expectedValue().equals(match.value()));
    }

    @Override
    public <T> Stream<Match<T>> matchFilter(FilterPattern<T> filterPattern, Object object, Captures captures)
    {
        return Stream.of(Match.of((T) object, captures))
            .filter(match -> filterPattern.predicate().test(match.value()));
    }
}
