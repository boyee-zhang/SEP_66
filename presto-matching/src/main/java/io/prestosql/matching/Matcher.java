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

import java.util.stream.Stream;

public interface Matcher
{
    default <T> Stream<Match<T>> match(Pattern<T> pattern, Object object)
    {
        return match(pattern, object, Captures.empty(), noContext());
    }

    static Void noContext()
    {
        return null;
    }

    default <T, C> Stream<Match<T>> match(Pattern<T> pattern, Object object, C context)
    {
        return match(pattern, object, Captures.empty(), context);
    }

    <T, C> Stream<Match<T>> match(Pattern<T> pattern, Object object, Captures captures, C context);

    <T, C> Stream<Match<T>> matchTypeOf(TypeOfPattern<T> typeOfPattern, Object object, Captures captures, C context);

    <T, C> Stream<Match<T>> matchWith(WithPattern<T> withPattern, Object object, Captures captures, C context);

    <T, C> Stream<Match<T>> matchCapture(CapturePattern<T> capturePattern, Object object, Captures captures, C context);

    <T, C> Stream<Match<T>> matchEquals(EqualsPattern<T> equalsPattern, Object object, Captures captures, C context);

    <T, C> Stream<Match<T>> matchFilter(FilterPattern<T> filterPattern, Object object, Captures captures, C context);
}
