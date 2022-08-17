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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class LikePredicate
        extends Expression
{
    private final Expression value;
    private final Expression pattern;
    private final Optional<Expression> escape;

    public LikePredicate(Expression value, Expression pattern, Expression escape)
    {
        this(value, pattern, Optional.of(escape));
    }

    @JsonCreator
    public LikePredicate(
            @JsonProperty("value") Expression value,
            @JsonProperty("pattern") Expression pattern,
            @JsonProperty("escape") Optional<Expression> escape)
    {
        requireNonNull(value, "value is null");
        requireNonNull(pattern, "pattern is null");
        requireNonNull(escape, "escape is null");

        this.value = value;
        this.pattern = pattern;
        this.escape = escape;
    }

    @JsonProperty
    public Expression getValue()
    {
        return value;
    }

    @JsonProperty
    public Expression getPattern()
    {
        return pattern;
    }

    @JsonProperty
    public Optional<Expression> getEscape()
    {
        return escape;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> result = ImmutableList.<Node>builder()
                .add(value)
                .add(pattern);

        escape.ifPresent(result::add);

        return result.build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LikePredicate that = (LikePredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(pattern, that.pattern) &&
                Objects.equals(escape, that.escape);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, pattern, escape);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
