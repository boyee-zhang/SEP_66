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

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class DoubleLiteral
        extends Literal
{
    private final double value;

    public DoubleLiteral(String value)
    {
        this(Double.parseDouble(requireNonNull(value, "value is null")));
    }

    @JsonCreator
    public DoubleLiteral(
            @JsonProperty("value") double value)
    {
        this.value = value;
    }

    @JsonProperty
    public double getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitDoubleLiteral(this, context);
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

        DoubleLiteral that = (DoubleLiteral) o;

        if (Double.compare(that.value, value) != 0) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("UnaryPlus")
    @Override
    public int hashCode()
    {
        long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return value == ((DoubleLiteral) other).value;
    }
}
