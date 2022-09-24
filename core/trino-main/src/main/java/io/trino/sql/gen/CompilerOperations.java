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

import io.trino.spi.block.Block;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BooleanType.BOOLEAN;

// These methods are statically bound by the compiler
@SuppressWarnings("UnusedDeclaration")
public final class CompilerOperations
{
    private CompilerOperations() {}

    public static boolean longGreaterThanZero(long value)
    {
        return value > 0;
    }

    public static boolean and(boolean left, boolean right)
    {
        return left && right;
    }

    public static boolean or(boolean left, boolean right)
    {
        return left || right;
    }

    public static boolean not(boolean value)
    {
        return !value;
    }

    public static boolean lessThan(int left, int right)
    {
        return left < right;
    }

    public static boolean greaterThan(int left, int right)
    {
        return left > right;
    }

    public static boolean testMask(@Nullable Block masks, int index)
    {
        if (masks != null) {
            if (masks.isNull(index)) {
                return false;
            }
            return BOOLEAN.getBoolean(masks, index);
        }
        return true;
    }

    public static int optionalChannelToIntOrNegative(Optional<Integer> channel)
    {
        return channel.orElse(-1);
    }

    public static void validateChannelsListLength(List<Integer> channels, int requiredSize)
    {
        int channelsSize = channels.size();
        // empty channels is allowed for intermediate aggregations
        checkArgument(channelsSize == 0 || requiredSize == 0 || channelsSize == requiredSize, "Invalid channels length, expected %s but found: %s", requiredSize, channelsSize);
    }
}
