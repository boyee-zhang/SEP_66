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
package io.trino.plugin.varada.storage.write.appenders;

import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStatsBuilder;

import java.nio.IntBuffer;

public class RealBlockAppender
        extends DataBlockAppender
{
    public RealBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        IntBuffer buff = (IntBuffer) juffersWE.getRecordBuffer();

        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    buff.put(0);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    int val = blockPos.getInt();
                    Float floatVal = Float.intBitsToFloat(val);
                    warmupElementStatsBuilder.updateMinMax(floatVal);
                    writeValue(val, buff);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                int val = blockPos.getInt();
                Float floatVal = Float.intBitsToFloat(val);
                warmupElementStatsBuilder.updateMinMax(floatVal);
                writeValue(val, buff);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(int val, IntBuffer buff)
    {
        juffersWE.updateRecordBufferProps(Float.intBitsToFloat(val));
        buff.put(val);
        advanceNullBuff();
    }
}
