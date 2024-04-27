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
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

public class CrcDoubleBlockAppender
        extends CrcBlockAppender
{
    public CrcDoubleBlockAppender(WriteJuffersWarmUpElement juffersWE)
    {
        super(juffersWE);
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    double val = blockPos.getDouble();
                    warmupElementStatsBuilder.updateMinMax(val);
                    writeValue(jufferPos, blockPos, val);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                double val = blockPos.getDouble();
                warmupElementStatsBuilder.updateMinMax(val);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    @Override
    protected AppendResult appendFromMapBlock(BlockPosHolder blockPos, int jufferPos, Object key)
    {
        int nullsCount = 0;
        Type valueType = ((MapType) blockPos.getType()).getValueType();
        for (; blockPos.inRange(); blockPos.advance()) {
            SqlMap elementBlock = (SqlMap) blockPos.getObject();
            int pos = elementBlock.seekKey(key);
            if (pos == -1 || elementBlock.getRawValueBlock().isNull(elementBlock.getRawOffset() + pos)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                double val = valueType.getDouble(elementBlock.getRawValueBlock(), elementBlock.getRawOffset() + pos);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(int jufferPos, BlockPosHolder blockPos, double val)
    {
        crcJuffers.put(val, jufferPos, blockPos);
        juffersWE.updateRecordBufferProps(val);
        advanceNullBuff();
    }
}
