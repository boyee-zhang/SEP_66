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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;
import io.trino.spi.block.Block;

import java.io.IOException;

import static io.trino.plugin.varada.storage.lucene.LuceneIndexer.LUCENE_NULL_STRING;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class LuceneArrayBlockAppender
        extends CrcBlockAppender
{
    private final LuceneIndexer luceneIndexer;

    public LuceneArrayBlockAppender(WriteJuffersWarmUpElement juffersWE,
            LuceneIndexer luceneIndexer)
    {
        super(juffersWE);
        this.luceneIndexer = luceneIndexer;
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        int nullsCount = 0;
        try {
            if (blockPos.mayHaveNull()) {
                while (blockPos.inRange()) {
                    if (blockPos.isNull()) {
                        nullsCount++;
                        nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                        luceneIndexer.addDoc(LUCENE_NULL_STRING);
                    }
                    else {
                        writeValue(blockPos);
                    }
                    blockPos.advance();
                }
            }
            else {
                while (blockPos.inRange()) {
                    writeValue(blockPos);
                    blockPos.advance();
                }
            }
        }
        catch (Exception e) {
            throw new WarmupException(
                    String.format("failed indexing value at position %d", blockPos.getPos()),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(BlockPosHolder blockPos)
            throws IOException
    {
        advanceNullBuff();
        Block value = (Block) blockPos.getObject();
        luceneIndexer.addDoc(getArrayValues(value));
    }

    private Slice[] getArrayValues(Block dataBlock)
    {
        int numberOfElements = dataBlock.getPositionCount();
        Slice[] values = new Slice[numberOfElements];
        for (int currInt = 0; currInt < numberOfElements; currInt++) {
            if (dataBlock.isNull(currInt)) {
                values[currInt] = LUCENE_NULL_STRING;
            }
            else {
                Slice slice = VARCHAR.getSlice(dataBlock, currInt);
                int sliceLength = slice.length();
                if (sliceLength > 0) {
                    values[currInt] = slice;
                }
                else {
                    values[currInt] = Slices.EMPTY_SLICE;
                }
            }
        }
        return values;
    }
}