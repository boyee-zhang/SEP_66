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
package io.trino.spi.connector;

public final class TableProcedureExecutionMode
{
    private final boolean readsData;
    private final boolean supportsFilter;
    private final boolean supportsSorting;

    public TableProcedureExecutionMode(boolean readsData, boolean supportsFilter, boolean supportsSorting)
    {
        this.readsData = readsData;

        if (!readsData) {
            // TODO currently only table procedures which process data are supported
            // this is temporary check to be dropped when execution flow will be added for
            // table procedures which do not read data
            throw new IllegalArgumentException("repartitioning not supported if table data is not processed");
        }

        if (!readsData) {
            if (supportsFilter) {
                throw new IllegalArgumentException("filtering not supported if table data is not processed");
            }
            if (supportsSorting) {
                throw new IllegalArgumentException("sorting not supported if table data is not processed");
            }
        }

        this.supportsFilter = supportsFilter;
        this.supportsSorting = supportsSorting;
    }

    public boolean isReadsData()
    {
        return readsData;
    }

    public boolean supportsFilter()
    {
        return supportsFilter;
    }

    public boolean supportsSorting()
    {
        return supportsSorting;
    }

    public static TableProcedureExecutionMode coordinatorOnly()
    {
        return new TableProcedureExecutionMode(false, false, false);
    }

    public static TableProcedureExecutionMode distributedWithFilteringAndRepartitioning()
    {
        return new TableProcedureExecutionMode(true, true, false);
    }
}
