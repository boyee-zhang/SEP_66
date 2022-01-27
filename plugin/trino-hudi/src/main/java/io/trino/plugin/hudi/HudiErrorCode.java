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

package io.trino.plugin.hudi;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum HudiErrorCode
        implements ErrorCodeSupplier
{
    HUDI_UNKNOWN_TABLE_TYPE(0, EXTERNAL),
    HUDI_INVALID_METADATA(1, EXTERNAL),
    HUDI_TOO_MANY_OPEN_PARTITIONS(2, USER_ERROR),
    HUDI_INVALID_PARTITION_VALUE(3, EXTERNAL),
    HUDI_BAD_DATA(4, EXTERNAL),
    HUDI_MISSING_DATA(5, EXTERNAL),
    HUDI_CANNOT_OPEN_SPLIT(6, EXTERNAL),
    HUDI_WRITER_OPEN_ERROR(7, EXTERNAL),
    HUDI_FILESYSTEM_ERROR(8, EXTERNAL),
    HUDI_CURSOR_ERROR(9, EXTERNAL),
    HUDI_WRITE_VALIDATION_FAILED(10, INTERNAL_ERROR),
    HUDI_INVALID_SNAPSHOT_ID(11, USER_ERROR);

    private final ErrorCode errorCode;

    HudiErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
