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
package io.trino.parquet.crypto;

import org.apache.parquet.ParquetRuntimeException;

import java.util.Arrays;

public class HiddenColumnException
        extends ParquetRuntimeException
{
    private static final long serialVersionUID = 1L;

    public HiddenColumnException(String[] columnPath, String filePath)
    {
        super("User does not have access to the encryption key for encrypted column = " + Arrays.toString(columnPath) + " for file: " + filePath);
    }
}
