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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;

class BigQueryUtil
{
    private BigQueryUtil() {}

    static boolean isRetryable(Throwable cause)
    {
        Throwable c = cause;
        while (c != null) {
            if (isRetryableInternalError(c)) {
                return true;
            }
            c = c.getCause();
        }
        // failed
        return false;
    }

    static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES = ImmutableSet.of(
            "HTTP/2 error code: INTERNAL_ERROR",
            "Connection closed with unknown cause",
            "Received unexpected EOS on DATA frame from server");

    static boolean isRetryableInternalError(Throwable t)
    {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException sse = (StatusRuntimeException) t;
            return sse.getStatus().getCode() == Status.Code.INTERNAL &&
                    INTERNAL_ERROR_MESSAGES.stream()
                            .anyMatch(errorMsg -> sse.getMessage().contains(errorMsg));
        }
        else {
            return false;
        }
    }

    static void convertAndThrow(BigQueryError error)
    {
        throw new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
    }
}
