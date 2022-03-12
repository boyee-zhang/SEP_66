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
package io.trino.tracing;

import com.google.inject.Inject;
import io.trino.spi.tracing.NoopTracer;
import io.trino.spi.tracing.Tracer;
import io.trino.spi.tracing.TracerProvider;

public class NoopTracerProvider
        implements TracerProvider
{
    public static final NoopTracerProvider NOOP_TRACER_PROVIDER = new NoopTracerProvider();
    public static final NoopTracer NOOP_TRACER = new NoopTracer();

    @Inject
    public NoopTracerProvider() {}

    @Override
    public Tracer getNewTracer()
    {
        return NOOP_TRACER;
    }
}
