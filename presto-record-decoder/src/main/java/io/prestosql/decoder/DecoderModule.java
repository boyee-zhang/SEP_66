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
package io.prestosql.decoder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.decoder.avro.AvroDecoderModule;
import io.prestosql.decoder.csv.CsvRowDecoder;
import io.prestosql.decoder.csv.CsvRowDecoderFactory;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.decoder.dummy.DummyRowDecoderFactory;
import io.prestosql.decoder.json.JsonRowDecoder;
import io.prestosql.decoder.json.JsonRowDecoderFactory;
import io.prestosql.decoder.proto.ProtobufRowDecoder;
import io.prestosql.decoder.proto.ProtobufRowDecoderFactory;
import io.prestosql.decoder.raw.RawRowDecoder;
import io.prestosql.decoder.raw.RawRowDecoderFactory;

import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

/**
 * Default decoder module. Installs the registry and all known decoder submodules.
 */
public class DecoderModule
        implements Module
{
    private final Module extension;

    public DecoderModule()
    {
        this(new AvroDecoderModule());
    }

    public DecoderModule(Module extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
    }

    @Override
    public void configure(Binder binder)
    {
        MapBinder<String, RowDecoderFactory> decoderFactoriesByName = MapBinder.newMapBinder(binder, String.class, RowDecoderFactory.class);
        decoderFactoriesByName.addBinding(ProtobufRowDecoder.NAME).to(ProtobufRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(CsvRowDecoder.NAME).to(CsvRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(JsonRowDecoder.NAME).to(JsonRowDecoderFactory.class).in(SINGLETON);
        decoderFactoriesByName.addBinding(RawRowDecoder.NAME).to(RawRowDecoderFactory.class).in(SINGLETON);
        binder.install(extension);
        binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
    }
}
