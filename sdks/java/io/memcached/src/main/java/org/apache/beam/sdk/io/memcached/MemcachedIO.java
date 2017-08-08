/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.memcached;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.auto.value.AutoValue;

import java.util.List;

import javax.annotation.Nullable;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * IO to read and write data on Memcached key-store.
 *
 */
@Experimental
public class MemcachedIO {
  /**
   * Read data from a JDBC datasource.
   *
   */
  public static Read read() {
    return new AutoValue_MemcachedIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_MemcachedIO_Write.Builder().build();
  }


  private MemcachedIO() {}

  /** A {@link PTransform} to read data from a memcached key-value store. */
  @AutoValue
  public abstract static class Read extends
  PTransform<PCollection<String>, PCollection<KV<String, Object>>> {
    @Nullable abstract ConnectionFactory getConnectionFactory();
    @Nullable abstract List<String> getAddresses();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionFactory(ConnectionFactory connFactory);
      abstract Builder setAddresses(List<String> addresses);
      abstract Read build();
    }

    public Read withConnectionFactory(ConnectionFactory connFactory) {
      checkArgument(connFactory != null,
          "MemcachedIO.read().withConnectionFactory(connFactory) called with null connFactory");
      return toBuilder().setConnectionFactory(connFactory).build();
    }

    public Read withAddresses(List<String> addresses) {
      checkArgument(addresses != null,
          "MemcachedIO.read().withAddresses(addresses) called with null addresses");
      return toBuilder().setAddresses(addresses).build();
    }

    @Override
    public PCollection<KV<String, Object>> expand(PCollection<String> input) {
      return input
          .apply(ParDo.of(new ReadFn(this)));
    }

    @Override
    public void validate(PipelineOptions options) {
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
    }

    /** A {@link DoFn} executing the SQL query to read from the database. */
    static class ReadFn extends DoFn<String, KV<String, Object>> {
      private MemcachedIO.Read spec;
      private MemcachedClient memcachedClient;

      private ReadFn(Read spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        memcachedClient = new MemcachedClient(
            spec.getConnectionFactory(),
            AddrUtil.getAddresses(spec.getAddresses()));
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String key = context.element();
        Object value = memcachedClient.get(key);
        context.output(KV.of(key, value));
      }

      @Teardown
      public void teardown() throws Exception {
        memcachedClient.shutdown();
      }
    }
  }

  /** A {@link PTransform} to read data from a memcached key-value store. */
  @AutoValue
  public abstract static class Write extends
  PTransform<PCollection<KV<String, KV<Integer, Object>>>, PDone> {
    @Nullable abstract ConnectionFactory getConnectionFactory();
    @Nullable abstract List<String> getAddresses();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionFactory(ConnectionFactory connFactory);
      abstract Builder setAddresses(List<String> addresses);
      abstract MemcachedIO.Write build();
    }

    public Write withConnectionFactory(ConnectionFactory connFactory) {
      checkArgument(connFactory != null,
          "MemcachedIO.read().withConnectionFactory(connFactory) called with null connFactory");
      return toBuilder().setConnectionFactory(connFactory).build();
    }

    public Write withAddresses(List<String> addresses) {
      checkArgument(addresses != null,
          "MemcachedIO.read().withAddresses(addresses) called with null addresses");
      return toBuilder().setAddresses(addresses).build();
    }

    @Override
    public PDone expand(PCollection<KV<String, KV<Integer, Object>>> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
    }

    /** A {@link DoFn} executing the SQL query to read from the database. */
    static class WriteFn extends DoFn<KV<String, KV<Integer, Object>>, Void> {
      private MemcachedIO.Write spec;
      private MemcachedClient memcachedClient;

      private WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        memcachedClient = new MemcachedClient(
            spec.getConnectionFactory(),
            AddrUtil.getAddresses(spec.getAddresses()));
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        memcachedClient.set(context.element().getKey(),
            context.element().getValue().getKey(),
            context.element().getValue().getValue());
      }

      @Teardown
      public void teardown() throws Exception {
        memcachedClient.shutdown();
      }
    }
  }
}
