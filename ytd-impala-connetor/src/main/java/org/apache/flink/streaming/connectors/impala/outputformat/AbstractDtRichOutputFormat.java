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
package org.apache.flink.streaming.connectors.impala.outputformat;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;


public abstract class AbstractDtRichOutputFormat<T> extends RichOutputFormat<T>{

    public transient Counter outRecords;
    public transient Counter outDirtyRecords;
    public transient Meter outRecordsRate;

    protected static int ROW_PRINT_FREQUENCY = 1000;
    protected static int DIRTY_PRINT_FREQUENCY = 1000;

    /**metric name of dirty data*/

    public static final String DT_NUM_RECORDS_OUT = "dtNumRecordsOut";

    public static final String DT_NUM_DIRTY_RECORDS_OUT = "dtNumDirtyRecordsOut";

    public static final String DT_NUM_RECORDS_OUT_RATE = "dtNumRecordsOutRate";

    public void initMetric() {
        outRecords = getRuntimeContext().getMetricGroup().counter(DT_NUM_RECORDS_OUT);
        outDirtyRecords = getRuntimeContext().getMetricGroup().counter(DT_NUM_DIRTY_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
    }

}
