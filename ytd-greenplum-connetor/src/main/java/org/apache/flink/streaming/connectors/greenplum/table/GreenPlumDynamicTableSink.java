package org.apache.flink.streaming.connectors.greenplum.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class GreenPlumDynamicTableSink implements DynamicTableSink {

    private final JdbcConnectorOptions jdbcOptions;
    private final Integer maxSize;
    private final DataType dataType;
    private final ResolvedCatalogTable catalogTable;

    public GreenPlumDynamicTableSink(
            JdbcConnectorOptions jdbcOptions
            , Integer maxSize
            , ResolvedCatalogTable catalogTable
    ) {
        this.jdbcOptions = jdbcOptions;
        this.maxSize = maxSize;
        this.dataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.catalogTable= catalogTable;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        GreenPlumSinkFunction gpSinkFunction = new GreenPlumSinkFunction(jdbcOptions,dataType, catalogTable,maxSize);
        return SinkFunctionProvider.of(gpSinkFunction,jdbcOptions.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new GreenPlumDynamicTableSink(jdbcOptions, maxSize, catalogTable);
    }

    @Override
    public String asSummaryString() {
        return "greenplum Table Sink";
    }

}
