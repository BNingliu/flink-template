package org.apache.flink.streaming.connectors.opengauss.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;

/**
 * @author lijiaqi
 */
public class OpenGaussDynamicTableSink implements DynamicTableSink {

    private final JdbcConnectorOptions jdbcOptions;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType dataType;
    private final ResolvedCatalogTable catalogTable;

    public OpenGaussDynamicTableSink(
            JdbcConnectorOptions jdbcOptions
            , EncodingFormat<SerializationSchema<RowData>> encodingFormat
            , ResolvedCatalogTable catalogTable
    ) {
        this.jdbcOptions = jdbcOptions;
        this.encodingFormat = encodingFormat;
        this.dataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.catalogTable= catalogTable;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        System.out.println("SinkRuntimeProvider");
        System.out.println(dataType);
//        SerializationSchema<RowData> serializationSchema = encodingFormat.createRuntimeEncoder(context, dataType);
        OpenGaussSinkFunction gbasedbtSinkFunction = new OpenGaussSinkFunction(jdbcOptions,dataType, catalogTable);
        return SinkFunctionProvider.of(gbasedbtSinkFunction,1);
    }

    @Override
    public DynamicTableSink copy() {
        return new OpenGaussDynamicTableSink(jdbcOptions, encodingFormat, catalogTable);
    }

    @Override
    public String asSummaryString() {
        return "OpenGauss Table Sink";
    }

}
