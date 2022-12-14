package org.apache.flink.streaming.connectors.greenplum.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

public class GreenPlumDynamicTableSource implements ScanTableSource {

    private final JdbcConnectorOptions options;
    private final TableSchema tableSchema;

    public GreenPlumDynamicTableSource(JdbcConnectorOptions options, TableSchema tableSchema) {
        this.options = options;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcDialect dialect = options.getDialect();
        String query = dialect.getSelectFromStatement(
                options.getTableName(),
                tableSchema.getFieldNames(),
                new String[0]
        );
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                //(TypeInformation<RowData>)
                .setRowDataTypeInfo( runtimeProviderContext
                .createTypeInformation(tableSchema.toRowDataType()));
        return InputFormatProvider.of(builder.build());

    }

    @Override
    public DynamicTableSource copy() {
        return new GreenPlumDynamicTableSource(options, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "greenplum Table Source";
    }

}
