package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.FilterPushDownHelper;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/** ClickHouse table source. */
public class ClickHouseDynamicTableSource    implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {

    private final ClickHouseReadOptions readOptions;

    private final Properties connectionProperties;

    private DataType physicalRowDataType;
    private ResolvedSchema resolvedSchema ;

    private String filterClause;

    private long limit = -1L;

    public ClickHouseDynamicTableSource(
            ClickHouseReadOptions readOptions,
            Properties properties,
            DataType physicalRowDataType,
            ResolvedSchema resolvedSchema

    ) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        this.physicalRowDataType = physicalRowDataType;
        this.resolvedSchema=resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        List<String> fileds =resolvedSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<DataType> columnDataTypes = resolvedSchema.getColumnDataTypes();

        AbstractClickHouseInputFormat.Builder builder =
                new AbstractClickHouseInputFormat.Builder()
                        .withOptions(readOptions)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(
                                fileds.toArray(new String[0]))
                        .withFieldTypes(
                                columnDataTypes.toArray(new DataType[0]))
                        .withRowDataTypeInfo(
                                runtimeProviderContext.createTypeInformation(physicalRowDataType))
                        .withFilterClause(filterClause)
                        .withLimit(limit);
//        AbstractClickHouseInputFormat.Builder builder =
//                new AbstractClickHouseInputFormat.Builder()
//                        .withOptions(readOptions)
//                        .withConnectionProperties(connectionProperties)
//                        .withFieldNames(
//                                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
//                        .withFieldTypes(
//                                DataType.getFieldDataTypes(physicalRowDataType)
//                                        .toArray(new DataType[0]))
//                        .withRowDataTypeInfo(
//                                runtimeProviderContext.createTypeInformation(physicalRowDataType))
//                        .withFilterClause(filterClause)
//                        .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource source =
                new ClickHouseDynamicTableSource(
                        readOptions, connectionProperties, physicalRowDataType,resolvedSchema);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filterClause = FilterPushDownHelper.convert(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalRowDataType = physicalRowDataType;
//        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

//    @Override
//    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
//        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
//    }
}
