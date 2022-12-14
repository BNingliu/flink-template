package org.apache.flink.connector.clickhouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ClickHouseDynamicTableSink} from
 * a logical description.
 *
 * <p>TODO: Partitioning strategy isn't well implemented.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private final String[] primaryKeys;

    private final String[] partitionKeys;

    private final DataType physicalRowDataType;

    private final ClickHouseDmlOptions options;

    private boolean dynamicGrouping = false;

    private  ResolvedSchema resolvedSchema ;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    public ClickHouseDynamicTableSink(
            @Nonnull ClickHouseDmlOptions options,
            @Nonnull String[] primaryKeys,
            @Nonnull String[] partitionKeys,
            @Nonnull DataType physicalRowDataType,
            @Nonnull ResolvedSchema resolvedSchema
    ) {
        this.options = options;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.physicalRowDataType = physicalRowDataType;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                ChangelogMode.insertOnly().equals(requestedMode) || primaryKeys.length > 0,
                "Please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        List<String> fileds =resolvedSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<DataType> columnDataTypes = resolvedSchema.getColumnDataTypes();

        AbstractClickHouseOutputFormat outputFormat =
                new AbstractClickHouseOutputFormat.Builder()
                        .withOptions(options)
                        .withFieldNames(
                                fileds.toArray(new String[0]))
                        .withFieldDataTypes(
                                columnDataTypes.toArray(new DataType[0]))
                        .withPrimaryKey(primaryKeys)
                        .withPartitionKey(partitionKeys)
                        .build();

//        AbstractClickHouseOutputFormat outputFormat =
//                new AbstractClickHouseOutputFormat.Builder()
//                        .withOptions(options)
//                        .withFieldNames(
//                                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]))
//                        .withFieldDataTypes(
//                                DataType.getFieldDataTypes(physicalRowDataType)
//                                        .toArray(new DataType[0]))
//                        .withPrimaryKey(primaryKeys)
//                        .withPartitionKey(partitionKeys)
//                        .build();
        return OutputFormatProvider.of(outputFormat, options.getParallelism());
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : partitionKeys) {
            if (partition.containsKey(partitionCol)) {
                staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
            }
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    @Override
    public DynamicTableSink copy() {
        ClickHouseDynamicTableSink sink =
                new ClickHouseDynamicTableSink(
                        options, primaryKeys, partitionKeys, physicalRowDataType,
                        resolvedSchema
                );
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitionSpec = staticPartitionSpec;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table sink";
    }
}
