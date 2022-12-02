package org.apache.flink.streaming.connectors.opengauss.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.connectors.opengauss.dialect.OpenGaussDialect;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author lijiaqi
 */
public class OpenGaussDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "opengauss";

    private static final String DRIVER_NAME = "org.opengauss.Driver";
    private static final Integer DEFAULT_MAX_ROW = 500;

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");

    public static final ConfigOption<String> DRIVER = ConfigOptions
            .key("driver")
            .stringType()
            .defaultValue(DRIVER_NAME)
            .withDescription("the jdbc driver.");



    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc user name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc password.");

    public static final ConfigOption<Integer> maxRows = ConfigOptions
            .key("connector.write.flush.max-rows")
            .intType()
            .defaultValue(DEFAULT_MAX_ROW)
            .withDescription("the jdbc driver.");
//    public static final ConfigOption<String> FORMAT = ConfigOptions
//            .key("format")
//            .stringType()
//            .noDefaultValue()
//            .withDescription("the format.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
//        requiredOptions.add(FORMAT);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();

        helper.validate();

        JdbcConnectorOptions jdbcOptions = getJdbcOptions(config);

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new OpenGaussDynamicTableSource(jdbcOptions, physicalSchema);

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

//        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
//                SerializationFormatFactory.class,
//                FactoryUtil.FORMAT);

        final ReadableConfig config = helper.getOptions();

        // validate all options
        helper.validate();

        // get the validated options
        JdbcConnectorOptions jdbcOptions = getJdbcOptions(config);

        // derive the produced data type (excluding computed columns) from the catalog table
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        // table sink
        return new OpenGaussDynamicTableSink(jdbcOptions, null, catalogTable);
    }

    private JdbcConnectorOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcConnectorOptions.Builder builder = JdbcConnectorOptions.builder()
                .setDriverName(DRIVER_NAME)
                .setDBUrl(url)
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(new OpenGaussDialect());

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

}
