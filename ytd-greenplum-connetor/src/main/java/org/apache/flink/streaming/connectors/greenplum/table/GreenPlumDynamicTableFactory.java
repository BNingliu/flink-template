package org.apache.flink.streaming.connectors.greenplum.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * @author liuningbo
 */
public class GreenPlumDynamicTableFactory implements  DynamicTableSinkFactory {

    public static final String IDENTIFIER = "ytd-greenplum";

    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private static final Integer DEFAULT_MAX_ROW = 500;
    private static final Integer DEFAULT_PORT = 5432;

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port")
            .intType()
            .defaultValue(DEFAULT_PORT)
            .withDescription("the jdbc database url.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions
            .key("database-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database name.");

    public static final ConfigOption<String> SCHEMA_NAME = ConfigOptions
            .key("schema-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc schema name.");

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
            .key("max-rows")
            .intType()
            .defaultValue(DEFAULT_MAX_ROW)
            .withDescription("the jdbc driver.");


    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(DATABASE_NAME);
        requiredOptions.add(SCHEMA_NAME);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(PORT);


        return requiredOptions;
    }
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(maxRows);
//        options.add(FactoryUtil.SINK_PARALLELISM);
//        options.add(FactoryUtil.CONNECTOR);
        return options;
    }




    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        try {
           // validate all options
           helper.validate();
           // get the validated options
           JdbcConnectorOptions jdbcOptions = getJdbcOptions(config);
           // derive the produced data type (excluding computed columns) from the catalog table
           ResolvedCatalogTable catalogTable = context.getCatalogTable();
           return new GreenPlumDynamicTableSink(jdbcOptions, config.getOptional(maxRows).get(), catalogTable);
           // table sink
       }catch (Exception e){
           e.printStackTrace();
       }
    return  null;
    }

    private JdbcConnectorOptions getJdbcOptions(ReadableConfig readableConfig) {
        String url =   "jdbc:postgresql://"+readableConfig.get(URL)+":"+readableConfig.get(PORT)+"/"+readableConfig.get(DATABASE_NAME);
        String table = readableConfig.get(SCHEMA_NAME) +"."+ readableConfig.get(TABLE_NAME);
        Integer integer = readableConfig.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null);

        final JdbcConnectorOptions.Builder builder = JdbcConnectorOptions.builder()
                .setDriverName(DRIVER_NAME)
                .setDBUrl(url)
                .setParallelism(integer)
                .setTableName(table);
//                .setDialect(new GreenPlumDialect());

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

}
