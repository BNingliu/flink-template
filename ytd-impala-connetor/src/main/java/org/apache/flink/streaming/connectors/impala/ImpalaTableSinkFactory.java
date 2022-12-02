package org.apache.flink.streaming.connectors.impala;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.impala.enums.EAuthMech;
import org.apache.flink.streaming.connectors.impala.table.ImpalaSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;


/**
 * impala connecter
 */
public class ImpalaTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "impala";


    public static final ConfigOption<String> HOST = key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("impala host and port,");


    public static final ConfigOption<String> PASSWORD = key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("impala password");


    public static final ConfigOption<String> USERNAME = key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("impala username");


    public static final ConfigOption<String> TABLENAME = key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("impala table-nam");


    public static final ConfigOption<String> DATASOURCE = key("database-name")
            .stringType()
            .noDefaultValue()
            .withDescription("impala database-name");

    public static final ConfigOption<Integer> AUTHMECH = key("auth-mech")
            .intType()
            .noDefaultValue()
            .withDescription("impala auth-mech ");
    public static final ConfigOption<String> STORETYPE = key("store-type")
            .stringType()
            .noDefaultValue()
            .withDescription("impala store-type");

    public static final ConfigOption<String> PATITTION = key("partition-fields")
            .stringType()
            .noDefaultValue()
            .withDescription("impala partition-fields");

    public static final ConfigOption<Integer> BATCHSIZE = key("batchSize")
            .intType()
            .noDefaultValue()
            .withDescription("impala batchSize");

    public static final ConfigOption<Integer> PARALLELISM = key("parallelism")
            .intType()
            .noDefaultValue()
            .withDescription("impala parallelism");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig config = helper.getOptions();
        String host = getImpalaJdbcUrl(config);
        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);
        String database = config.getOptional(DATASOURCE).orElse("default");
        String tableName = config.get(TABLENAME);
        String storeType = config.get(STORETYPE);
        String partitionFields = config.get(PATITTION);
        Integer batchSize = config.get(BATCHSIZE);
        Integer parallelism = config.getOptional(PARALLELISM).orElse(1);

//        DataType dataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
//        DataType dataType1 = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new ImpalaSink(
                host,
                username,
                password,
                database,
                tableName,
                resolvedSchema,
                storeType,
                partitionFields,
                batchSize,
                config.get(AUTHMECH),
                parallelism
        );
    }

    public String getImpalaJdbcUrl(ReadableConfig config) {
        String host = config.getOptional(HOST).orElse(null);
        Integer authMech = config.getOptional(AUTHMECH).orElse(null);
        StringBuilder urlBuffer = new StringBuilder(host);
        urlBuffer.append("/").append(config.getOptional(DATASOURCE).orElse("default"));
        if (authMech == EAuthMech.NoAuthentication.getType()) {
            return host;
        } else if (authMech == EAuthMech.Kerberos.getType()) {
            String krbRealm = null;
            String krbHostFqdn = null;
            String krbServiceName = null;
            urlBuffer.append(";"
                    .concat("AuthMech=1;")
                    .concat("KrbRealm=").concat(krbRealm).concat(";")
                    .concat("KrbHostFQDN=").concat(krbHostFqdn).concat(";")
                    .concat("KrbServiceName=").concat(krbServiceName).concat(";")
            );
            host = urlBuffer.toString();
        } else if (authMech == EAuthMech.UserName.getType()) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(config.getOptional(USERNAME).get()).concat(";")
                    .concat("PWD=;")
                    .concat("UseSasl=0")
            );
            host = urlBuffer.toString();
        } else if (authMech == EAuthMech.NameANDPassword.getType()) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(config.getOptional(USERNAME).get()).concat(";")
                    .concat("PWD=").concat(config.getOptional(PASSWORD).get())
            );
            host = urlBuffer.toString();
        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }
        return host;
    }


    // 当 connector 与 IDENTIFIER 一直才会找到 RedisTableSinkFactory 通过
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(TABLENAME);
        options.add(DATASOURCE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(AUTHMECH);
        options.add(STORETYPE);
        options.add(PATITTION);
        options.add(BATCHSIZE);
        options.add(PARALLELISM);
        return options;
    }
}

