package com.ytd.template.dialect;

public enum Platform {
	
	defaultCommon(1), //默认模式
	hiveToMysql(3), //hive同步到mysql
	mysqlBatchToHive(2), //mysql同步到hive(批)
	mysqlStreamToHive(4); //mysql到hive流


	private int platform;

	private Platform(int platform){
		this.platform = platform;
	}

    public int getPlatform() {
        return platform;
    }

    public static Platform getPlatform(int platform) {
        for(Platform pf : values()) {
            if(pf.getPlatform()==platform) return pf;
        }
        throw new RuntimeException("not support platform {"+platform+"}");
    }

    /**
     * es 单独适配器逻辑未剥离
     * @return
     */
    public Dialect getDialect(){
    	switch (platform) {
		case 1:
			return new DeafultDialect();
		case 2:
			return new MysqlBatchToHiveDialect();
		case 3:
			return new HiveToMysqlDialect();
		case 4:
			return new MysqlStreamToHiveDialect();
		default:
			break;
		}
    	return null;
    }
    
}
