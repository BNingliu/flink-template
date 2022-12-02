package com.ytd.template.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HdfsUtil
 * @Author ChristChen
 * @Date 2019/12/9 17:24
 * @Description TODO
 */
public class HdfsUtil {

    protected static final Logger log = LoggerFactory.getLogger(HdfsUtil.class);


    private String hdfsUrl;

    private String kerberosFlag = ReadProperties.getProperty("hdfs.kerberos.flag");
    private String kerberosConf=ReadProperties.getProperty("hdfs.kerberos.conf");
    private String jaasConf=ReadProperties.getProperty("hdfs.kerberos.jaas.conf");

    private String loginUser=ReadProperties.getProperty("hdfs.kerberos.loginUser");
    private String loginPath=ReadProperties.getProperty("hdfs.kerberos.path");


    private String host =ReadProperties.getProperty("hdfs.host");
    private int port = Integer.valueOf(ReadProperties.getProperty("hdfs.port"));

    Configuration configuration;

    public HdfsUtil() {
        this.init(host, port, true);
    }

    public HdfsUtil(boolean append) {
        this.init(this.host, this.port, append);
    }

    public HdfsUtil(String host, int port, boolean append) {
        this.init(host, port, true);
    }

    public void init(String host, int port, boolean append) {
        if(configuration==null){
            configuration = new Configuration();
            hdfsUrl = "hdfs://" + host + ":" + port;
            configuration.set("fs.defaultFS", hdfsUrl);
            if (append) {
                configuration.setBoolean("dfs.support.append", true);
                configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy" ,"NEVER");
                configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable" ,"true");
            }
            configuration.set("fs.hdfs.impl.disable.cache", "true");
            if("true".equals(kerberosFlag)) {
                log.info("======启用kerberos认证=======");
                System.setProperty("java.security.krb5.conf", kerberosConf);
                System.setProperty("java.security.auth.login.config", jaasConf);
                //System.setProperty("java.security.krb5.realm", kerberosRealm);
                //System.setProperty("java.security.krb5.kdc", kerberosKdc);
                configuration.setBoolean("hadoop.security.authorization", true);
                configuration.set("hadoop.security.authentication", "Kerberos");
                //conf.set("dfs.namenode.kerberos.principal", nameNodeprincipal);
                //conf.set("dfs.datanode.kerberos.principal", datanodePrincipal);
                UserGroupInformation.setConfiguration(configuration);
                //设置登录的kerberos principal和对应的keytab文件，其中keytab文件需要kdc管理员生成给到开发人员
                try {
                    UserGroupInformation.loginUserFromKeytab(loginUser,loginPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.info("kerberos 认证成功");
            }

        }
    }


    public Boolean exist(String hdfsFilePath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            log.info("check file exist in HDFS : {}", hdfsFilePath);
            return fileSystem.exists(new Path(hdfsFilePath));
        } catch (Exception e) {
            log.error("check exists failed : ", e);
        }
        throw new Exception("检查文件是否存在时异常");
    }


    /**
     * 修改制定文件或路径的权限为 777
     *
     * @param path
     */
    public void changePermission(String path) {
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fileSystem.setPermission(new Path(path), permission);
        } catch (Exception e) {
            log.error("change permission error:", e);
        }
    }


    /**
     * web端下载文件
     *
     * @param hdfsPath     文件的路径
     * @param outputStream 输出流
     **/
    public void webDownload(String hdfsPath, OutputStream outputStream) {
        try (
                FileSystem fileSystem = FileSystem.get(configuration);
                FSDataInputStream inputStream = fileSystem.open(new Path(hdfsPath));
        ) {
            IOUtils.copyBytes(inputStream, outputStream, configuration);
        } catch (Exception e) {
            log.error("tail file error:", e);
        }
    }


    /**
     * @param hdfsFilePath
     * @return 读取hdfs文件中内容
     */
    public List<String> getFileToString(String hdfsFilePath) throws Exception {
        if (!exist(hdfsFilePath)) {
            throw new Exception("路径不存在");
        }

        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            FSDataInputStream inputStream = fileSystem.open(new Path(hdfsFilePath));
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));//防止中文乱码

            List<String> result = new ArrayList<>();
            for (; ; ) {
                String line = bf.readLine();

                if (line == null)
                    break;
                result.add(line);
            }
            return result;
        }

    }



    public String  getFileToString2(String hdfsFilePath) throws Exception {
        if (!exist(hdfsFilePath)) {
            throw new Exception("路径不存在");
        }

        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            FSDataInputStream inputStream = fileSystem.open(new Path(hdfsFilePath));
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = inputStream.read()) != -1) {
                sb.append((char) ch);

            }
            return sb.toString();

        } catch (Exception e) {
            log.error("get file info failed : ", e);
        }
        return "";

    }

    public String getFileToString3(String hdfsFilePath) throws Exception {
        if (!exist(hdfsFilePath)) {
            throw new Exception("路径不存在");
        }

        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            FSDataInputStream inputStream = fileSystem.open(new Path(hdfsFilePath));
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));//防止中文乱码
            String line;
            List<String> result = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            while ((line = bf.readLine()) != null) {
                sb.append(line).append("\n");
//                result.add(line);
            }
            return sb.toString();

        } catch (Exception e) {
            log.error("get file info failed : ", e);
        }
        return "";
    }


    public static void main(String[] args) throws Exception {
        HdfsUtil hdfsUtil = new HdfsUtil("192.168.2.131", 8020, false);
        // 获取文件信息
        List<String> fileToString = hdfsUtil.getFileToString("/streamx/flink/flink-libs/user-lib/a2.sql");
//        List<SqlNodeInfo> sqlNodeInfos = FlinkSqlParserUtil.parseSqlContext(sql);

        System.out.println(fileToString);
    }


}
