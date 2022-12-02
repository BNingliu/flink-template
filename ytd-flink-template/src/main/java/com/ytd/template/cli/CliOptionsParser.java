/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ytd.template.cli;

import org.apache.commons.cli.*;

public class CliOptionsParser {

    public static final Option OPTION_PATH_MODE = Option
            .builder("m")
            .required(false)
            .type(Integer.class)
            .numberOfArgs(1)
            .argName("jar path")
            .desc("this is jar path mode .")
            .build();

    private static final Option OPTION_SQL_FILE = Option
            .builder("f")
            .required(true) //参数是否必须
            .longOpt("file")
            .numberOfArgs(1)
            .argName("SQL file path")
            .desc("The SQL file path.")
            .build();


    public static final Option OPTION_PARALLELISM = Option
            .builder("p")
            .required(true)
            .type(Integer.class)
            .longOpt("parallelism")
            .numberOfArgs(1)
            .argName("working space dir")
            .desc("set Parallelism .")
            .build();

    private static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    private static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_PARALLELISM);
        options.addOption(OPTION_PATH_MODE);
        return options;
    }



    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    //{"[{colName:id,colType:number},{colName:name,colType:string}]"}
    //JSON.parseArray(args[0])
    public static CliOptions parseClient(String[] args) {


        if (args.length < 3) {
            throw new RuntimeException("./sql-submit -f <sql-file> -p <parallelism> -m <jarPathMode>");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(
                    line.getOptionValue(CliOptionsParser.OPTION_SQL_FILE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_PARALLELISM.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_PATH_MODE.getOpt())
            );
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
