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

import com.ytd.template.constant.SystemConstant;
import com.ytd.template.enums.SqlCommand;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;

/**
 * Simple parser for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

    private SqlCommandParser() {
        // private
    }

    //解析SQL语句,相邻的SQL可以用;分割,也可以不分割
    public static List<SqlCommandCall> parse(List<String> lineList) {

        if (CollectionUtils.isEmpty(lineList)) {
            throw new RuntimeException("lineList is null");
        }

        List<SqlCommandCall> sqlCommandCallList = new ArrayList<>();

        StringBuilder stmt = new StringBuilder();

        for (String line : lineList) {
            //开头是 -- 的表示注释
            if (line.trim().isEmpty() || line.startsWith(SystemConstant.COMMENT_SYMBOL) ||
                    trimStart(line).startsWith(SystemConstant.COMMENT_SYMBOL)) {
                continue;
            }
            stmt.append(SystemConstant.LINE_FEED).append(line);
            if (line.trim().endsWith(SystemConstant.SEMICOLON)) {
                Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
                if (optionalCall.isPresent()) {
                    sqlCommandCallList.add(optionalCall.get());
                } else {
                    throw new RuntimeException("不支持该语法使用" + stmt.toString() + "'");
                }
                stmt.setLength(0);
            }
        }

        return sqlCommandCallList;

    }
    private static String trimStart(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        final char[] value = str.toCharArray();

        int start = 0, last = 0 + str.length() - 1;
        int end = last;
        while ((start <= end) && (value[start] <= ' ')) {
            start++;
        }
        if (start == 0 && end == last) {
            return str;
        }
        if (start >= end) {
            return "";
        }
        return str.substring(start, end);
    }



    public static List<SqlCommandCall> parse(String[] lines) {
        int i = 0;
        boolean isMatch = false;
        List<SqlCommandCall> calls = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : lines) {
//        }
//        for (String line : lines) {
            if (line.trim().isEmpty() || line.startsWith("--")) {
                // skip empty line and comment line
                continue;
            }
            for (SqlCommand command : SqlCommand.values()) {
                Matcher matcher = command.pattern.matcher(line.trim());
                isMatch = matcher.matches();
                if (isMatch) {
                    i++;
                    break;
                }
            }
            if (!(isMatch && i > 1)) {
                stmt.append("\n").append(line);
            }

            if (line.trim().endsWith(";") || (i > 1 & isMatch)) {
                if (!stmt.toString().trim().isEmpty()) {
                    Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
                    if (optionalCall.isPresent()) {
                        calls.add(optionalCall.get());
                    } else {
                        throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
                    }
                }
                // clear string builder
                stmt.setLength(0);
                if (i > 1 & isMatch) {
                    stmt.append("\n").append(line);
                }
            }
        }
        return calls;
    }


    public static List<SqlCommandCall> parseLines(List<String> lines) {
        List<SqlCommandCall> calls = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : lines) {
            stmt.setLength(0);
            String[] lineSplit = line.split("\n");
            for (String str : lineSplit) {
                if (str.trim().isEmpty() || str.startsWith("--")) {
                    // skip empty line and comment line
                    continue;
                }
                stmt.append("\n").append(str);
            }
            Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
            if (optionalCall.isPresent()) {
                calls.add(optionalCall.get());
            } else {
                throw new RuntimeException("Unsupported command '" + lines + "'");
            }
        }
        return calls;
    }

    public static Optional<SqlCommandCall> parse(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        // parse
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.pattern.matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1);
                }
                return cmd.operandConverter.apply(groups)
                        .map((operands) -> new SqlCommandCall(cmd, operands));
            }
        }
        return Optional.empty();
    }


    /**
     * Call of SQL command with operands and command type.
     */
    public static class SqlCommandCall {
        public final SqlCommand command;
        public final String[] operands;

        public SqlCommandCall(SqlCommand command, String[] operands) {
            this.command = command;
            this.operands = operands;
        }

        public SqlCommandCall(SqlCommand command) {
            this(command, new String[0]);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandCall that = (SqlCommandCall) o;
            return command == that.command && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return command + "(" + Arrays.toString(operands) + ")";
        }
    }
}
