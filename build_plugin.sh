#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail
#set -e 根据返回值来判断，一个命令是否运行失败,脚本只要发生错误，就终止执行；但是不适用于管道命令。所谓管道命令，就是多个子命令通过管道运算符（|）组合成为一个大的命令。
# Bash 会把最后一个子命令的返回值，作为整个命令的返回值。也就是说，只要最后一个子命令不失败，管道命令总是会执行成功，因此它后面命令依然会执行，set -e就失效了。
# set -o pipefail用来解决这种情况，只要一个子命令失败，整个管道命令就失败，脚本就会终止执行。

ROOT=`dirname "$0"`     #获取当前shell脚本文件所在的目录（命令中“`”不是英文的单引号，而是英文输入法下的“~”同一个按键下面的那个符号。）
ROOT=`cd "$ROOT"; pwd`  #进入当前shell脚本文件所在的目录，并获取当前目录的路径

export DORIS_HOME=${ROOT} #将当前目录作为环境变量 $DORIS_HOME 的值

. ${DORIS_HOME}/env.sh    #执行${DORIS_HOME}/env.sh脚本，进行编译前的各项环境检查

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --plugin           build special plugin
  Eg.
    $0 --plugin xxx     build xxx plugin
    $0                  build all plugins
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'plugin' \
  -l 'clean' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then  #“$?”表示上一条命令的返回状态，如果为0，表示上一条命令正确执行；如果非0，表示上一条命令执行不正确
    usage              #执行usage()函数输出信息
fi

eval set -- "$OPTS"

ALL_PLUGIN=1
CLEAN=0
if [ $# == 1 ] ; then  # “$#”表示命令行中所有参数的个数
    # defuat
    ALL_PLUGIN=1
    CLEAN=0
else
    while true; do
        case "$1" in
            --plugin)  ALL_PLUGIN=0 ; shift ;;
            --clean)  CLEAN=1 ; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Get params:
    BUILD_ALL_PLUGIN       -- $ALL_PLUGIN
    CLEAN                  -- $CLEAN
"

# 编译fe_plugin之前需要首先判断FE是否完成编译  check if palo-fe.jar exist
if [ ! -f "$DORIS_HOME/fe/target/palo-fe.jar" ]; then
    echo "ERROR: palo-fe.jar does not exist. Please build FE first"
    exit -1
fi

cd ${DORIS_HOME}
PLUGIN_MODULE=
if [ ${ALL_PLUGIN} -eq 1 ] ; then
    cd ${DORIS_HOME}/fe_plugins
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean         # mvn clean：清理mevean项目建的临时文件,一般是模块下的target目录； mvn install：打包会安装到本地的maven仓库中
    fi
    echo "build all plugins"
    ${MVN_CMD} package -DskipTests  # mvn package：打包到本项目，一般是在项目target目录下；mvn deploy：将打包的文件发布到远程(如服务器)参考,提供其他人员进行下载依赖
else
    PLUGIN_MODULE=$1    #编译某一个特定的module
    cd ${DORIS_HOME}/fe_plugins/$PLUGIN_MODULE
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    echo "build plugin $PLUGIN_MODULE"
    ${MVN_CMD} package -DskipTests
fi

cd ${DORIS_HOME}
# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/fe_plugins/output/
mkdir -p ${DORIS_OUTPUT}

if [ ${ALL_PLUGIN} -eq 1 ] ; then
    cp -p ${DORIS_HOME}/fe_plugins/*/target/*.zip ${DORIS_OUTPUT}/
else
    cp -p ${DORIS_HOME}/fe_plugins/$PLUGIN_MODULE/target/*.zip ${DORIS_OUTPUT}/
fi

echo "***************************************"
echo "Successfully build Doris FE Plugin"
echo "***************************************"

exit 0

