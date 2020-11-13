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

##############################################################
# This script is used to compile Apache Doris(incubating)
# Usage:
#    sh build.sh        build both Backend and Frontend.
#    sh build.sh -clean clean previous output and build.
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail
#set -e 根据返回值来判断，一个命令是否运行失败,脚本只要发生错误，就终止执行；但是不适用于管道命令。所谓管道命令，就是多个子命令通过管道运算符（|）组合成为一个大的命令。
# Bash 会把最后一个子命令的返回值，作为整个命令的返回值。也就是说，只要最后一个子命令不失败，管道命令总是会执行成功，因此它后面命令依然会执行，set -e就失效了。
# set -o pipefail用来解决这种情况，只要一个子命令失败，整个管道命令就失败，脚本就会终止执行。

ROOT=`dirname "$0"`    #获取当前shell脚本文件所在的目录（命令中“`”不是英文的单引号，而是英文输入法下的“~”同一个按键下面的那个符号。）
ROOT=`cd "$ROOT"; pwd` #进入当前shell脚本文件所在的目录，并获取当前目录的路径

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh  #执行${DORIS_HOME}/env.sh脚本，进行编译前的各项环境检查

# build thirdparty libraries if necessary
if [[ ! -f ${DORIS_THIRDPARTY}/installed/lib/libs2.a ]]; then  #判断libs2.a文件是否存在，并且是否为普通文件
    echo "Thirdparty libraries need to be build ..."
    ${DORIS_THIRDPARTY}/build-thirdparty.sh                    #执行脚本build-thirdparty.sh下载并编译第三方依赖库
fi

#PARALLEL=$[$(nproc)/4+1]
PARALLEL=12

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --fe               build Frontend
     --clean            clean and build target
     --with-mysql       enable MySQL support(default)
     --without-mysql    disable MySQL support
     --with-lzo         enable LZO compress support(default)
     --without-lzo      disable LZO compress  support

  Eg.
    $0                                      build Backend and Frontend without clean
    $0 --be                                 build Backend without clean
    $0 --be --without-mysql                 build Backend with MySQL disable
    $0 --be --without-mysql --without-lzo   build Backend with both MySQL and LZO disable
    $0 --fe --clean                         clean and build Frontend
    $0 --fe --be --clean                    clean and build both Frontend and Backend
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'be' \
  -l 'fe' \
  -l 'clean' \
  -l 'with-mysql' \
  -l 'without-mysql' \
  -l 'with-lzo' \
  -l 'without-lzo' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then   #“$?”表示上一条命令的返回状态，如果为0，表示上一条命令正确执行；如果非0，表示上一条命令执行不正确
    usage               #执行usage()函数输出信息
fi

eval set -- "$OPTS"

BUILD_BE=
BUILD_FE=
CLEAN=
RUN_UT=
WITH_MYSQL=ON
WITH_LZO=ON
HELP=0
if [ $# == 1 ] ; then  # “$#”表示命令行中所有参数的个数
    # defuat
    BUILD_BE=1
    BUILD_FE=1
    CLEAN=0
    RUN_UT=0
else
    BUILD_BE=0
    BUILD_FE=0
    CLEAN=0
    RUN_UT=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            --ut) RUN_UT=1   ; shift ;;
            --with-mysql) WITH_MYSQL=ON; shift ;;
            --without-mysql) WITH_MYSQL=OFF; shift ;;
            --with-lzo) WITH_LZO=ON; shift ;;
            --without-lzo) WITH_LZO=OFF; shift ;;
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

if [ ${CLEAN} -eq 1 -a ${BUILD_BE} -eq 0 -a ${BUILD_FE} -eq 0 ]; then
    echo "--clean can not be specified without --fe or --be"
    exit 1
fi

echo "Get params:
    BUILD_BE    -- $BUILD_BE
    BUILD_FE    -- $BUILD_FE
    CLEAN       -- $CLEAN
    RUN_UT      -- $RUN_UT
    WITH_MYSQL  -- $WITH_MYSQL
    WITH_LZO    -- $WITH_LZO
"

# Clean and build generated code
echo "Build generated code"
cd ${DORIS_HOME}/gensrc
if [ ${CLEAN} -eq 1 ]; then
   make clean
fi
# DO NOT using parallel make(-j) for gensrc
make             #执行${DORIS_HOME}/gensrc/目录下的Makefile文件，进行编译
cd ${DORIS_HOME}

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}           # 设置编译类型，可以为DEBUG、RELEASE、BCC、ASAN、LSAN、UBSAN、TSAN
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${DORIS_HOME}/be/build_${CMAKE_BUILD_TYPE} # 设置编译路径
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${DORIS_HOME}/be/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}                        # 创建编译路径
    cd ${CMAKE_BUILD_DIR}                              # 进入编译目录
    ${CMAKE_CMD} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DMAKE_TEST=OFF -DWITH_MYSQL=${WITH_MYSQL} -DWITH_LZO=${WITH_LZO} ../ # 使用cmake工具，根据上一层目录中的CMakeLists.txt文件生成Makefile文件
    make -j${PARALLEL} # 根据camke生成的Makefile文件执行编译，参数“-j”用来设置编译的并行度
    make install       # 将编译好的可执行文件安装到camke生成的Makefile文件中指定的路径
    cd ${DORIS_HOME}
fi

# Build docs, should be built before Frontend
echo "Build docs"
cd ${DORIS_HOME}/docs
./build_help_zip.sh
cd ${DORIS_HOME}

# Clean and build Frontend
if [ ${BUILD_FE} -eq 1 ] ; then
    echo "Build Frontend"
    cd ${DORIS_HOME}/fe
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} package -DskipTests
    cd ${DORIS_HOME}
fi

# Clean and prepare output dir
DORIS_OUTPUT=${DORIS_HOME}/output/
mkdir -p ${DORIS_OUTPUT}

#Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/fe/bin ${DORIS_OUTPUT}/fe/conf \
               ${DORIS_OUTPUT}/fe/webroot/ ${DORIS_OUTPUT}/fe/lib/

    cp -r -p ${DORIS_HOME}/bin/*_fe.sh ${DORIS_OUTPUT}/fe/bin/
    cp -r -p ${DORIS_HOME}/conf/fe.conf ${DORIS_OUTPUT}/fe/conf/
    rm -rf ${DORIS_OUTPUT}/fe/lib/*
    cp -r -p ${DORIS_HOME}/fe/target/lib/* ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/fe/target/palo-fe.jar ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/docs/build/help-resource.zip ${DORIS_OUTPUT}/fe/lib/
    cp -r -p ${DORIS_HOME}/webroot/* ${DORIS_OUTPUT}/fe/webroot/
fi
if [ ${BUILD_BE} -eq 1 ]; then
    install -d ${DORIS_OUTPUT}/be/bin  \
               ${DORIS_OUTPUT}/be/conf \
               ${DORIS_OUTPUT}/be/lib/ \
               ${DORIS_OUTPUT}/be/www  \
               ${DORIS_OUTPUT}/udf/lib \
               ${DORIS_OUTPUT}/udf/include

    cp -r -p ${DORIS_HOME}/be/output/bin/* ${DORIS_OUTPUT}/be/bin/
    cp -r -p ${DORIS_HOME}/be/output/conf/* ${DORIS_OUTPUT}/be/conf/
    cp -r -p ${DORIS_HOME}/be/output/lib/* ${DORIS_OUTPUT}/be/lib/
    cp -r -p ${DORIS_HOME}/be/output/www/* ${DORIS_OUTPUT}/be/www/
    cp -r -p ${DORIS_HOME}/be/output/udf/*.a ${DORIS_OUTPUT}/udf/lib/
    cp -r -p ${DORIS_HOME}/be/output/udf/include/* ${DORIS_OUTPUT}/udf/include/
fi

echo "***************************************"
echo "Successfully build Doris"
echo "***************************************"

if [[ ! -z ${DORIS_POST_BUILD_HOOK} ]]; then
    eval ${DORIS_POST_BUILD_HOOK}
fi

exit 0
