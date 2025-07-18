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
# This script is used to compile StarRocks
# Usage: 
#    sh build.sh --help
# Eg:
#    sh build.sh                                      build all
#    sh build.sh  --be                                build Backend without clean
#    sh build.sh  --fe --clean                        clean and build Frontend and Spark Dpp application
#    sh build.sh  --fe --be --clean                   clean and build Frontend, Spark Dpp application and Backend
#    sh build.sh  --spark-dpp                         build Spark DPP application alone
#    sh build.sh  --hive-udf                          build Hive UDF alone
#    BUILD_TYPE=build_type ./build.sh --be            build Backend is different mode (build_type could be Release, Debug, or Asan. Default value is Release. To build Backend in Debug mode, you can execute: BUILD_TYPE=Debug ./build.sh --be)
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################
startTime=$(date +%s)
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
MACHINE_TYPE=$(uname -m)

export STARROCKS_HOME=${ROOT}

if [ -z $BUILD_TYPE ]; then
    export BUILD_TYPE=Release
fi

cd $STARROCKS_HOME
if [ -z $STARROCKS_VERSION ]; then
    tag_name=$(git describe --tags --exact-match 2>/dev/null)
    branch_name=$(git symbolic-ref -q --short HEAD)
    if [ ! -z $tag_name ]; then
        export STARROCKS_VERSION=$tag_name
    elif [ ! -z $branch_name ]; then
        export STARROCKS_VERSION=$branch_name
    else
        export STARROCKS_VERSION=$(git rev-parse --short=7 HEAD)
    fi
fi

if [ -z $STARROCKS_COMMIT_HASH ] ; then
    export STARROCKS_COMMIT_HASH=$(git rev-parse --short=7 HEAD)
fi

set -eo pipefail
. ${STARROCKS_HOME}/env.sh

if [[ $OSTYPE == darwin* ]] ; then
    PARALLEL=$(sysctl -n hw.ncpu)
    # We know for sure that build-thirdparty.sh will fail on darwin platform, so just skip the step.
else
    if [[ ! -f ${STARROCKS_THIRDPARTY}/installed/llvm/lib/libLLVMInstCombine.a ]]; then
        echo "Thirdparty libraries need to be build ..."
        ${STARROCKS_THIRDPARTY}/build-thirdparty.sh
    fi
    PARALLEL=$[$(nproc)/4+1]
fi

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --format-lib       build StarRocks format library, only with shared-data mode cluster
     --fe               build Frontend and Spark Dpp application
     --spark-dpp        build Spark DPP application
     --hive-udf         build Hive UDF
     --clean            clean and build target
     --enable-shared-data
                        build Backend with shared-data feature support
     --use-staros       DEPRECATED, an alias of --enable-shared-data option
     --with-gcov        build Backend with gcov, has an impact on performance
     --without-gcov     build Backend without gcov(default)
     --with-bench       build Backend with bench(default without bench)
     --with-clang-tidy  build Backend with clang-tidy(default without clang-tidy)
     --without-java-ext build Backend without java-extensions(default with java-extensions)
     --without-starcache
                        build Backend without starcache library
     -j                 build Backend parallel
     --output-compile-time 
                        save a list of the compile time for every C++ file in ${ROOT}/compile_times.txt.
                        Turning this option on automatically disables ccache.
     --without-tenann
                        build without vector index tenann library
     --with-compress-debug-symbol {ON|OFF}
                        build with compressing debug symbol. (default: $WITH_COMPRESS)
     --with-source-file-relative-path {ON|OFF}
                        build source file with relative path. (default: $WITH_RELATIVE_SRC_PATH)
     --without-avx2     build Backend without avx2(instruction)    
     --with-maven-batch-mode {ON|OFF}
                        build maven project in batch mode (default: $WITH_MAVEN_BATCH_MODE)
     --output           specify the output directory (default: $STARROCKS_HOME/output)
     --disable-java-check-style
                        disable Java checkstyle checks during build (default: $DISABLE_JAVA_CHECK_STYLE)
     -h,--help          Show this help message
  Eg.
    $0                                           build all
    $0 --be                                      build Backend without clean
    $0 --format-lib                              build StarRocks format library without clean
    $0 --fe --clean                              clean and build Frontend and Spark Dpp application
    $0 --fe --be --clean                         clean and build Frontend, Spark Dpp application and Backend
    $0 --spark-dpp                               build Spark DPP application alone
    $0 --hive-udf                                build Hive UDF
    $0 --be --output PATH                        build Backend that outputs results to a specified path (relative paths are supported).
    BUILD_TYPE=build_type ./build.sh --be        build Backend is different mode (build_type could be Release, Debug, or Asan. Default value is Release. To build Backend in Debug mode, you can execute: BUILD_TYPE=Debug ./build.sh --be)
    DISABLE_JAVA_CHECK_STYLE=ON ./build.sh       build with Java checkstyle disabled
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o 'hj:' \
  -l 'be' \
  -l 'format-lib' \
  -l 'fe' \
  -l 'spark-dpp' \
  -l 'hive-udf' \
  -l 'clean' \
  -l 'with-gcov' \
  -l 'with-bench' \
  -l 'with-clang-tidy' \
  -l 'without-gcov' \
  -l 'without-java-ext' \
  -l 'without-starcache' \
  -l 'with-brpc-keepalive' \
  -l 'use-staros' \
  -l 'enable-shared-data' \
  -l 'output-compile-time' \
  -l 'without-tenann' \
  -l 'with-compress-debug-symbol:' \
  -l 'with-source-file-relative-path:' \
  -l 'without-avx2' \
  -l 'with-maven-batch-mode:' \
  -l 'output:' \
  -l 'help' \
  -l 'disable-java-check-style' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_BE=
BUILD_FORMAT_LIB=
BUILD_FE=
BUILD_SPARK_DPP=
BUILD_HIVE_UDF=
CLEAN=
WITH_GCOV=OFF
WITH_BENCH=OFF
WITH_CLANG_TIDY=OFF
WITH_COMPRESS=ON
WITH_STARCACHE=ON
USE_STAROS=OFF
BUILD_JAVA_EXT=ON
OUTPUT_COMPILE_TIME=OFF
WITH_TENANN=ON
WITH_RELATIVE_SRC_PATH=ON

# Default to OFF, turn it ON if current shell is non-interactive
WITH_MAVEN_BATCH_MODE=OFF
if [ -x "$(command -v tty)" ] ; then
    # has `tty` and `tty` cmd indicates a non-interactive shell.
    if ! tty -s >/dev/null 2>&1; then
        WITH_MAVEN_BATCH_MODE=ON
    fi
fi

MSG=""
MSG_FE="Frontend"
MSG_DPP="Spark Dpp application"
MSG_BE="Backend"
MSG_FORMAT_LIB="Format Lib"
if [[ -z ${USE_AVX2} ]]; then
    USE_AVX2=ON
fi
if [[ -z ${USE_AVX512} ]]; then
    ## Disable it by default
    USE_AVX512=OFF
fi
if [[ -z ${USE_SSE4_2} ]]; then
    USE_SSE4_2=ON
fi
if [[ -z ${USE_BMI_2} ]]; then
    USE_BMI_2=ON
fi
if [[ -z ${ENABLE_JIT} ]]; then
    ENABLE_JIT=ON
fi
if [[ -z ${DISABLE_JAVA_CHECK_STYLE} ]]; then
    DISABLE_JAVA_CHECK_STYLE=OFF
fi

if [[ -z ${CCACHE} ]] && [[ -x "$(command -v ccache)" ]]; then
    CCACHE=ccache
fi

if [ -e /proc/cpuinfo ] ; then
    # detect cpuinfo
    if [[ -z $(grep -o 'avx[^ ]\+' /proc/cpuinfo) ]]; then
        USE_AVX2=OFF
    fi
    if [[ -z $(grep -o 'avx512' /proc/cpuinfo) ]]; then
        USE_AVX512=OFF
    fi
    if [[ -z $(grep -o 'sse4[^ ]*' /proc/cpuinfo) ]]; then
        USE_SSE4_2=OFF
    fi
    if [[ -z $(grep -o 'bmi2' /proc/cpuinfo) ]]; then
        USE_BMI_2=OFF
    fi
fi

if [[ -z ${ENABLE_QUERY_DEBUG_TRACE} ]]; then
	ENABLE_QUERY_DEBUG_TRACE=OFF
fi

if [[ -z ${ENABLE_FAULT_INJECTION} ]]; then
    ENABLE_FAULT_INJECTION=OFF
fi

HELP=0
if [ $# == 1 ] ; then
    # default. `sh build.sh``
    BUILD_BE=1
    BUILD_FE=1
    BUILD_SPARK_DPP=1
    BUILD_HIVE_UDF=1
    BUILD_FORMAT_LIB=0
    CLEAN=0
elif [[ $OPTS =~ "-j " ]] && [ $# == 3 ]; then
    # default. `sh build.sh -j 32`
    BUILD_BE=1
    BUILD_FE=1
    BUILD_SPARK_DPP=1
    BUILD_HIVE_UDF=1
    BUILD_FORMAT_LIB=0
    CLEAN=0
    PARALLEL=$2
else
    BUILD_BE=0
    BUILD_FORMAT_LIB=0
    BUILD_FE=0
    BUILD_SPARK_DPP=0
    BUILD_HIVE_UDF=0
    CLEAN=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --format-lib) BUILD_FORMAT_LIB=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --spark-dpp) BUILD_SPARK_DPP=1 ; shift ;;
            --hive-udf) BUILD_HIVE_UDF=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            --with-gcov) WITH_GCOV=ON; shift ;;
            --without-gcov) WITH_GCOV=OFF; shift ;;
            --enable-shared-data|--use-staros) USE_STAROS=ON; shift ;;
            --with-bench) WITH_BENCH=ON; shift ;;
            --with-clang-tidy) WITH_CLANG_TIDY=ON; shift ;;
            --without-java-ext) BUILD_JAVA_EXT=OFF; shift ;;
            --without-starcache) WITH_STARCACHE=OFF; shift ;;
            --output-compile-time) OUTPUT_COMPILE_TIME=ON; shift ;;
            --without-tenann) WITH_TENANN=OFF; shift ;;
            --without-avx2) USE_AVX2=OFF; shift ;;
            --with-compress-debug-symbol) WITH_COMPRESS=$2 ; shift 2 ;;
            --with-source-file-relative-path) WITH_RELATIVE_SRC_PATH=$2 ; shift 2 ;;
            --with-maven-batch-mode) WITH_MAVEN_BATCH_MODE=$2 ; shift 2 ;;
            --output) STARROCKS_OUTPUT=$2 ; shift 2 ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            -j) PARALLEL=$2; shift 2 ;;
            --disable-java-check-style) DISABLE_JAVA_CHECK_STYLE=ON; shift ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ "${BUILD_TYPE}" == "ASAN" && "${WITH_GCOV}" == "ON" ]]; then
    echo "Error: ASAN and gcov cannot be enabled at the same time. Please disable one of them."
    exit 1
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [ ${CLEAN} -eq 1 ] && [ ${BUILD_BE} -eq 0 ] && [ ${BUILD_FORMAT_LIB} -eq 0 ] && [ ${BUILD_FE} -eq 0 ] && [ ${BUILD_SPARK_DPP} -eq 0 ] && [ ${BUILD_HIVE_UDF} -eq 0 ]; then
    echo "--clean can not be specified without --fe or --be or --format-lib or --spark-dpp or --hive-udf"
    exit 1
fi
if [ ${BUILD_BE} -eq 1 ] && [ ${BUILD_FORMAT_LIB} -eq 1 ]; then
    echo "--format-lib can not be specified with --be"
    exit 1
fi
if [ ${BUILD_FORMAT_LIB} -eq 1 ]; then
    echo "do not build java extensions when build format-lib."
    BUILD_JAVA_EXT=OFF
fi

echo "Get params:
    BUILD_BE                    -- $BUILD_BE
    BUILD_FORMAT_LIB            -- $BUILD_FORMAT_LIB
    BE_CMAKE_TYPE               -- $BUILD_TYPE
    BUILD_FE                    -- $BUILD_FE
    BUILD_SPARK_DPP             -- $BUILD_SPARK_DPP
    BUILD_HIVE_UDF              -- $BUILD_HIVE_UDF
    CCACHE                      -- ${CCACHE}
    CLEAN                       -- $CLEAN
    WITH_GCOV                   -- $WITH_GCOV
    WITH_BENCH                  -- $WITH_BENCH
    WITH_CLANG_TIDY             -- $WITH_CLANG_TIDY
    WITH_COMPRESS_DEBUG_SYMBOL  -- $WITH_COMPRESS
    WITH_STARCACHE              -- $WITH_STARCACHE
    ENABLE_SHARED_DATA          -- $USE_STAROS
    USE_AVX2                    -- $USE_AVX2
    USE_AVX512                  -- $USE_AVX512
    USE_SSE4_2                  -- $USE_SSE4_2
    USE_BMI_2                   -- $USE_BMI_2
    PARALLEL                    -- $PARALLEL
    ENABLE_QUERY_DEBUG_TRACE    -- $ENABLE_QUERY_DEBUG_TRACE
    ENABLE_FAULT_INJECTION      -- $ENABLE_FAULT_INJECTION
    BUILD_JAVA_EXT              -- $BUILD_JAVA_EXT
    OUTPUT_COMPILE_TIME         -- $OUTPUT_COMPILE_TIME
    WITH_TENANN                 -- $WITH_TENANN
    WITH_RELATIVE_SRC_PATH      -- $WITH_RELATIVE_SRC_PATH
    WITH_MAVEN_BATCH_MODE       -- $WITH_MAVEN_BATCH_MODE
    DISABLE_JAVA_CHECK_STYLE    -- $DISABLE_JAVA_CHECK_STYLE
"

check_tool()
{
    local toolname=$1
    if [ -e $STARROCKS_THIRDPARTY/installed/bin/$toolname ] ; then
        return 0
    fi
    if which $toolname &>/dev/null ; then
        return 0
    fi
    return 1
}

# check protoc and thrift
for tool in protoc thrift
do
    if ! check_tool $tool ; then
        echo "Can't find command tool '$tool'!"
        exit 1
    fi
done

# Clean and build generated code
echo "Build generated code"
cd ${STARROCKS_HOME}/gensrc
if [ ${CLEAN} -eq 1 ]; then
   make clean
   rm -rf ${STARROCKS_HOME}/fe/fe-core/target
fi
# DO NOT using parallel make(-j) for gensrc
make
cd ${STARROCKS_HOME}

if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    export LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64/server/
    WITH_TENANN=OFF
else
    export LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server/
fi

addon_mvn_opts=""
if [ "x$WITH_MAVEN_BATCH_MODE" = "xON" ] ; then
    # this option is only available with mvn >= 3.6
    addon_mvn_opts="--batch-mode"
fi

if [ "x$DISABLE_JAVA_CHECK_STYLE" = "xON" ] ; then
    # Add checkstyle.skip parameter to disable Java checkstyle
    addon_mvn_opts="${addon_mvn_opts} -Dcheckstyle.skip=true"
fi

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] || [ ${BUILD_FORMAT_LIB} -eq 1 ] ; then
    if ! ${CMAKE_CMD} --version; then
        echo "Error: cmake is not found"
        exit 1
    fi

    # When build starrocks format lib, USE_STAROS must be ON
    if [ ${BUILD_FORMAT_LIB} -eq 1 ] ; then
        USE_STAROS=ON
        WITH_TENANN=OFF
    fi

    CMAKE_BUILD_TYPE=$BUILD_TYPE
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/build_${CMAKE_BUILD_TYPE}
    if [ "${WITH_GCOV}" = "ON" ]; then
        CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/build_${CMAKE_BUILD_TYPE}_gcov
    fi
    if [ ${BUILD_FORMAT_LIB} -eq 1 ] ; then
        CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/build_${CMAKE_BUILD_TYPE}_format-lib
    fi

    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${STARROCKS_HOME}/be/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}

    source ${STARROCKS_HOME}/bin/common.sh

    cd ${CMAKE_BUILD_DIR}
    if [ "${USE_STAROS}" == "ON"  ]; then
      if [ -z "$STARLET_INSTALL_DIR" ] ; then
        # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
        STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
      fi
      export STARLET_INSTALL_DIR
    fi
    
    if [ "${OUTPUT_COMPILE_TIME}" == "ON" ]; then
        rm -f ${ROOT}/compile_times.txt
        CXX_COMPILER_LAUNCHER=${ROOT}/build-support/compile_time.sh
    else
        CXX_COMPILER_LAUNCHER=${CCACHE}
    fi
    if [ "${WITH_CLANG_TIDY}" == "ON" ];then
        # this option cannot work with clang-14
        WITH_COMPRESS=OFF
    fi


    ${CMAKE_CMD} -G "${CMAKE_GENERATOR}"                                \
                  -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}        \
                  -DSTARROCKS_HOME=${STARROCKS_HOME}                    \
                  -DSTARLET_INSTALL_DIR=${STARLET_INSTALL_DIR}          \
                  -DCMAKE_CXX_COMPILER_LAUNCHER=${CXX_COMPILER_LAUNCHER} \
                  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}                \
                  -DMAKE_TEST=OFF -DWITH_GCOV=${WITH_GCOV}              \
                  -DUSE_AVX2=$USE_AVX2 -DUSE_AVX512=$USE_AVX512         \
                  -DUSE_SSE4_2=$USE_SSE4_2 -DUSE_BMI_2=$USE_BMI_2       \
                  -DENABLE_QUERY_DEBUG_TRACE=$ENABLE_QUERY_DEBUG_TRACE  \
                  -DWITH_BENCH=${WITH_BENCH}                            \
                  -DWITH_CLANG_TIDY=${WITH_CLANG_TIDY}                  \
                  -DWITH_COMPRESS=${WITH_COMPRESS}                      \
                  -DWITH_STARCACHE=${WITH_STARCACHE}                    \
                  -DUSE_STAROS=${USE_STAROS}                            \
                  -DENABLE_FAULT_INJECTION=${ENABLE_FAULT_INJECTION}    \
                  -DBUILD_BE=${BUILD_BE}                                \
                  -DWITH_TENANN=${WITH_TENANN}                          \
                  -DSTARROCKS_JIT_ENABLE=${ENABLE_JIT}                  \
                  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON                    \
                  -DBUILD_FORMAT_LIB=${BUILD_FORMAT_LIB}                \
                  -DWITH_RELATIVE_SRC_PATH=${WITH_RELATIVE_SRC_PATH}    \
                  ..

    time ${BUILD_SYSTEM} -j${PARALLEL}
    if [ "${WITH_CLANG_TIDY}" == "ON" ];then
        exit 0
    fi

    ${BUILD_SYSTEM} install

    # Build Java Extensions
    if [ ${BUILD_JAVA_EXT} = "ON" ]; then
        echo "Build Java Extensions"
        cd ${STARROCKS_HOME}/java-extensions
        if [ ${CLEAN} -eq 1 ]; then
            ${MVN_CMD} clean
        fi
        ${MVN_CMD} $addon_mvn_opts package -DskipTests -T ${PARALLEL}
        cd ${STARROCKS_HOME}
    else
        echo "Skip Building Java Extensions"
    fi
fi

cd ${STARROCKS_HOME}

# Assesmble FE modules
FE_MODULES=
if [ ${BUILD_FE} -eq 1 ] || [ ${BUILD_SPARK_DPP} -eq 1 ] || [ ${BUILD_HIVE_UDF} -eq 1 ]; then
    if [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp"
    fi
    if [ ${BUILD_HIVE_UDF} -eq 1 ]; then
        FE_MODULES="fe-common,hive-udf"
    fi
    if [ ${BUILD_FE} -eq 1 ]; then
        FE_MODULES="hive-udf,fe-common,spark-dpp,fe-core"
    fi
fi

# Clean and build Frontend
if [ ${FE_MODULES}x != ""x ]; then
    echo "Build Frontend Modules: $FE_MODULES"
    cd ${STARROCKS_HOME}/fe
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} $addon_mvn_opts package -am -pl ${FE_MODULES} -DskipTests -T ${PARALLEL}
    cd ${STARROCKS_HOME}/java-extensions
    ${MVN_CMD} $addon_mvn_opts package -am -pl hadoop-ext -DskipTests -T ${PARALLEL}
    cd ${STARROCKS_HOME}
fi


# Clean and prepare output dir
STARROCKS_OUTPUT=${STARROCKS_OUTPUT:="${STARROCKS_HOME}/output/"}
echo "OUTPUT DIR=${STARROCKS_OUTPUT}"
mkdir -p ${STARROCKS_OUTPUT}

# Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_FE} -eq 1 ]; then
        install -d ${STARROCKS_OUTPUT}/fe/bin ${STARROCKS_OUTPUT}/fe/conf/ \
                   ${STARROCKS_OUTPUT}/fe/webroot/ ${STARROCKS_OUTPUT}/fe/lib/ \
                   ${STARROCKS_OUTPUT}/fe/spark-dpp/ ${STARROCKS_OUTPUT}/fe/hive-udf

        cp -r -p ${STARROCKS_HOME}/bin/*_fe.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/bin/show_fe_version.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/bin/common.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/conf/fe.conf ${STARROCKS_OUTPUT}/fe/conf/
        cp -r -p ${STARROCKS_HOME}/conf/udf_security.policy ${STARROCKS_OUTPUT}/fe/conf/
        cp -r -p ${STARROCKS_HOME}/conf/hadoop_env.sh ${STARROCKS_OUTPUT}/fe/conf/
        cp -r -p ${STARROCKS_HOME}/conf/core-site.xml ${STARROCKS_OUTPUT}/fe/conf/
        cp -r -p ${STARROCKS_HOME}/conf/cluster_snapshot.yaml ${STARROCKS_OUTPUT}/fe/conf/

        rm -rf ${STARROCKS_OUTPUT}/fe/lib/*
        cp -r -p ${STARROCKS_HOME}/fe/fe-core/target/lib/* ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_HOME}/fe/fe-core/target/starrocks-fe.jar ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/hadoop-ext/target/starrocks-hadoop-ext.jar ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_HOME}/webroot/* ${STARROCKS_OUTPUT}/fe/webroot/
        cp -r -p ${STARROCKS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/fe/spark-dpp/
        cp -r -p ${STARROCKS_HOME}/fe/hive-udf/target/hive-udf-1.0.0.jar ${STARROCKS_OUTPUT}/fe/hive-udf/
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/async-profiler ${STARROCKS_OUTPUT}/fe/bin/
        MSG="${MSG} √ ${MSG_FE}"
    elif [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        install -d ${STARROCKS_OUTPUT}/fe/spark-dpp/
        rm -rf ${STARROCKS_OUTPUT}/fe/spark-dpp/*
        cp -r -p ${STARROCKS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/fe/spark-dpp/
        cp -r -p ${STARROCKS_HOME}/fe/hive-udf/target/hive-udf-1.0.0.jar ${STARROCKS_HOME}/fe/hive-udf/
        MSG="${MSG} √ ${MSG_DPP}"
    fi
fi

if [ ${BUILD_FORMAT_LIB} -eq 1 ]; then
    rm -rf ${STARROCKS_OUTPUT}/format-lib/*
    mkdir -p ${STARROCKS_OUTPUT}/format-lib
    cp -r ${STARROCKS_HOME}/be/output/format-lib/* ${STARROCKS_OUTPUT}/format-lib/
    # format $BUILD_TYPE to lower case
    ibuildtype=`echo ${BUILD_TYPE} | tr 'A-Z' 'a-z'`
    if [ "${ibuildtype}" == "release" ] ; then
        pushd ${STARROCKS_OUTPUT}/format-lib/ &>/dev/null
        FORMAT_LIB=libstarrocks_format.so
        FORMAT_LIB_DEBUGINFO=libstarrocks_format.debuginfo
        echo "Split $FORMAT_LIB debug symbol to $FORMAT_LIB_DEBUGINFO ..."
        # strip be binary
        # if eu-strip is available, can replace following three lines into `eu-strip -g -f starrocks_be.debuginfo starrocks_be`
        objcopy --only-keep-debug $FORMAT_LIB $FORMAT_LIB_DEBUGINFO
        strip --strip-debug $FORMAT_LIB
        objcopy --add-gnu-debuglink=$FORMAT_LIB_DEBUGINFO $FORMAT_LIB
        popd &>/dev/null
    fi
    MSG="${MSG} √ ${MSG_FORMAT_LIB}"
fi

if [ ${BUILD_BE} -eq 1 ]; then
    rm -rf ${STARROCKS_OUTPUT}/be/lib/*
    mkdir -p ${STARROCKS_OUTPUT}/be/lib/jni-packages
    mkdir -p ${STARROCKS_OUTPUT}/be/lib/py-packages

    install -d ${STARROCKS_OUTPUT}/be/bin  \
               ${STARROCKS_OUTPUT}/be/conf \
               ${STARROCKS_OUTPUT}/be/lib/hadoop \
               ${STARROCKS_OUTPUT}/be/www

    cp -r -p ${STARROCKS_HOME}/be/output/bin/* ${STARROCKS_OUTPUT}/be/bin/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/be.conf ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/udf_security.policy ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/be_test.conf ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/cn.conf ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/hadoop_env.sh ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/log4j2.properties ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/core-site.xml ${STARROCKS_OUTPUT}/be/conf/

    if [ "${BUILD_TYPE}" == "ASAN" ]; then
        cp -r -p ${STARROCKS_HOME}/be/output/conf/asan_suppressions.conf ${STARROCKS_OUTPUT}/be/conf/
    fi
    cp -r -p ${STARROCKS_HOME}/be/output/lib/starrocks_be ${STARROCKS_OUTPUT}/be/lib/
    cp -r -p ${STARROCKS_HOME}/be/output/lib/libmockjvm.so ${STARROCKS_OUTPUT}/be/lib/libjvm.so
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jemalloc/bin/jeprof ${STARROCKS_OUTPUT}/be/bin
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jemalloc/lib-shared/libjemalloc.so.2 ${STARROCKS_OUTPUT}/be/lib/libjemalloc.so.2
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jemalloc-debug/lib/libjemalloc.so.2 ${STARROCKS_OUTPUT}/be/lib/libjemalloc-dbg.so.2
    ln -s ./libjemalloc.so.2 ${STARROCKS_OUTPUT}/be/lib/libjemalloc.so

    # format $BUILD_TYPE to lower case
    ibuildtype=`echo ${BUILD_TYPE} | tr 'A-Z' 'a-z'`
    if [ "${ibuildtype}" == "release" ] ; then
        pushd ${STARROCKS_OUTPUT}/be/lib/ &>/dev/null
        BE_BIN=starrocks_be
        BE_BIN_DEBUGINFO=starrocks_be.debuginfo
        echo "Split $BE_BIN debug symbol to $BE_BIN_DEBUGINFO ..."
        # strip be binary
        # if eu-strip is available, can replace following three lines into `eu-strip -g -f starrocks_be.debuginfo starrocks_be`
        objcopy --only-keep-debug $BE_BIN $BE_BIN_DEBUGINFO
        strip --strip-debug $BE_BIN
        objcopy --add-gnu-debuglink=$BE_BIN_DEBUGINFO $BE_BIN
        popd &>/dev/null
    fi
    cp -r -p ${STARROCKS_HOME}/be/output/www/* ${STARROCKS_OUTPUT}/be/www/

    if [ "${BUILD_JAVA_EXT}" == "ON" ]; then
        # note that conf files will not be overwritten when doing upgrade.
        # so we have to preserve directory structure to avoid upgrade incompatibility.
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/hadoop/lib/native ${STARROCKS_OUTPUT}/be/lib/hadoop/native
        cp -r -p ${STARROCKS_HOME}/java-extensions/hadoop-lib/target/hadoop-lib ${STARROCKS_OUTPUT}/be/lib/hadoop/common
        cp -r -p ${STARROCKS_HOME}/java-extensions/jdbc-bridge/target/starrocks-jdbc-bridge-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/udf-extensions/target/udf-extensions-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/java-utils/target/starrocks-java-utils.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/jni-connector/target/starrocks-jni-connector.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/hudi-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/starrocks-hudi-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/starrocks-hudi-reader.jar ${STARROCKS_OUTPUT}/be/lib/hudi-reader-lib
        cp -r -p ${STARROCKS_HOME}/java-extensions/odps-reader/target/odps-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/odps-reader/target/starrocks-odps-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/odps-reader/target/starrocks-odps-reader.jar ${STARROCKS_OUTPUT}/be/lib/odps-reader-lib
        cp -r -p ${STARROCKS_HOME}/java-extensions/iceberg-metadata-reader/target/iceberg-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/iceberg-metadata-reader/target/starrocks-iceberg-metadata-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/iceberg-metadata-reader/target/starrocks-iceberg-metadata-reader.jar ${STARROCKS_OUTPUT}/be/lib/iceberg-reader-lib
        cp -r -p ${STARROCKS_HOME}/java-extensions/common-runtime/target/common-runtime-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/paimon-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/starrocks-paimon-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/starrocks-paimon-reader.jar ${STARROCKS_OUTPUT}/be/lib/paimon-reader-lib
        cp -r -p ${STARROCKS_HOME}/java-extensions/kudu-reader/target/kudu-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/kudu-reader/target/starrocks-kudu-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/kudu-reader/target/starrocks-kudu-reader.jar ${STARROCKS_OUTPUT}/be/lib/kudu-reader-lib
        cp -r -p ${STARROCKS_HOME}/java-extensions/hadoop-ext/target/starrocks-hadoop-ext.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/hive-reader/target/hive-reader-lib ${STARROCKS_OUTPUT}/be/lib/
        cp -r -p ${STARROCKS_HOME}/java-extensions/hive-reader/target/starrocks-hive-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
        cp -r -p ${STARROCKS_HOME}/java-extensions/hive-reader/target/starrocks-hive-reader.jar ${STARROCKS_OUTPUT}/be/lib/hive-reader-lib
    fi

    cp -r -p ${STARROCKS_HOME}/be/extension/python-udf/src/flight_server.py ${STARROCKS_OUTPUT}/be/lib/py-packages

    MSG="${MSG} √ ${MSG_BE}"
fi



cp -r -p "${STARROCKS_HOME}/LICENSE.txt" "${STARROCKS_OUTPUT}/LICENSE.txt"
build-support/gen_notice.py "${STARROCKS_HOME}/licenses,${STARROCKS_HOME}/licenses-binary" "${STARROCKS_OUTPUT}/NOTICE.txt" all

endTime=$(date +%s)
totalTime=$((endTime - startTime))

echo "***************************************"
echo "Successfully build StarRocks ${MSG} ; StartTime:$(date -d @$startTime '+%Y-%m-%d %H:%M:%S'), EndTime:$(date -d @$endTime '+%Y-%m-%d %H:%M:%S'), TotalTime:${totalTime}s"
echo "***************************************"

if [[ ! -z ${STARROCKS_POST_BUILD_HOOK} ]]; then
    eval ${STARROCKS_POST_BUILD_HOOK}
fi

exit 0
