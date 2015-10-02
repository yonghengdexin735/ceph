#!/bin/bash

BIN_PATH=${TESTDIR}/fsstress/ltp-full-20091231/testcases/kernel/fs/fsstress/fsstress

if [ ! -f ${BIN_PATH} ]
then
    path=`pwd`
    mkdir -p ${TESTDIR}/fsstress
    cd ${TESTDIR}/fsstress
    wget -q -O ${TESTDIR}/fsstress/ltp-full.tgz http://download.ceph.com/qa/ltp-full-20091231.tgz
    tar xzf ${TESTDIR}/fsstress/ltp-full.tgz
    rm ${TESTDIR}/fsstress/ltp-full.tgz
    cd ${TESTDIR}/fsstress/ltp-full-20091231/testcases/kernel/fs/fsstress
    make
    cd $path
fi

command="${BIN_PATH} -d fsstress-`hostname`$$ -l 1 -n 1000 -p 10 -v"

echo "Starting fsstress $command"
mkdir fsstress`hostname`-$$
$command
