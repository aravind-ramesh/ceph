#!/bin/bash
#
# Copyright (C) 2016 Western Digital Corporation
#
# Author: Aravind Ramesh
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7121" # git grep '\<7107\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}


#
# Create ec pool with different crc omap size and different (k, m) and
# perform read/write
#
function TEST_crc_omap_size() {
    local dir=$1
    
    echo "DBG12 TEST_crc_omap_size"
    setup $dir || return 1
    run_mon $dir a || return 1
    for id in $(seq 0 7) ; do
        run_osd $dir $id \
	    --osd-ec-verify-stripelet-crc=true
    done

    create_testdata $dir

    for i in $(seq 1 4) ; do
	local omap_size=$((${i} * ${i} * 1024))
	local k=2
	local m=1
	for j in $(seq 0 3) ; do
	    local profilename=ecprofile${i}${j}
	    local poolname=ecpool${i}${j}
	    set_ec_profile_and_create_pool $dir $profilename $k $m $omap_size $poolname
	    # Following (k m) configuration (2 1), (3 2), (4 2), (5 3) is used.
	    k=$((${k}+1))
	    m=$((${k}-${m}))
	    write_objects_to_pool $dir $poolname || return 1
	    read_objects_from_pool $dir $poolname || return 1
	done
    done

    wait_for_clean || return 1
    # Initiate deep scrub on all OSDs
    for k in $(seq 0 7) ; do
	ceph osd deep-scrub $k
    done
    delete_testdata $dir
    teardown $dir || return 1
}

function set_ec_profile_and_create_pool() {
    local dir=$1
    local profilename=$2
    local k=$3
    local m=$4
    local omap_size=$5
    local poolname=$6

    ceph osd erasure-code-profile set $profilename \
        k=$k m=$m ruleset-failure-domain=osd omap_size=$omap_size || return 1
    ceph osd pool create $poolname 8 8 erasure $profilename || return 1
    #wait_for_clean || return 1
}

function create_testdata() {
    local dir=$1

    dd if=/dev/urandom of=$dir/testdata.1 bs=512 count=1
    dd if=/dev/urandom of=$dir/testdata.2 bs=512 count=8
    dd if=/dev/urandom of=$dir/testdata.3 bs=512 count=1024
    dd if=/dev/urandom of=$dir/testdata.4 bs=512 count=4096
    dd if=/dev/urandom of=$dir/testdata.5 bs=512 count=6000
    dd if=/dev/urandom of=$dir/testdata.6 bs=1024 count=7000
}

function delete_testdata() {
    local dir=$1
    rm $dir/testdata.*
}

function write_objects_to_pool() {
    local dir=$1
    local poolname=$2
    for j in $(seq 1 6) ; do
	rados -p $poolname put obj${j} $dir/testdata.${j} || return 1
    done
}

function read_objects_from_pool() {
    local dir=$1
    local poolname=$2
    for j in $(seq 1 6) ; do
	rados -p $poolname get obj${j} $dir/testdata.get.${j} || return 1
    done
}

main osd-omap-size "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#    test/osd/osd-omap-size.sh # TEST_corrupt_and_repair_replicated"
# End:
