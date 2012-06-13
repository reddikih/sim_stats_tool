#!/bin/sh

DATA_TYPE=
RATIO_RD=
RATIO_CD=
RATIO_CM=
SQL_FILE=

while getopts ht:r:c:m:f: OPT
do
    case $OPT in
        t)  DATA_TYPE=$OPTARG
            ;;
        r)  RATIO_RD=$OPTARG
            ;;
        c)  RATIO_CD=$OPTARG
            ;;
        m)  RATIO_CM=$OPTARG
            ;;
        f)  SQL_FILE=$OPTARG
            ;;
        h)  echo "Usage -t datatype -r ratioread -c ratiocd -m ratiocm -f sqlitefile" 1>&2
            exit 1
            ;;
        \?) echo "Usage -t datatype -r ratioread -c ratiocd -m ratiocm -f sqlitefile" 1>&2
            exit 1
            ;;
    esac
done

shift `expr $OPTIND - 1`

# for debug
echo "DATA_TYPE = "$DATA_TYPE
echo "RATIO_RD = "$RATIO_RD
echo "RATIO_CD = "$RATIO_CD
echo "RATIO_CM = "$RATIO_CM
echo "SQL_FILE = "$SQL_FILE
echo ""

SQLITE_MODE='.mode csv'
SQLITE_HEADER='.header on'

if [ -z $SQL_FILE -o ! $SQL_FILE ]; then
    echo "database file is not specified!."
    exit 1
fi

if [ $DATA_TYPE = "energy" ]
then
########################################################################
# TODO 2012.05.16
# 現状ではキャッシュメモリのパラメータは固定比率でシミュレーションして
# いるのでSQLで条件指定する必要は無いが，早急にキャッシュメモリもパ
# ラメータとして実験出来るようにして，SQLも修正すること
########################################################################

    # for debug
    # cat <<EOF
    sqlite3 $SQL_FILE <<EOF
${SQLITE_MODE}
${SQLITE_HEADER}
select
  t1t2.numdd as numdd,
  t1t2.normal as normal,
  t1t2.maid as maid,
  t3.raposda as raposda
from
 ((select
     mt.numdd as numdd,
     cd.joule + dd.joule as normal
   from meta mt, cachedisk cd, datadisk dd
   where mt.id = cd.meta_id
   and mt.id = dd.meta_id
   and mt.storage = 'normal'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = 0.0
  ) t1
  join
  (select
     mt.numdd as numdd,
     cd.joule + dd.joule as maid
   from meta mt, cachedisk cd, datadisk dd
   where mt.id = cd.meta_id
   and mt.id = dd.meta_id
   and mt.storage = 'maid'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = ${RATIO_CD}
  ) t2
  on t1.numdd = t2.numdd ) t1t2
  join
  (select
     mt.numdd as numdd,
     cd.joule + dd.joule as raposda
   from meta mt, cachedisk cd, datadisk dd
   where mt.id = cd.meta_id
   and mt.id = dd.meta_id
   and mt.storage = 'raposda'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = ${RATIO_CD}
  ) t3
  on t1t2.numdd = t3.numdd
;
EOF
echo "------------------------------"
fi

if [ $DATA_TYPE = "perf" ]
then

    sqlite3 $SQL_FILE <<EOF
${SQLITE_MODE}
${SQLITE_HEADER}
select
  t1t2.numdd as numdd,
  t1t2.normal as normal,
  t1t2.maid as maid,
  t3.raposda as raposda
from
 ((select
     mt.numdd as numdd,
     pf.avg_resp as normal
   from meta mt, performance pf
   where mt.id = pf.meta_id
   and mt.storage = 'normal'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = 0.0
  ) t1
  join
  (select
     mt.numdd as numdd,
     pf.avg_resp as maid
   from meta mt, performance pf
   where mt.id = pf.meta_id
   and mt.storage = 'maid'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = ${RATIO_CD}
  ) t2
  on t1.numdd = t2.numdd ) t1t2
  join
  (select
     mt.numdd as numdd,
     pf.avg_resp as raposda
   from meta mt, performance pf
   where mt.id = pf.meta_id
   and mt.storage = 'raposda'
   and mt.ratioread = ${RATIO_RD}
   and mt.numdd between 32 and 1024
   and mt.ratiocd = ${RATIO_CD}
  ) t3
  on t1t2.numdd = t3.numdd
;
EOF
echo "------------------------------"
fi

if [ $DATA_TYPE = "spin" ]; then

    for STRG in maid raposda
    do
        sqlite3 $SQL_FILE <<EOF
${SQLITE_MODE}
${SQLITE_HEADER}
select
  dd7355.numdd as numdd,
  dd7355.spin73 as ${STRG}73,
  dd7355.spin55 as ${STRG}55,
  dd37.spin37   as ${STRG}37
from 
  ((
  select
    mt.numdd as numdd,
    mt.ratioread as rread,
    dd.spinup as spin73
  from meta mt, datadisk dd
  where mt.id = dd.meta_id
  and mt.storage = "${STRG}"
  and mt.ratiocd = ${RATIO_CD}
  and mt.ratioread = 0.7
  group by mt.numdd
  ) dd73
  join
  (
  select
    mt.numdd as numdd,
    mt.ratioread as rread,
    dd.spinup as spin55
  from meta mt, datadisk dd
  where mt.id = dd.meta_id
  and mt.storage = "${STRG}"
  and mt.ratiocd = ${RATIO_CD}
  and mt.ratioread = 0.5
  group by mt.numdd
  ) dd55 on dd73.numdd = dd55.numdd ) dd7355
  join
  (
  select
    mt.numdd as numdd,
    mt.ratioread as rread,
    dd.spinup as spin37
  from meta mt, datadisk dd
  where mt.id = dd.meta_id
  and mt.storage = "${STRG}"
  and mt.ratiocd = ${RATIO_CD}
  and mt.ratioread = 0.3
  group by mt.numdd
  ) dd37 on dd7355.numdd = dd37.numdd
;
EOF
    done
echo "------------------------------"
fi

if [ $DATA_TYPE = "hit" ]; then
    sqlite3 $SQL_FILE <<EOF
${SQLITE_MODE}
${SQLITE_HEADER}
select
  numdd,
  cast(sum(case when storage = 'maid' then hit end) as real ) / 
    sum(case when storage = 'maid' then total end) as maid,
  cast(sum(case when storage = 'raposda' then hit end) as real ) / 
    sum(case when storage = 'raposda' then total end) as raposda
from 
(
select * from cachehit ch, meta mt
where mt.id = ch.meta_id 
and mt.storage in ('maid', 'raposda') 
and mt.ratiocd = ${RATIO_CD}
and mt.ratioread = ${RATIO_RD}
)
group by numdd
;
EOF
echo "------------------------------"
fi
