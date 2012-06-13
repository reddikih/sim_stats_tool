#!/usr/bin/env python3

import sys, os, re, collections, logging
from pssim_dbif import DBInterFace

rx = re.compile(r".*?(normal|maid|raposda)_dd(\d{4})_cdratio(\d{3})_cm(\d{1,3})[MGT]B_readratio(0[357])_blksw(on|off)_dcsw(on|off)")
MetaData = collections.namedtuple('MetaData', 'storage numdd ratiocd cachemem ratioread block diskcache')

CacheDiskStat = collections.namedtuple('CacheDiskStat', 'type elapsed joule kwh')
DataDiskStat = collections.namedtuple('DataDiskStat', 'type elapsed joule kwh spinup spindown')
PerfStat = collections.namedtuple('PerfStat', 'avg_resp throughput bandwidth numreq numread numwrite')
# SpinStat = collections.namedtuple('SpinStat', 'spinup spindown')
OverflowStat = collections.namedtuple('OverflowStat', 'count time')
CacheHitStat = collections.namedtuple('CacheHitStat', 'type storage hit total ratio')

FormatDD = collections.namedtuple('FormatDD', 'type diskno dataid size state start end energy access')
FormatCR = collections.namedtuple('FormatCR', 'type requestid dataid size arrival response access')
FormatBW = collections.namedtuple('FormatBW', 'type dataid memoryid time replica result')
FormatCMH = collections.namedtuple('FormatCMH', 'type dataid storeid time replica result')
FormatCDH = collections.namedtuple('FormatCDH', 'type dataid diskid time result')

dbobj = None

logging.basicConfig(
    level=logging.DEBUG,
    filename='log.txt',
    format='%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s'
)

# listing files under the root directory
def traverse(rootpath):
    for path in os.listdir(rootpath):
        path = os.path.join(rootpath, path)
        if (os.path.isdir(path)):
            traverse(path)
        elif (os.path.isfile(path)):
            extra_meta_data(path)

# extract meta data from a filepath
def extra_meta_data(path):
    m = rx.match(path)
    if (m):
        metadata = MetaData(*m.groups())
        (dirname, filename) = os.path.split(path)
        (shortname, extension) = os.path.splitext(filename)

        if (shortname.lower() == 'datadisk'):
            calculate_energy(shortname.lower(), path, metadata)
        elif (shortname.lower() == 'cachedisk'):
            calculate_energy(shortname.lower(), path, metadata)
        elif (shortname.lower() == 'clientrequest'):
            calculate_performance(path, metadata)
        elif (shortname.lower() == 'bufferwritableratio'):
            calculate_buffer_overflow(path, metadata)
        elif (shortname.lower() == 'cachediskhitratio' or
              shortname.lower() == 'cachememoryhitratio'):
            calculate_cache_hit(path, metadata)

        print("done : ", metadata, filename)
        logging.info('done : ' + str(metadata) + filename)

def calculate_energy(disktype, path, meta):
    (dirname, filename) = os.path.split(path)
    (shortname, extension) = os.path.splitext(filename)
    
    joule = 0.0
    max_elapsed = 0.0
    spinup = 0
    spindown = 0

    for lino, line in enumerate(open(path, 'r'), start=1):
        try:
            if (lino == 1):
                continue
            line = line.strip()
            aline = line.split(',')
            record = FormatDD(*aline)
            joule += float(record.energy)
            end = float(record.end)
            if (max_elapsed < end):
                max_elapsed = end
            if (record.state.lower() == 'spinup'):
                spinup += 1
            if (record.state.lower() == 'spindown'):
                spindown += 1
        except ValueError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            # return
        except TypeError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            # return
    
    if (disktype.lower() == 'datadisk'):
        diskstat = DataDiskStat(disktype, (max_elapsed / 3600), joule, (joule / (3600 * 1000)), spinup, spindown)
        query = 'INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?)'.format(disktype)
        # spinstat = SpinStat(spinup, spindown)
    elif (disktype.lower() == 'cachedisk'):
        diskstat = CacheDiskStat(disktype, (max_elapsed / 3600), joule, (joule / (3600 * 1000)))
        query = 'INSERT INTO {} VALUES (?, ?, ?, ?, ?)'.format(disktype)
    
    # outform_dd = 'type={0},elapsed_hour={1},joule={2},kWh={3},spinup={4},spindown={5}\n'
    # print_stat(disktype + '.stat', diskstat, outform_dd, meta)

    # output to the db(sqlite) file instead of text file.
    dbinsert_stat(diskstat, query, meta)

def calculate_performance(path, meta):
    (dirname, filename) = os.path.split(path)
    (shortname, extension) = os.path.splitext(filename)

    num_req = 0
    num_read = 0
    num_write = 0
    bandwidth = 0.0
    sum_resp = 0.0
    sum_size = 0
    
    for lino, line in enumerate(open(path, 'r'), start=1):
        try:
            if (lino == 1):
                continue
            line = line.strip()
            aline = line.split(',')
            record = FormatCR(*aline)
            num_req += 1
            if (record.access.lower() == 'read\n'):
                num_read += 1
            elif (record.access.lower() == 'write\n'):
                num_write += 1
            sum_resp += float(record.response) - float(record.arrival)
            sum_size += int(record.size)
        except TypeError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return
        except ValueError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return

    avg_resp = sum_resp / num_req
    throughput = num_req / sum_resp if sum_resp > 0 else 0.0
    bandwidth = sum_size / sum_resp if sum_resp > 0 else 0.0
    perfstat = PerfStat(avg_resp, throughput, bandwidth, num_req, num_read, num_write)
    
    # outform = 'avg_resp={0},throughput={1},bandwidth={2},numreq={3},numread={4},numwrite={5}\n'
    # print_stat('performance.stat', perfstat, outform, meta)

    query = 'INSERT INTO performance VALUES (?,?,?,?,?,?,?)'
    dbinsert_stat(perfstat, query, meta)

def calculate_buffer_overflow(path, meta):
    (dirname, filename) = os.path.split(path)
    (shortname, extension) = os.path.splitext(filename)

    count = 0
    time = ""

    for lino, line in enumerate(open(path, 'r'), start=1):
        try:
            if (lino == 1):
                continue
            line = line.strip()
            aline = line.split(',')
            record = FormatBW(*aline)

            if (record.result.lower() == 'false'):
                count += 1
                if (len(time) == 0):
                    time = record.time
                else:
                    time = time + ":" + record.time
        except ValueError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return
        except TypeError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return
    
    overflowstat = OverflowStat(count, time)
    # outform = 'count={0},time={1}\n'
    # print_stat('overflow.stat', overflowstat, outform, meta)

    query = 'INSERT INTO overflow VALUES (?,?,?)'
    dbinsert_stat(overflowstat, query, meta)

def calculate_cache_hit(path, meta):
    (dirname, filename) = os.path.split(path)
    (shortname, extension) = os.path.splitext(filename)

    total = 0
    hit = 0
    cachetype = ''
    if (shortname.lower().find('mem') > 0):
        cachetype = 'memory'
    elif (shortname.lower().find('disk') > 0):
        cachetype = 'disk'

    for lino, line in enumerate(open(path, 'r'), start=1):
        try:
            if (lino == 1):
                continue
            line = line.strip()
            aline = line.split(',')
            if (cachetype == 'memory'):
                record = FormatCMH(*aline)
            elif (cachetype == 'disk'):
                record = FormatCDH(*aline)

            if (record.result.lower() == 'true'):
                hit += 1
            total += 1
        except ValueError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return
        except TypeError as err:
            print('Error!! {filename}:{lino}: {err}'.format(**locals()))
            return

    hitratio = hit / total if total else 0
    cachehitstat = CacheHitStat(cachetype, meta.storage, hit, total, hitratio)
    # outform = 'type={0},hit={1},total={2},ratio={3}\n'
    # print_stat('cachehit.stat', cachehitstat, outform, meta)

    query = 'INSERT INTO cachehit VALUES (?,?,?,?,?,?)'
    dbinsert_stat(cachehitstat, query, meta)


def print_stat(statname, stat, outform, meta, a_mode='a', a_encoding='utf-8'):
    metaheader = '#meta storage={},numdd={},ratiocd={},cachemem={},ratioread={},block={},diskcache={}\n'

    with open(os.path.join(outdir,statname), mode=a_mode, encoding=a_encoding) as outfile:
        outfile.write(metaheader.format(*meta))
        outfile.write(outform.format(*stat))


def dbinsert_stat(stat, query, meta):
    global dbobj

    if (not dbobj):
        dbobj = DBInterface(dbname)
        if (not dbobj):
            raise ValueError('dbobj is not created!! Why!?')
    
    # insert meta data into meta table.
    if (dbobj.isNewMeta(meta)):
        dbobj.createMetaRecord(meta)

    # get the id which was inserted to the db.
    metaid = dbobj.getMetaId(meta)

    # create a question mark of the SQL
    qmark = list()
    qmark.append(metaid)
    qmark.extend(list(stat))

    dbobj.executeQuery(query, qmark)


def create_db(dbname):
    global dbobj

    if (not dbobj):
        dbobj = DBInterFace(dbname)


if __name__ == '__main__':
    if (len(sys.argv) > 3 or len(sys.argv) < 2):
        # print('Usage {0} target_dir [destination_dir]'.format(sys.argv[0]))
        print('Usage: {0} target_dir'.format(sys.argv[0]))
        exit()

    # create a db file
    import datetime
    dt = datetime.datetime.now()
    create_db(dt.strftime('%Y%m%d-%H%M%S'));

    if (len(sys.argv) == 3):
        outdir = os.path.join(os.getcwd(), sys.argv[2])
        if not os.path.exists(outdir):
            os.mkdir(outdir)

    traverse(sys.argv[1])
