#!/usr/bin/env python3

import sqlite3
import logging

# logging.basicConfig(
#     level=logging.DEBUG,
#     filename='log.txt',
#     format='%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s'
# )

class DBUtil:
    
    def convertMetaToDB(self, meta):
        tmplist = list()
        tmplist.append(meta.storage)
        tmplist.append(self.convertNum(meta.numdd))
        tmplist.append(self.convertRatios(meta.ratiocd, 100))
        tmplist.append(self.convertNum(meta.cachemem))
        tmplist.append(self.convertRatios(meta.ratioread, 10))
        tmplist.append(self.convertSW(meta.block))
        tmplist.append(self.convertSW(meta.diskcache))
        return tuple(tmplist)

    def convertNum(self, num):
        return int(num)

    def convertRatios(self, rationum, divid):
        return int(rationum) / divid

    def convertSW(self, sw):
        if (sw.lower() == 'on'):
            return 1
        else:
            return 0

class DBInterFace:
    suffix = ".sqlite3"
    dbUtil = DBUtil()
    
    table_names = dict(meta='meta',
                       cachedisk='cachedisk',
                       cachehit='cachehit',
                       datadisk='datadisk',
                       overflow='overflow',
                       performance='performance')

    def __init__(self, filename):
        if (len(filename) < 1):
            raise ValueError('filename is null.')

        self.dbfile = filename + self.suffix
        self.conn = sqlite3.connect(self.dbfile)
        self.createTables()

    def isNewMeta(self, meta):
        query = '''
SELECT COUNT(*) 
FROM meta 
WHERE storage = ? 
AND numdd = ? 
AND ratiocd = ? 
AND cachemem = ? 
AND ratioread = ? 
AND block = ? 
AND diskcache = ?
'''
        dbMeta = self.dbUtil.convertMetaToDB(meta)
        cursor = self.conn.cursor()
        logging.debug('query : ' + query.replace('\n', ' '))
        logging.debug('dbmeta : ' + str(dbMeta))
        cursor.execute(query, dbMeta)
        resRow = cursor.fetchone()
        logging.debug('result : ' + str(resRow[0]))
        if (resRow[0] > 0):
            return False
        
        return True

    def createMetaRecord(self, meta):
        query = '''
INSERT INTO meta VALUES
(null, ?, ?, ?, ?, ?, ?, ?)
'''
        # for value in meta:
        #     print(value)
        metaTuple = self.dbUtil.convertMetaToDB(meta)

        cursor = self.conn.cursor()
        cursor.execute(query, metaTuple)
        logging.debug('query : ' + query.replace('\n', ' '))
        logging.debug('dbmeta : ' + str(metaTuple))
        self.conn.commit()
        cursor.close()

    def getMetaId(self, meta):
        result = -1
        query = '''
SELECT max(id) FROM meta
WHERE storage = ?
AND   numdd = ?
AND   ratiocd = ?
AND   cachemem = ?
AND   ratioread = ?
AND   block = ?
AND   diskcache = ?
'''

        dbMeta = self.dbUtil.convertMetaToDB(meta)
        cursor = self.conn.cursor()
        cursor.execute(query, dbMeta)
        resRow = cursor.fetchone()
        if (resRow):
            result = resRow[0]
        cursor.close()
        return result

    def executeDML(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def executeQuery(self, sql, qmark = None):
        cursor = self.conn.cursor()
        if (not qmark):
            cursor.execute(sql)
        else:
            logging.debug('sql : ' + sql.replace('\n', ' '))
            logging.debug('qmark : ' + str(qmark))
            cursor.execute(sql, qmark)
        self.conn.commit()
        cursor.close()
        
    def createTables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
CREATE TABLE meta (
       id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
       storage TEXT NOT NULL,
       numdd INTEGER NOT NULL,
       ratiocd REAL NOT NULL,
       cachemem INTEGER NOT NULL,
       ratioread REAL NOT NULL,
       block INTEGER NOT NULL,
       diskcache INTEGER NOT NULL
);
''')

        cursor.execute('''
CREATE TABLE cachedisk (
       meta_id INTEGER NOT NULL,
       type TEXT NOT NULL,
       elapsed_hour REAL NOT NULL,
       joule REAL NOT NULL,
       kWh REAL NOT NULL,
       FOREIGN KEY (meta_id) REFERENCES meta(id)
);
''')

        cursor.execute('''
CREATE TABLE cachehit (
       meta_id INTEGER NOT NULL,
       type TEXT NOT NULL,
       storage TEXT NOT NULL,
       hit INTEGER NOT NULL,
       total INTEGER NOT NULL,
       ratio REAL NOT NULL,
       FOREIGN KEY (meta_id) REFERENCES meta(id)
);
''')

        cursor.execute('''
CREATE TABLE datadisk (
       meta_id INTEGER NOT NULL,
       type TEXT NOT NULL,
       elapsed_hour REAL NOT NULL,
       joule REAL NOT NULL,
       kWh REAL NOT NULL,
       spinup INTEGER NOT NULL,
       spindown INTEGER NOT NULL,
       FOREIGN KEY (meta_id) REFERENCES meta(id)
);
''')

        cursor.execute('''
CREATE TABLE overflow (
       meta_id INTEGER NOT NULL,
       count INTEGER NOT NULL,
       time TEXT NOT NULL,
       FOREIGN KEY (meta_id) REFERENCES meta(id)
);
''')

        cursor.execute('''
CREATE TABLE performance (
       meta_id INTEGER NOT NULL,
       avg_resp REAL NOT NULL,
       throughput REAL NOT NULL,
       bandwidth REAL NOT NULL,
       numreq INTEGER NOT NULL,
       numread INTEGER,
       numwrite INTEGER,
       FOREIGN KEY (meta_id) REFERENCES meta(id)
);
''')
