#!/usr/bin/python3
# -*-  coding: utf-8 -*-

import os
import signal
import sys
import argparse
import pymysql
import time

#默认值
#pt-online-schema-change -h127.0.0.1 -P 3306 -uroot -p'xxxxx' --charset=utf8 
#--recursion-method=none --alter "ENGINE=InnoDB "  D=test,t=table1 --critical-load Threads_connected:6000,Threads_running:800 --max-load 
#Threads_connected:5000,Threads_running:20 --set-vars lock_wait_timeout=3,innodb_lock_wait_timeout=3 --execute --chunk-size=100

class PtOscVar(object):

    def __init__(self): 
        self.port = ps.port
        self.charset = 'utf8'
        self.user = 'xxx'
        self.password = 'xxx'
        self.recursion_method = 'none'
        self.critical_load = 'Threads_connected:10000,Threads_running:800'
        self.max_load = 'Threads_connected:8000,Threads_running:500'
        self.set_vars = 'lock_wait_timeout=3,innodb_lock_wait_timeout=3'
        self.chunk_size = 100
        self.pt_bin = '/data1/xuzong/percona-toolkit-3.1/bin/pt-online-schema-change'
        self.ddl = ps.sql
        self.db = ps.db
        self.table = ps.table 
        if ps.host:
            self.domain = ps.host
        else:
            self.domain = 'xxxx'
        if ps.mode == "dry" :
            self.mode = '--dry-run'
        elif ps.mode == "exec":
            self.mode = '--execute'
        else:
            self.mode = ps.mode
        if ps.critical_load:
            self.critical_load = ps.critical_load
        if ps.max_load:
            self.max_load = self.critical_load
        if ps.set_vars:
            self.set_vars = ps.set_vars
        if ps.chunk_size :
            self.chunk_size = ps.chunk_size
        if ps.bin_dir:
            self.pt_bin = ps.bin_dir

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def MyConn(self, sql):
        results = []
        try:
            conn = pymysql.connect(host=self.domain, port=int(self.port), user=self.user, passwd=self.password , db=self.db,
                                   cursorclass=pymysql.cursors.DictCursor)
            conn.set_charset('utf8')
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            results = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            print(e)
        return results
    
    def signal_handler(self, signal, frame):
        #print ('\n大傻春,你要干神马!\n')
        tri_del = """`%s`.`pt_osc_%s_%s_del`""" %(self.db, self.db, self.table)
        tri_upd = """`%s`.`pt_osc_%s_%s_upd`""" %(self.db, self.db, self.table)
        tri_ins = """`%s`.`pt_osc_%s_%s_ins`""" %(self.db, self.db, self.table)
        db_table = """`%s`.`_%s_new`""" %(self.db, self.table)
        sql_list=["DROP TRIGGER IF EXISTS %s;"%tri_del,"DROP TRIGGER IF EXISTS %s;"%tri_upd,"DROP TRIGGER IF EXISTS %s;"%tri_ins,"DROP TABLE IF EXISTS %s;"%db_table]
        #print(sql_list)
        #[print(self.MyConn(sql)) for sql in sql_list]
        for sql in sql_list:
            print(sql)
            print(self.MyConn(sql))
        if self.key == 1:
            self.key = 0
            #self.Run()
        else:
            sys.exit(0)

    def check(self):
        sql_list = ["""show tables from %s like '_%s_new';"""%(self.db, self.table), """show tables from %s like 'pt_osc_%s_%s_%s';"""%(self.db, self.db, self.table, '%')]
        #print(self.MyConn(sql)) 
        for sql in sql_list:
            if len(self.MyConn(sql)) != 0:
                print('存在历史new表或者触发器,默认会调用删除并重试')
                self.key = 1
                self.signal_handler('1','1')

    def Run(self):
        self.check()
        cmd = """%s -h %s -P %s -u%s -p'%s' --charset=%s --recursion-method=%s --alter "%s" D=%s,t=%s --critical-load %s --max-load %s --set-vars %s --chunk-size=%s %s""" %(self.pt_bin, self.domain, self.port, self.user, self.password, self.charset, self.recursion_method, self.ddl, self.db, self.table, self.critical_load, self.max_load, self.set_vars, self.chunk_size, self.mode)
        print(cmd)
        print(os.popen(cmd).read())
        self.check()
        

if __name__ == '__main__':

    parse = argparse.ArgumentParser(description='Pt-osc Management Tool Gather')

    parse.add_argument('--host', '-H', help='service host')
    parse.add_argument('--port', '-p', required=True, help='service port')
    parse.add_argument('--sql', '-sql', required=True, help='ddl sql')
    parse.add_argument('--db', '-d', required=True, help='database')
    parse.add_argument('--table', '-t', required=True, help='table')
    parse.add_argument('--mode', '-m', required=True, help='exec mode',nargs='?',const='--execute')
    parse.add_argument('--critical-load', '-cl', help='table')
    parse.add_argument('--max-load', '-ml', help='max-load')
    parse.add_argument('--set-vars', '-st', help='set-vars')
    parse.add_argument('--chunk-size', '-s', help='chunk-size')
    parse.add_argument('--bin_dir', '-bin', help='bin dir')
    
    ps=parse.parse_args()

    
    PtOscVar().Run()
    time.sleep(1)
