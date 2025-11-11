#!/usr/bin/python3
# -*-  coding: utf-8 -*-

import os
import signal
import sys
import argparse
import pymysql
import time
import re

from subprocess import check_output
from mmap import mmap
from threading import Thread
from Queue import Queue

from pt_ddl import PtOscVar


def checkip(ip):
    return re.search(r'^((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)$',ip)

def all_tables_in_port(port, user, host, passwd):
    con = pymysql.connect(user=user, passwd=passwd,
                host=host, port=port)
    cur = con.cursor()
    cur.execute(r'''select table_schema, table_name from information_schema.tables where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') and table_schema like 'oid_uuid_%';''')
    rows = cur.fetchall()
    cur.close()
    con.close()
    return ['%s.%s' % (s, t) for s, t in rows]

def do_optimize_table(user, passwd, port):
    hostlist = ['1','2'] #ip列表
    # for tag in tagnum:
    #     dns="m"+str(port)+"i."+tag+".grid.sina.com.cn"
    #     cmd = """dig +short %s""" % dns
    #     ip = os.popen(cmd).read()
    #     if ip:
    #         hostlist.append(ip)
    #  # 这里再套一层的原因是因为有的端口有CNAME域名,有的没有. CNAME解析结果举例
    #  # m33060i.eos.gridcom.cn.
    #  # 10.75.1.1
    #  # 而非CNAME域名解析结果却没有域名
    #  # 10.75.1.1
    for host in hostlist:
        if checkip(host):
            host = host.split('\n')[0]
            break
    #print(host,port)
    tables = all_tables_in_port(port, user, host, passwd)
    for table_name in tables:
        cmd = """./pt_ddl.py -p %s -sql 'engine=innodb ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 '  -d %s -t %s -m exec > pt_test.log"""%(port,table_name.split('.')[0],table_name.split('.')[1]) 
        print(cmd)
        os.system(cmd)
        

def optimize_table_thread(user, passwd, port_queue):
    while not port_queue.empty():
        port = port_queue.get()
        do_optimize_table(user, passwd, port)

if __name__ == '__main__':
    portlist= [3306,3307] #按照端口并行
    user="xxxxx"
    passwd="xxxxx"
    port_queue = Queue()
    [port_queue.put(t) for t in portlist]
    thread_list = [Thread(target=optimize_table_thread, args=(user, passwd,
                port_queue)) for i in range(len(portlist))]
    [t.start() for t in thread_list]
    [t.join() for t in thread_list]
