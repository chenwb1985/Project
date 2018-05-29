#-*-coding:utf-8-*-  
#name:mornitor.py

from cxOracle import *
from time import strftime,gmtime
import time
import os,sys,re
import salt.client
import subprocess

#监控各主机状态(CPU和内存)
def monitor_status(ora,cpu_val,memory_val):
    try:
        #saltStack本地API
        local = salt.client.LocalClient()

        #CPU使用率
        cpuinfo = local.cmd('*','cmd.run',['cat /proc/stat'])
        for i in cpuinfo:
            if cpuinfo[i]:
                CPU = cpuinfo[i].split()
                total = float(CPU[1])+float(CPU[2])+float(CPU[3])+float(CPU[4])+float(CPU[5])+float(CPU[6])+float(CPU[7])
                perc = round(((float(CPU[1])+float(CPU[2])) / total) * 100, 2)

                #服务器信息
                id = create_id()
                server_info = [id]
                server_info.append(i)
                server_info.append('1') #CPU使用率
                server_info.append('CPU使用率')
                server_info.append(str(perc) +"%")
                #服务器信息插入
                insert_server_info(ora,server_info)

                #超过阈值时，插入服务器警告信息表
                if perc > float(cpu_val):
                    insert_server_warning_info(ora,id,cpu_val)

        #内存信息
        minfo = local.cmd('*','cmd.run',['cat /proc/meminfo'])
        for i in minfo:
            if minfo[i]:
                MOM = minfo[i].split()
                total = float(MOM[1])
                free = float(MOM[4])
                #四舍五入保留2位小数
                perc = round(((total - free)/ total ) * 100, 2)

                #服务器信息
                id = create_id()
                server_info = [id]
                server_info.append(i)
                server_info.append('3') #内存使用率
                server_info.append('内存使用率')
                server_info.append(str(perc) +"%")
                #服务器信息插入
                insert_server_info(ora,server_info)

                #超过阈值时，插入服务器警告信息表
                if perc > float(memory_val):
                    insert_server_warning_info(ora,id,memory_val)

        #事务提交
        ora.commit()
    except Exception, e:
        #事务回滚
        ora.rollback()
        print str(e)

#监控硬盘信息
def monitor_disk(ora,disk_val):
    try:
        #saltStack本地API
        local = salt.client.LocalClient()

        #硬盘信息
        d_dict = {}
        dfinfo = local.cmd('*','cmd.run',['df -h'])
        for i in dfinfo:
            if dfinfo[i]:
                DISK = dfinfo[i].split()
                #循环次数
                loop = (len(DISK) - 7)/6
                d_list = []
                for j in range(1,loop + 1):
                    dict = {}
                    dict['Filesystem'] = DISK[j*6+1]
                    dict['Size'] = DISK[j*6+2]
                    dict['Used'] = DISK[j*6+3]
                    dict['Avail'] = DISK[j*6+4]
                    dict['Use%'] = DISK[j*6+5]
                    dict['Mountedon'] = DISK[j*6+6]
                    d_list.append(dict)
                d_dict[i] = d_list

        #取得系统时间
        systime = strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        #将检测到的数据登录到表中
        for k in d_dict:
            lst = d_dict[k]
            seq = 1
            for m in lst:
                #创建id
                id = create_id()
                #硬盘信息
                disk_info = [id]
                disk_info.append(k)
                disk_info.append(seq)
                disk_info.append(m['Filesystem'])
                disk_info.append(m['Size'])
                disk_info.append(m['Used'])
                disk_info.append(m['Avail'])
                disk_info.append(m['Use%'])
                disk_info.append(m['Mountedon'])
                disk_info.append(systime)
                seq = seq + 1
                #硬盘信息插入
                insert_disk_info(ora,disk_info)
                used = m['Use%'].replace('%','')
                #超过阈值时，插入服务器警告信息表
                if float(used) > float(disk_val):
                    insert_disk_warning_info(ora,id,m['Filesystem'] + "分区使用率",disk_val,k,seq-1)

        #事务提交
        ora.commit()
    except Exception, e:
        #事务回滚
        ora.rollback()
        print str(e)

#DB服务器表空间信息
def monitor_tablespace(ora,ts_val):
    try:
        #Oracle主机列表查询
        host_list = ora.Query("select priip1 as hostname from hosts t1 inner join options t2 on(t1.role = t2.id and t2.val = 'Oracle') order by hostname",["hostname"])
        for i in host_list:
            host = i['hostname']
            #数据库配置取得
            db_info = ora.Query('''select db_host,db_port,db_sid,db_user,db_password,ts_id from db_info where db_host = '%s' ''' % (host),["db_host","db_port","db_sid","db_user","db_password","ts_id"])
            #监控表空间名取得
            if db_info:
                ts_list = ora.Query('''select ts_name from tablespaces where ts_id = '%s' and ctl_flag = 1 ''' % (db_info[0]['ts_id']),["ts_name"])
                #需要监控的表空间有时
                if ts_list:
                    #TNS字符串
                    tns = '''(DESCRIPTION=
                                (ADDRESS_LIST=
                                    (ADDRESS=(PROTOCOL=TCP)
                                        (HOST=%s)(PORT=%s)))
                                        (CONNECT_DATA=(SERVICE_NAME=%s)))''' % ( db_info[0]['db_host'], db_info[0]['db_port'], db_info[0]['db_sid'])
                    #创建oracle连接
                    ora_m = cxOracle(db_info[0]['db_user'], db_info[0]['db_password'], tns)
                    

                    #查询表空间的使用率
                    for j in ts_list:
                        used = ora_m.Query(''' 
                                         select a.tablespace_name, round(((total - free) / total) * 100, 2) as used
                                          from (select tablespace_name, sum(bytes) / 1024 / 1024 / 1024 as total
                                                  from dba_data_files
                                                 group by tablespace_name) a,
                                               (select tablespace_name, sum(bytes) / 1024 / 1024 / 1024 as free
                                                  from dba_free_space
                                                 group by tablespace_name) b
                                         where a.tablespace_name = b.tablespace_name
                                         and a.tablespace_name = '%s' ''' % (j['ts_name']),["tablespace_name","used"])
                        if used:
                            #服务器信息
                            id = create_id()
                            server_info = [id]
                            server_info.append(db_info[0]['db_host'])
                            server_info.append('4') #表空间使用率
                            server_info.append(j['ts_name'] + '空间使用率')
                            server_info.append(str(used[0]['used']) +"%")
                            #服务器信息插入
                            insert_server_info(ora,server_info)

                            #超过阈值时，插入服务器警告信息表
                            if float(used[0]['used']) > float(ts_val):
                                insert_server_warning_info(ora,id,ts_val)
                    #关闭连接
                    ora_m.Close()

        #事务提交
        ora.commit()
    except Exception, e:
        #事务回滚
        ora.rollback()
        print str(e)

#DB索引状态监测
def monitor_index(ora):
    try:
        #Oracle主机列表查询
        host_list = ora.Query("select priip1 as hostname from hosts t1 inner join options t2 on(t1.role = t2.id and t2.val = 'Oracle') order by hostname",["hostname"])
        for i in host_list:
            host = i['hostname']
            #数据库配置取得
            db_info = ora.Query('''select db_host,db_port,db_sid,db_user,db_password,ts_id from db_info where db_host = '%s' ''' % (host),["db_host","db_port","db_sid","db_user","db_password","ts_id"])
            #失效索引查询
            if db_info:
                #TNS字符串
                tns = '''(DESCRIPTION=
                            (ADDRESS_LIST=
                                (ADDRESS=(PROTOCOL=TCP)
                                    (HOST=%s)(PORT=%s)))
                                    (CONNECT_DATA=(SERVICE_NAME=%s)))''' % ( db_info[0]['db_host'], db_info[0]['db_port'], db_info[0]['db_sid'])
                #创建oracle连接
                ora_m = cxOracle(db_info[0]['db_user'], db_info[0]['db_password'], tns)

                #查询SQL
                sql = '''   Select owner as index_owner, index_name
                              From dba_indexes
                             where status = 'UNUSABLE'
                               and owner not in ('SYS',
                                                 'SYSTEM',
                                                 'SYSMAN',
                                                 'EXFSYS',
                                                 'WMSYS',
                                                 'OLAPSYS',
                                                 'OUTLN',
                                                 'DBSNMP',
                                                 'ORDSYS',
                                                 'ORDPLUGINS',
                                                 'MDSYS',
                                                 'CTXSYS',
                                                 'AURORA$ORB$UNAUTHENTICATED',
                                                 'XDB',
                                                 'FLOWS_030000',
                                                 'FLOWS_FILES')
                            union
                            select index_owner, index_name
                              from dba_ind_partitions
                             where status = 'UNUSABLE'
                               and index_owner not in ('SYS',
                                                       'SYSTEM',
                                                       'SYSMAN',
                                                       'EXFSYS',
                                                       'WMSYS',
                                                       'OLAPSYS',
                                                       'OUTLN',
                                                       'DBSNMP',
                                                       'ORDSYS',
                                                       'ORDPLUGINS',
                                                       'MDSYS',
                                                       'CTXSYS',
                                                       'AURORA$ORB$UNAUTHENTICATED',
                                                       'XDB',
                                                       'FLOWS_030000',
                                                       'FLOWS_FILES')
                            union
                            Select Index_Owner, Index_Name
                              From DBA_IND_SUBPARTITIONS
                             Where status = 'UNUSABLE'
                               and index_owner not in ('SYS',
                                                       'SYSTEM',
                                                       'SYSMAN',
                                                       'EXFSYS',
                                                       'WMSYS',
                                                       'OLAPSYS',
                                                       'OUTLN',
                                                       'DBSNMP',
                                                       'ORDSYS',
                                                       'ORDPLUGINS',
                                                       'MDSYS',
                                                       'CTXSYS',
                                                       'AURORA$ORB$UNAUTHENTICATED',
                                                       'XDB',
                                                       'FLOWS_030000',
                                                       'FLOWS_FILES')
                             '''
                index_list = ora_m.Query(sql,["index_owner","index_name"])
                #将失效索引插入索引信息表
                for j in index_list:
                    #索引信息
                    id = create_id()
                    index_info = [id]
                    index_info.append(db_info[0]['db_host'])
                    index_info.append(db_info[0]['db_sid'])
                    index_info.append(db_info[0]['db_user'])
                    index_info.append(j['index_owner'])
                    index_info.append(j['index_name'])
                    #失效索引信息插入
                    insert_index_warning_info(ora,index_info)

                #关闭连接
                ora_m.Close()

        #事务提交
        ora.commit()
    except Exception, e:
        #事务回滚
        ora.rollback()
        print str(e)

#DB网络连通状态监测
def monitor_net(ora,ping_list):
    try:
        #对主机列表进行ping操作
        ping_list = ping_list.split(",")
        for ip in ping_list:
            p = subprocess.Popen(["ping -c 1 -w 1 "+ ip],stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
            out=p.stdout.read()
            regex=re.compile('100% packet loss')
            #网络不通时
            if len(regex.findall(out)) != 0:
                id = create_id()
                #将不通的主机信息插入表中
                insert_net_check(ora,id,ip)

        #事务提交
        ora.commit()
    except Exception, e:
        #事务回滚
        ora.rollback()
        print str(e) 

#插入服务器状态表
def insert_server_info(ora,server_info):
    try:
        #SQL文
        sql = '''insert into server_info (id,
                                          host,
                                          warning_type,
                                          warning,
                                          monitor_val,
                                          monitor_time,
                                          create_by,
                                          create_time,
                                          update_by,
                                          update_time,
                                          del_flg) 
                                        values
                                          ('%s','%s','%s','%s','%s',sysdate,'system',sysdate,'system',sysdate,'0')
        ''' % (server_info[0],server_info[1],server_info[2],server_info[3],server_info[4])
        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#插入服务器状态表
def insert_disk_info(ora,disk_info):
    try:
        #SQL文
        sql = '''insert into disk_info   (id,
                                          host,
                                          seq,
                                          filesystem,
                                          dsize,
                                          used,
                                          avail,
                                          use_perc,
                                          mountedon,
                                          monitor_time,
                                          create_by,
                                          create_time,
                                          update_by,
                                          update_time,
                                          del_flg) 
                                        values
                                          ('%s','%s',%d,'%s','%s','%s','%s','%s','%s',to_date('%s','yyyy-MM-dd hh24:mi:ss'),'system',sysdate,'system',sysdate,'0')
        ''' % (disk_info[0],disk_info[1],disk_info[2],disk_info[3],disk_info[4],disk_info[5],disk_info[6],disk_info[7],disk_info[8],disk_info[9])
        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#插入服务器警告信息表
def insert_server_warning_info(ora,id,set_val):
    try:
        #SQL文
        sql = '''insert into server_warning_info
                  select id,
                         host,
                         warning_type,
                         warning,
                         '%s',
                         monitor_val,
                         monitor_time,
                         'system',
                         sysdate,
                         'system',
                         sysdate,
                         '0',
                         '0'
                    from server_info
                   where id = '%s'
                ''' % (str(set_val)+"%",id)
        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#插入服务器警告信息表[硬盘信息]
def insert_disk_warning_info(ora,id,warning,set_val,host,seq):
    try:
        #SQL文
        sql = '''insert into server_warning_info
                  select id,
                         host,
                         '2',
                         '%s',
                         '%s',
                         use_perc,
                         monitor_time,
                         'system',
                         sysdate,
                         'system',
                         sysdate,
                         '0',
                         '0'
                    from disk_info
                   where id = '%s'
                     and host = '%s'
                     and seq = %s
                ''' % (warning,set_val+"%",id,host,seq)
        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#失效索引信息插入
def insert_index_warning_info(ora,index_info):
    try:
        #SQL文
        sql = '''insert into index_warning_info
                  (  id,
                     host,
                     sid,
                     user_name,
                     index_owner,
                     index_name,
                     monitor_time,
                     create_by,
                     create_time,
                     update_by,
                     update_time,
                     del_flg,
                     send_flg
                  ) values (
                     '%s'
                    ,'%s'
                    ,'%s'
                    ,'%s'
                    ,'%s'
                    ,'%s'
                    ,sysdate
                    ,'system'
                    ,sysdate
                    ,'system'
                    ,sysdate
                    ,'0'
                    ,'0') ''' % (index_info[0],index_info[1],index_info[2],index_info[3],index_info[4],index_info[5])

        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#网络状态check结果插入
def insert_net_check(ora,id,host):
    try:
        #SQL文
        sql = '''insert into net_check
                  (  id,
                     host,
                     status,
                     monitor_time,
                     create_by,
                     create_time,
                     update_by,
                     update_time,
                     del_flg
                  ) values (
                     '%s'
                    ,'%s'
                    ,'NG'
                    ,sysdate
                    ,'system'
                    ,sysdate
                    ,'system'
                    ,sysdate
                    ,'0') ''' % (id,host)

        #数据插入
        ora.Exec(sql)
    except Exception, e:
        raise e

#服务器信息表ID
def create_id():
    try:
        #插入用id作成
        nowTime = time.time()
        id = str(int(round(nowTime * 1000)))    #毫秒级时间戳
        return id
    except Exception, e:
        print str(e)
