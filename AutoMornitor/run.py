#coding:utf-8
from cxOracle import *
from threading import Timer
import mod_config
import mail
import mornitor

#DB配置取得
host = mod_config.getConfig("DATABASE","DB_HOST")  #HOST名
port = mod_config.getConfig("DATABASE","DB_PORT")  #端口号
sid  = mod_config.getConfig("DATABASE","DB_SID")   #SID
user = mod_config.getConfig("DATABASE","DB_USER")  #用户名
pwd  = mod_config.getConfig("DATABASE","DB_PASS")  #密码
tns = '''(DESCRIPTION=
            (ADDRESS_LIST=
                (ADDRESS=(PROTOCOL=TCP)
                        (HOST=%s)(PORT=%s)))
                        (CONNECT_DATA=(SERVICE_NAME=%s)))''' % ( host, port, sid)


#MAIL配置取得
mail_host = mod_config.getConfig("MAIL","MAIL_HOST")  #MAIL_HOST名
mail_user = mod_config.getConfig("MAIL","MAIL_USER")  #MAIL_USER名
mail_pass = mod_config.getConfig("MAIL","MAIL_PASS")  #MAIL_PASS
mailto_list = mod_config.getConfig("MAIL","MAILTO_LIST").split(",")  #邮件发送列表
att_file_path = mod_config.getConfig("MAIL","ATT_FILE_PATH")         #附件文件路径

#监控阈值取得
cpu_val = mod_config.getConfig("SETTING_VAL","CPU_VAL")       #CPU使用率阈值
disk_val = mod_config.getConfig("SETTING_VAL","DISK_VAL")     #硬盘使用率阈值
memory_val = mod_config.getConfig("SETTING_VAL","MEM_VAL")    #内存使用率阈值
tablespace_val = mod_config.getConfig("SETTING_VAL","TS_VAL") #ORACLE表空间使用率阈值

#网络列表
ping_list = mod_config.getConfig("PING_LIST","PING_LIST")

#定时监控(CPU、内存)
def loop_mornitor_status():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #数据监控
    mornitor.monitor_status(ora,cpu_val,memory_val)
    #关闭oracle连接
    ora.Close()
    print "服务器监控【CPU、内存】"
    
    #定时监控启动(CPU、内存)
    Timer(60, loop_mornitor_status).start()

#定时监控(硬盘)
def loop_mornitor_disk():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #数据监控
    mornitor.monitor_disk(ora,disk_val)
    #关闭oracle连接
    ora.Close()
    print "服务器监控【硬盘】"
    
    #定时监控启动(硬盘)
    Timer(1830, loop_mornitor_disk).start()

#定时监控(DB表空间)
def loop_mornitor_ts():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #数据监控
    mornitor.monitor_tablespace(ora,tablespace_val)
    #关闭oracle连接
    ora.Close()
    print "服务器监控【DB表空间】"
    
    #定时监控启动(DB表空间)
    Timer(1845, loop_mornitor_ts).start()

#定时监控(索引失效)
def loop_mornitor_index():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #数据监控
    mornitor.monitor_index(ora)
    #关闭oracle连接
    ora.Close()
    print "服务器监控【索引失效】"
    
    #定时监控启动(DB表空间)
    Timer(625, loop_mornitor_index).start()

#定时监控(网络状态)
def loop_mornitor_net():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #数据监控
    mornitor.monitor_net(ora,ping_list)
    #关闭oracle连接
    ora.Close()
    print "服务器监控【网络状态】"
    
    #定时监控启动(网络状态)
    Timer(1225, loop_mornitor_net).start()

#定时邮件
def loop_mail():

    #创建oracle连接
    ora = cxOracle(user, pwd, tns)
    #邮件发送
    mail.send_mail(ora,mail_host,mail_user,mail_pass,mailto_list,att_file_path)
    #关闭oracle连接
    ora.Close()
    print "邮件发送"
    
    #定时邮件启动
    Timer(3600, loop_mail).start()

#定时监控启动(CPU、内存)
Timer(60, loop_mornitor_status).start()

#定时监控启动(硬盘)
Timer(1830, loop_mornitor_disk).start()

#定时监控启动(DB表空间)
Timer(1845, loop_mornitor_ts).start()

#定时监控启动(索引失效)
Timer(625, loop_mornitor_index).start()

#定时监控启动(网络状态)
Timer(1225, loop_mornitor_net).start()

#定时邮件启动
Timer(3600, loop_mail).start()