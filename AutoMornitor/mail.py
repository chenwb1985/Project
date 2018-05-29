#-*-coding:utf-8-*-  
#name:mail.py
#========================================== 
# 导入smtplib和MIMEText 
#========================================== 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
import smtplib 
import time

#========================================== 
# 发送邮件 
#========================================== 
def send_mail(ora,mail_host,mail_user,mail_pass,mailto_list,att_file_path):
    try: 
        #取得服务器报警信息
        sql = '''select host, warning_type, warning, set_val, monitor_val, monitor_time
                  from (select host,
                               warning_type,
                               warning,
                               set_val,
                               monitor_val,
                               to_char(monitor_time, 'yyyy-mm-dd hh:mi:ss') monitor_time
                          from server_warning_info
                         where del_flg = '%s'
                           and send_flg = '%s'
                        union
                        select host,
                               '5' as warning_type,
                               '索引失效' as warning,
                               user_name as set_val,
                               index_name as monitor_val,
                               to_char(monitor_time, 'yyyy-mm-dd hh:mi:ss') monitor_time
                          from index_warning_info
                         where del_flg = '%s'
                           and send_flg = '%s')
                 order by host, warning_type
                      ''' % ('0','0','0','0')
        keys = ['host','warning_type','warning','set_val','monitor_val','monitor_time']
        lst = ora.Query(sql, keys)

        #警告信息存在时
        if lst:
            #生成文件
            file_name = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time())) + '.csv'
            ora.Export(sql,att_file_path + file_name,',')

            #构造邮件信息
            msgRoot = MIMEMultipart('related')  
            msgRoot['Subject'] = '服务器监控警告'
            msgRoot['From'] = mail_user 
            msgRoot['To'] = ";".join(mailto_list)

            #构造附件  
            att = MIMEText(open(att_file_path + file_name, 'rb').read(), 'base64', 'utf-8')  
            att["Content-Type"] = 'application/octet-stream'  
            att["Content-Disposition"] = '''attachment; filename=%s''' % (file_name)
            msgRoot.attach(att) 

            #将发送状态改为已发送
            #开始事务
            #ora.begin()
            updsql = '''update server_warning_info set send_flg = %s where del_flg = %s and send_flg = %s''' % ('1','0','0')
            ora.Exec(updsql)
            updsql4index = '''update index_warning_info set send_flg = %s where del_flg = %s and send_flg = %s''' % ('1','0','0')
            ora.Exec(updsql4index)

            #发送邮件
            s = smtplib.SMTP() 
            s.connect(mail_host) 
            s.login(mail_user,mail_pass) 
            s.sendmail(mail_user, mailto_list, msgRoot.as_string()) 
            s.close() 

            #发送成功后，提交事务
            ora.commit()
            print "发送成功"
    except Exception, e: 
        #回滚事务
        ora.rollback()
        print str(e) 
        print "发送失败"
