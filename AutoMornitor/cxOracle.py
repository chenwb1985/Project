#coding:utf-8
import cx_Oracle
import sys  
import os  
reload(sys)  
sys.setdefaultencoding('utf8')  
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# 封装的类
class cxOracle:
    '''
    tns的取值tnsnames.ora对应的配置项的值，如：
    tns = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.139)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=ORCL)))'
    '''
    def __init__(self ,uname, upwd,tns ):
        self ._uname = uname
        self ._upwd = upwd
        self ._tns = tns
        self ._conn = None
        self ._ReConnect()

    def _ReConnect(self ):
        if not self._conn :
            self ._conn = cx_Oracle.connect (self. _uname, self ._upwd, self._tns)
        else:
            pass

    def __del__(self ):
        if self. _conn:
            self ._conn. close()
            self ._conn = None

    def _NewCursor(self ):
        cur = self. _conn.cursor ()
        if cur:
            return cur
        else:
            print "#Error# Get New Cursor Failed."
            return None

    def _DelCursor(self , cur):
        if cur:
            cur .close()

    # 检查是否允许执行的sql语句
    def _PermitedUpdateSql(self ,sql):
        rt = True
        lrsql = sql. lower()
        sql_elems = [ lrsql.strip ().split()]

        # update和delete最少有四个单词项
        if len( sql_elems) < 4 :
            rt = False
        # 更新删除语句，判断首单词，不带where语句的sql不予执行
        elif sql_elems[0] in [ 'update', 'delete']:
            if 'where' not in sql_elems :
                rt = False

        return rt

    # 导出结果为文件
    def Export(self , sql, file_name, colfg ='||'):
        rt = self. Query1(sql )
        if rt:
            with open( file_name, 'a') as fd:
                for row in rt:
                    ln_info = ''
                    for col in row:
                         ln_info += str( col) + colfg
                    ln_info += '\n'
                    fd .write( ln_info)
    # 查询1
    def Query1(self , sql, nStart=0 , nNum=- 1):
        rt = []

        # 获取cursor
        cur = self. _NewCursor()
        if not cur:
            return rt

        # 查询到列表
        cur .execute(sql)
        if ( nStart==0 ) and (nNum==1 ):
            rt .append( cur.fetchone ())
        else:
            rs = cur. fetchall()
            if nNum==- 1:
                rt .extend( rs[nStart:])
            else:
                rt .extend( rs[nStart:nStart +nNum])

        # 释放cursor
        self ._DelCursor(cur)

        return rt

    # 查询
    def Query(self , sql, keys, nStart=0 , nNum=- 1):
        rt = []
        ret = []

        # 获取cursor
        cur = self. _NewCursor()
        if not cur:
            return rt

        # 查询到列表
        cur .execute(sql)
        if ( nStart==0 ) and (nNum==1 ):
            rt .append( cur.fetchone ())
        else:
            rs = cur. fetchall()
            if nNum==- 1:
                rt .extend( rs[nStart:])
            else:
                rt .extend( rs[nStart:nStart +nNum])

        # 释放cursor
        self ._DelCursor(cur)

        #元组==>字典类型
        for i in rt:
            dictrs = dict.fromkeys(keys,"")
            index = 0
            for j in keys:
                dictrs[j] = i[index]
                index = index + 1
            ret.append(dictrs)

        return ret

    # 更新
    def Exec(self ,sql):
        # 获取cursor
        rt = None
        cur = self. _NewCursor()
        if not cur:
            return rt

        # 判断sql是否允许其执行
        #if not _PermitedUpdateSql(sql ):
        #    return rt

        # 执行语句
        rt = cur. execute(sql )

        # 释放cursor
        self ._DelCursor(cur)

        return  rt

    #开启事务
    def begin(self):

        self._conn.autocommit(0)

    #事务提交
    def commit(self):

        self._conn.commit()

    #事务回滚
    def rollback(self):

        self._conn.rollback()

    #关闭连接
    def Close(self):
        self.__del__()

