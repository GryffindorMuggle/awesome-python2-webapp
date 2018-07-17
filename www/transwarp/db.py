#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''数据库连接操作模块，封装数据库操作API
'''
import time
import uuid
import functools
import threading

engine = None

def next_id(t=None):
	"""
	生成一个唯一的ID，由当前时间+伪随机数拼接得到
	"""
	if t is None:
		t=time.time()
	return '%015d%s000' % (int(t*1000),uuid.uuid4().hex)

def create_engine(user,password,database,host='127.0.0.1',port='3306',**kw):
	"""
	db模块的核心函数，用于数据库连接，生成全局对象engine，engine对象持有数据库连接
	"""
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine  is already initialized.')
	params = dict(user=user,password= password,database=database,host=host,port=port)
	defaults = dict(use_unicode= True,charset='utf8',collation='utf8_general_ci',autocommit=False)
	for k,v in  defaults.iteritems():
		params[k] = kw.pop(k,v)
	params.update(kw)
	params['buffered'] = True
	engine = _Engine(lambda:mysql.connector.connect(**params))

def conneciton():
	"""
	db模块核心函数，用于获取一个数据库的连接
	通过_ConectionCtx对_db_ctx的封装，使得惰性连接可以自动获取和释放，
	也就是可以使用with语法来处理数据库连接
	 _ConnectionCtx    实现with语法
    ^
    |
    _db_ctx           _DbCtx实例
    ^
    |
    _DbCtx            获取和释放惰性连接
    ^
    |
    _LasyConnection   实现惰性连接
	"""
	return _ConnectionCtx()

def with_connection(func):
	"""
	设计一个装饰器，替换with语法
	比如:
        @with_connection
        def foo(*args, **kw):
            f1()
            f2()
            f3()
	"""
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper

def transaction():
    """
    db模块核心函数 用于实现事物功能
    支持事物:
        with db.transaction():
            db.select('...')
            db.update('...')
            db.update('...')
    支持事物嵌套:
        with db.transaction():
            transaction1
            transaction2
            ...
    """
    return _TransactionCtx()

def with_transaction(func):
	"""
	设计一个装饰器 替换with语法，让代码更优雅
    比如:
        @with_transaction
        def do_in_transaction():
    >>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select_one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
	"""
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _TransactionCtx():
			func(*args,**kw)
	return _wrapper

@with_connection
def _select(sql,first,*args):
	"""
	执行SQL，返回一个结果或者多个结果组成的列表
	"""
	global _db_ctx
	cursor = None
	sql = sql.replace("?","%s")
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if  first:
			values = cursor.fetchone()
			if not values:
				return None
			return  Dict(names,values)
		return [Dict(name,x )  for  x in cursor.fetcheall()]
	finally:
		if cursor:
			cursor.close()

def select_one(sql,*args):
	"""
	执行SQL返回一个结果
	没有结果报错
	只有一个结果，返回一个结果
	有多个结果， 只返回一个结果
	"""
	return _select(sql,True,*args)

def select_int(sql,*args):
	"""
	执行SQL返回一个数值，当返回多个数值将触发异常
	"""
	d = _select(sql,True,*args)
	if len(d) != 1:
		raise MultiColumnsError('Except only one column.')
	return d.values()[0]

def select(sql,*args):
	"""
	以列表的形式返回结果
	"""
	return _select(sql,False,*args)

@with_connection
def _update(sql,*args):
	"""
	执行update语句，返回update行数
	"""
	global _db_ctx
	cursor = None
	sql  = sql.replace('?','%s')
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,args)
		r = cursor.rowcount
		if _db_ctx.transactions == 0:
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def update(sql,*args):
	"""
	执行update函数，返回update行数
	"""
	return _update(sql,*args)

def insert(table,**kw):
	"""
	执行INSERT语句
	"""
	cols,args=zip(*kw.iteritems())
	sql = "insert into `%s` (%s) values (%s)" %(table,','.join(['%s'] % col for col in cols), ','.join(['?'  for i in range(len(cols))]))
	return _update(sql,*args)

class Dict(dict):
	"""
	字典对象
	实现简单的可以通过属性访问的字典，比如x.key = value
	"""
	def __init__(self,names=(),values=(),**kw):
		super(Dict,self).__init__(**kw)
		for k ,v in zip(names,values):
			self[k] =v
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no Attribute '%s'" % key)

	def __setattr__(self,key,value):
		self[key] = value

class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass

class _Engine(object):
	"""
	数据库引擎,
	用于保存db模块的核心函数：create_engine创建出来的数据路连接
	"""
	def __init__(self,connect):
		self._connect=connect

	def connect(self):
		return self._connect

class _LasyConnection(object):
	"""
	惰性连接对象，
	仅当需要cursor对象时，才连接数据库，获取连接
	"""
	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			_connection = engine.connect()
			self.connection = _connection
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()

	def cleanup(self):
		if self.connection:
			_connection = self.connection
			self.connection = None
			_connection.close()

class _DbCtx(threading.local):
	"""
	db模块的核心对象，数据库连接的上下文对象，负责从数据库获取和释放连接取得的连接时惰性连接对象，
	因此只有调用cursor对象时，才会真正获取数据库连接，该对象是一个Thread local 对象，
	因此绑定在此对象上的数据仅对本线程可见
	"""
	def __init__(self,):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None

	def init(self):
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection = None

	def cursor(self):
		return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	"""
	因为_DbCtx实现了连接的获取和释放，并没有实现连接的自动获取和释放，_ConnectCtx 在_DbCtx基础上实现了该功能，
	因此可以对_ConnectCtx使用with语法，比如：
	with connection():
		pass
		with connection():
			pass
	"""
	def __enter__(self):
		"""
		获取一个惰性连接对象
		"""
		global _db_ctx
		self.should_cleanup =  False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self,exctype,excvalue,traceback):
		"""
		释放连接
		"""
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

	# def connection():
	# 	return _ConnectionCtx()

class _TransactionCtx(object):
	"""
	事物嵌套比connection嵌套复杂，事物嵌套需要计数，每遇到一层嵌套计数+1，离开一层嵌套就-1,计数为0时提交事物
	"""
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions += 1
		return  self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions -= 1
		try:
			if _db_ctx.transactions == 0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.commit()
		except:
			_db_ctx.rollback()
			raise

	def rollback():
		global _db_ctx
		_db_ctx.rollback()

if __name__ =='__main__':
	create_engine('root','root123','test','127.0.0.1')
	update('drop table if exists user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
	import doctest
	doctest.testmod()
