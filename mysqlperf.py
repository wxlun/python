#!/usr/bin/python
#coding=utf-8

import MySQLdb
import MySQLdb.cursors

conn = MySQLdb.connect(host='172.16.103.210',user='wxlun',passwd='wxlun',cursorclass = MySQLdb.cursors.DictCursor)
cursor = conn.cursor()
show_max_connections = cursor.execute('show global variables like "max_connections"')
global_max_connections = cursor.fetchall()
max_connections_dict = {}
for i in global_max_connections:
    max_connections_dict[i['Variable_name']]=i['Value']
show_global_status=cursor.execute('show global status')
global_status = cursor.fetchall()
global_status_dict={}
for i in global_status:
    global_status_dict[i['Variable_name']]=i['Value']
cursor.close()
conn.close()
print '------------------------------------------'
print '通过计算多种状态的百分比看MySQL的性能情况:'
print '------------------------------------------'
print '---读写百分比'
com_select=int(global_status_dict['Com_select'])
com_insert=int(global_status_dict['Com_insert'])
com_update=int(global_status_dict['Com_update'])
com_delete=int(global_status_dict['Com_delete'])
print '服务器启动到目前查询: %s, 插入: %s, 更新: %s, 删除: %s' %(com_select,com_insert,com_update,com_delete)
read_rate = round(com_select * 1.0 / (com_select + com_insert + com_update + com_delete), 4) * 100  
write_rate = round((com_insert + com_update + com_delete) * 1.0 / (com_select + com_insert + com_update + com_delete), 4) * 100
print '读百分比: %s%%    写百分比: %s%%' %(read_rate,write_rate)

print '---慢查询比例'
slow_queries=int(global_status_dict['Slow_queries'])
slow_query_rate = round(slow_queries * 1.0 / (com_select + com_update + com_delete), 4) * 100
print '服务器启动到目前慢查询: %s, 慢查询比例: %s%%' %(slow_queries,slow_query_rate)

print '---连接数检查'
max_connections = int(max_connections_dict['max_connections'])
print '数据库运行的最大连接数: %s' %max_connections
connections = int(global_status_dict['Connections'])
print '数据库运行到目前总共被连接: %s' %connections
threads_connected = int(global_status_dict['Threads_connected'])
print '当前连接数: %s' %threads_connected
threads_running = int(global_status_dict['Threads_running'])
print '当前正在运行的连接数: %s' %threads_running
max_used_connections = int(global_status_dict['Max_used_connections'])
print '最大一次的连接数: %s' %max_used_connections
threads_created = int(global_status_dict['Threads_created'])
print '创建连接线程的次数: %s' %threads_created
current_thread_rate = round((threads_connected * 1.0 / max_connections), 4) * 100
print '当前连接数的比例：%s%%' %(current_thread_rate)
max_thread_rate = round((max_used_connections * 1.0 / max_connections), 4) * 100
print '最大一次的连接数比例: %s%%' %(current_thread_rate)
connect_thread_rate = round((connections - threads_created) * 1.0 / connections, 4) * 100
print '连接线程缓存命中率: %s%%' %(connect_thread_rate)

print '---表缓存'
table_open_cache_misses = int(global_status_dict['Table_open_cache_misses'])
print '新打开的表的次数: %s' %table_open_cache_misses
table_open_cache_hits = int(global_status_dict['Table_open_cache_hits'])
print '从表缓存中拿已打开的表的次数: %s' %table_open_cache_hits
opened_tables = int(global_status_dict['Opened_tables'])
print '打开表的总次数: %s' %opened_tables

com_table_hit_rate = round(table_open_cache_misses * 1.0 / (table_open_cache_misses + table_open_cache_hits) ,4) * 100
print '连接线程缓存命中率: %s%%' %(com_table_hit_rate)

print '---临时表'
created_tmp_disk_tables = int(global_status_dict['Created_tmp_disk_tables'])
print '磁盘上创建临时表的次数: %s' %created_tmp_disk_tables
created_tmp_tables = int(global_status_dict['Created_tmp_tables'])
print '创建临时表的总次数: %s' %created_tmp_tables
create_dtmp_table_rate = round(created_tmp_disk_tables * 1.0 / created_tmp_tables, 4) * 100
print '在磁盘上创建临时表的比例：%s%%' %(create_dtmp_table_rate)

print '---额外的排序'
sort_merge_passes = int(global_status_dict['Sort_merge_passes'])
print '磁盘中进行额外排序的次数: %s' %sort_merge_passes
sort_scan = int(global_status_dict['Sort_scan'])
print '通过表扫描进行排序的总次数: %s' %sort_scan
create_disk_sort_rate = round(sort_merge_passes * 1.0 / sort_scan, 4) * 100
print '磁盘排序的比例%s%%' %(create_disk_sort_rate)


print '---binlog缓冲'
binlog_cache_disk_use = int(global_status_dict['Binlog_cache_disk_use'])
print '在磁盘上创建临时文件用于保存 binlog 的次数: %s' %binlog_cache_disk_use
binlog_cache_use = int(global_status_dict['Binlog_cache_use'])
print '缓冲binlog的总次数: %s' %binlog_cache_use
binlog_cache_rate = round(binlog_cache_disk_use * 1.0 / binlog_cache_use, 4) * 100
print '磁盘上创建临时文件保存 binlog 的比例%s%%' %(binlog_cache_rate)

print '---redo日志'
innodb_log_waits = int(global_status_dict['Innodb_log_waits'])
print 'innodb redo日志等待缓冲区刷新的次数: %s' %innodb_log_waits

print '---innodb缓存'
innodb_buffer_pool_read_requests = int(global_status_dict['Innodb_buffer_pool_read_requests'])
print '读取页的总次数: %s' %innodb_buffer_pool_read_requests

innodb_buffer_pool_reads = int(global_status_dict['Innodb_buffer_pool_reads'])
print '从磁盘读取页的次数: %s' %innodb_buffer_pool_reads

innodb_hit = round((int(innodb_buffer_pool_read_requests)-int(innodb_buffer_pool_reads)) * 1.0 / int(innodb_buffer_pool_read_requests) ,4) * 100
print 'innodb缓存命中率: %s%%' %(innodb_hit)
