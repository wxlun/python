# coding: utf-8
import MySQLdb
import time
import os

# delete config
# 如果VIEW_OR_RUN = "VIEW",仅生成小批量删除的脚本但不执行
# 如果VIEW_OR_RUN = "RUN",生成小批量删除的脚本并直接调用执行
VIEW_OR_RUN = "VIEW"
DELETE_DATABASE_NAME = ""
DELETE_TABLE_NAME = ""
DELETE_TABLE_KEY = ""
DELETE_CONDITION = ""
DELETE_ROWS_PER_BATCH = 100000
SLEEP_SECOND_PER_BATCH = 0.5

# MySQL Connection Config
Default_MySQL_Host = '172.16.103.210'
Default_MySQL_Port = 3306
Default_MySQL_User = "wxlun"
Default_MySQL_Password = 'wxlun'

Default_MySQL_Charset = "utf8"
Default_MySQL_Connect_TimeOut = 120
Default_MySQL_Socket = "/export/data/mysql/tmp/mysql.sock"

# Common config:
DATETIME_FORMAT = '%Y-%m-%d %X'
EXEC_DETAIL_FILE = 'exec_detail.txt'
EXEC_SCRIPT_FILE = 'delete_scripts.sql'


def highlight(s):
    return "%s[30;2m%s%s[1m" % (chr(27), s, chr(27))


def print_warning_message(message):
    """
    以红色字体显示消息内容
    :param message: 消息内容
    :return: 无返回值
    """
    message = str(message)
    print(highlight('') + "%s[31;1m%s%s[0m" % (chr(27), message, chr(27)))
    global EXEC_DETAIL_FILE
    write_file(EXEC_DETAIL_FILE, message)


def print_info_message(message):
    """
    以绿色字体输出提醒性的消息
    :param message: 消息内容
    :return: 无返回值
    """
    message = str(message)
    print(highlight('') + "%s[32;2m%s%s[0m" % (chr(27), message, chr(27)))
    global EXEC_DETAIL_FILE
    write_file(EXEC_DETAIL_FILE, message)


def write_file(file_path, message):
    """
    将传入的message追加写入到file_path指定的文件中
    请先创建文件所在的目录
    :param file_path: 要写入的文件路径
    :param message: 要写入的信息
    :return:
    """
    file_handle = open(file_path, 'a')
    file_handle.writelines(message)
    # 追加一个换行以方便浏览
    file_handle.writelines(chr(13))
    file_handle.close()


def get_user_choose_option(input_options, input_message):
    while_flag = True
    choose_option = None
    while while_flag:
        print_info_message(input_message)
        str_input = raw_input("")
        for input_option in input_options:
            if str_input.strip() == input_option:
                choose_option = input_option
                while_flag = False
    return choose_option


def get_mysql_connection():
    """
    根据默认配置返回数据库连接
    :return: 数据库连接
    """
    if Default_MySQL_Host.lower() == 'localhost':
        conn = MySQLdb.connect(
                host=Default_MySQL_Host,
                port=Default_MySQL_Port,
                user=Default_MySQL_User,
                passwd=Default_MySQL_Password,
                connect_timeout=Default_MySQL_Connect_TimeOut,
                charset=Default_MySQL_Charset,
                db=DELETE_DATABASE_NAME,
                unix_socket=Default_MySQL_Socket
        )
    else:
        conn = MySQLdb.connect(
                host=Default_MySQL_Host,
                port=Default_MySQL_Port,
                user=Default_MySQL_User,
                passwd=Default_MySQL_Password,
                connect_timeout=Default_MySQL_Connect_TimeOut,
                charset=Default_MySQL_Charset,
                db=DELETE_DATABASE_NAME
        )
    return conn


def mysql_exec(sql_script, sql_param=None):
    """
    执行传入的脚本，返回影响行数
    :param sql_script:
    :param sql_param:
    :return: 脚本最后一条语句执行影响行数
    """
    try:
        conn = get_mysql_connection()
        print_info_message("在服务器{0}上执行脚本:{1}".format(
                conn.get_host_info(), sql_script))
        cursor = conn.cursor()
        if sql_param is not None:
            cursor.execute(sql_script, sql_param)
        else:
            cursor.execute(sql_script)
        affect_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return affect_rows
    except Exception as ex:
        cursor.close()
        conn.rollback()
        raise Exception(str(ex))


def mysql_exec_many(sql_script_list):
    """
    执行传入的脚本，返回影响行数
    :param sql_script_list: 要执行的脚本List,List中每个元素为sql_script, sql_param对
    :return: 返回执行每个脚本影响的行数列表
    """
    try:
        conn = get_mysql_connection()
        exec_result_list = []
        for sql_script, sql_param in sql_script_list:
            print_info_message("在服务器{0}上执行脚本:{1}".format(
                    conn.get_host_info(), sql_script))
            cursor = conn.cursor()
            if sql_param is not None:
                cursor.execute(sql_script, sql_param)
            else:
                cursor.execute(sql_script)
            affect_rows = cursor.rowcount
            exec_result_list.append("影响行数：{0}".format(affect_rows))
        conn.commit()
        cursor.close()
        conn.close()
        return exec_result_list

    except Exception as ex:
        cursor.close()
        conn.rollback()
        raise Exception(str(ex))


def mysql_query(sql_script, sql_param=None):
    """
    执行传入的SQL脚本，并返回查询结果
    :param sql_script:
    :param sql_param:
    :return: 返回SQL查询结果
    """
    try:
        conn = get_mysql_connection()
        print_info_message("在服务器{0}上执行脚本:{1}".format(
                conn.get_host_info(), sql_script))
        cursor = conn.cursor()
        if sql_param is not None:
            cursor.execute(sql_script, sql_param)
        else:
            cursor.execute(sql_script)
        exec_result = cursor.fetchall()
        cursor.close()
        conn.close()
        return exec_result
    except Exception as ex:
        cursor.close()
        conn.close()
        raise Exception(str(ex))


def get_column_info_list(table_name):
    sql_script = """
DESC {0}
""".format(table_name)
    column_info_list = []
    query_result = mysql_query(sql_script=sql_script, sql_param=None)
    for row in query_result:
        column_name = row[0]
        column_type = row[1]
        column_key = row[3]
        column_info = column_name, column_key, column_type
        column_info_list.append(column_info)
    return column_info_list


def get_id_range():
    """
    按照传入的表获取要删除数据最大ID、最小ID、删除总行数
    :return: 返回要删除数据最大ID、最小ID、删除总行数
    """
    global DELETE_TABLE_NAME
    global DELETE_CONDITION
    sql_script = """
SELECT
MAX({2}) AS MAX_ID,
MIN({2}) AS MIN_ID,
COUNT(1) AS Total_Count
FROM {0}
WHERE {1};
""".format(DELETE_TABLE_NAME, DELETE_CONDITION, DELETE_TABLE_KEY)

    query_result = mysql_query(sql_script=sql_script, sql_param=None)
    max_id, min_id, total_count = query_result[0]
    # 此处有一坑，可能出现total_count不为0 但是max_id 和min_id 为None的情况
    # 因此判断max_id和min_id 是否为NULL
    if (max_id is None) or (min_id is None):
        max_id, min_id, total_count = 0, 0, 0
    return max_id, min_id, total_count


def delete_data(current_min_id, current_max_id):
    sql_script = """
DELETE FROM {0}
WHERE {4} <= {1}
and {4} >= {2}
AND {3};
""".format(DELETE_TABLE_NAME,
           current_max_id,
           current_min_id,
           DELETE_CONDITION,
           DELETE_TABLE_KEY
           )
    global EXEC_SCRIPT_FILE
    global VIEW_OR_RUN
    if VIEW_OR_RUN == 'RUN':
        row_count = mysql_exec(sql_script)
        print_info_message("影响行数：{0}".format(row_count))
        time.sleep(SLEEP_SECOND_PER_BATCH)
    else:
        print_info_message("生成删除脚本(未执行)")
        print_info_message(sql_script)
    tmp_script = """
USE {0};
""".format(DELETE_DATABASE_NAME) + sql_script + """
COMMIT;
SELECT SLEEP('{0}');
##=====================================================##
""".format(SLEEP_SECOND_PER_BATCH)
    write_file(file_path=EXEC_SCRIPT_FILE, message=tmp_script)


def loop_delete_data():
    max_id, min_id, total_count = get_id_range()
    if min_id == max_id:
        print_info_message("无数据需要结转")
        return
    current_min_id = min_id
    global DELETE_ROWS_PER_BATCH
    while current_min_id <= max_id:
        print_info_message("*" * 70)
        current_max_id = current_min_id + DELETE_ROWS_PER_BATCH
        delete_data(current_min_id, current_max_id)
        current_percent = (current_max_id - min_id) * 100.0 / (max_id - min_id)
        left_rows = max_id - current_max_id
        if left_rows < 0:
            left_rows = 0
        current_percent_str = "%.2f" % current_percent
        info = "当前进度{0}/{1},剩余{2},进度为{3}%"
        info = info.format(current_max_id,
                           max_id,
                           left_rows,
                           current_percent_str)
        print_info_message(info)
        current_min_id = current_max_id
    print_info_message("*" * 70)
    print_info_message("执行完成")


def check_config():
    try:
        global DELETE_DATABASE_NAME
        global DELETE_TABLE_NAME
        global DELETE_TABLE_KEY
        global DELETE_CONDITION
        global VIEW_OR_RUN

        if str(DELETE_DATABASE_NAME).strip() == "":
            print_warning_message("数据库名不能为空")
            return False
        if str(DELETE_TABLE_NAME).strip() == "":
            print_warning_message("表名不能为空")
            return False
        if str(DELETE_CONDITION).strip() == "":
            print_warning_message("删除条件不能为空")
            return False
        source_columns_info_list = get_column_info_list(DELETE_TABLE_NAME)
        column_count = len(source_columns_info_list)
        primary_key_count = 0
        for column_id in range(column_count):
            source_column_name, source_column_key, source_column_type = source_columns_info_list[column_id]
            if source_column_key.lower() == 'pri':
                primary_key_count += 1
                if not ('int' in str(source_column_type).lower()):
                    print_warning_message("主键不为int或bigint")
                    return False
                else:
                    global DELETE_TABLE_KEY
                    DELETE_TABLE_KEY = source_column_name

        if primary_key_count == 0:
            print_warning_message("未找到主键，不瞒足迁移条件")
            return False

        if primary_key_count > 1:
            print_warning_message("要删除的表使用复合主键，不满足迁移条件")
            return False

        return True
    except Exception as ex:
#        print_warning_message("执行出现异常，异常为{0}".format(ex.message))
        return False


def clean_env():
    global DELETE_DATABASE_NAME
    global DELETE_TABLE_NAME
    global DELETE_TABLE_KEY
    global DELETE_CONDITION
    global VIEW_OR_RUN
    DELETE_DATABASE_NAME = ""
    DELETE_TABLE_NAME = ""
    DELETE_TABLE_KEY = ""
    DELETE_CONDITION = ""
    VIEW_OR_RUN = "VIEW"

    if os.path.exists(EXEC_SCRIPT_FILE):
        os.remove(EXEC_SCRIPT_FILE)
    if os.path.exists(EXEC_DETAIL_FILE):
        os.remove(EXEC_DETAIL_FILE)


def user_confirm():
    if VIEW_OR_RUN == 'RUN':
        info = """
您将在服务器{0}上{1}中删除表{2}中数据

删除数据条件为:
DELETE FROM {3}
WHERE {4}
"""
    else:
        info = """
将生成在服务器{0}上{1}中删除表{2}中数据的脚本

删除数据条件为:
DELETE FROM {3}
WHERE {4}
"""
    info = info.format(Default_MySQL_Host,
                       DELETE_DATABASE_NAME,
                       DELETE_TABLE_NAME,
                       DELETE_TABLE_NAME,
                       DELETE_CONDITION)

    if VIEW_OR_RUN == "RUN":
        print_warning_message(info)
    else:
        print_info_message(info)
    input_options = ['yes', 'no']
    input_message = """
请输入yes继续或输入no退出，yes/no?
"""
    user_option = get_user_choose_option(input_options=input_options,
                                         input_message=input_message)
    if user_option == "no":
        return False
    else:
        return True


def delete_table_data(database_name, table_name, delete_condition, is_run, is_need_confirm):
    global DELETE_DATABASE_NAME
    global DELETE_TABLE_NAME
    global DELETE_TABLE_KEY
    global DELETE_CONDITION
    global VIEW_OR_RUN
    DELETE_DATABASE_NAME = database_name
    DELETE_TABLE_NAME = table_name
    DELETE_CONDITION = delete_condition
    DELETE_TABLE_KEY = ''
    if is_run:
        VIEW_OR_RUN = "RUN"
    else:
        VIEW_OR_RUN = "VIEW"
    check_result = check_config()
    if not check_result:
        return
    if is_need_confirm:
        confirm_result = user_confirm()
    else:
        confirm_result = True
    if confirm_result:
        loop_delete_data()


def main():
    clean_env()
    delete_table_data(database_name="wxlun",
                      table_name="wxlun_stock02",
                      delete_condition="sid<29997519",
                      is_run=True,
                      is_need_confirm=True)


if __name__ == '__main__':
    main()
