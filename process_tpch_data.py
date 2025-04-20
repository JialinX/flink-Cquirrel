import pandas as pd
import sqlite3
import re
from datetime import datetime

def parse_tpch_line(line):
    """解析 TPC-H 格式的数据行"""
    # 使用正则表达式匹配操作类型和数据
    match = re.match(r'([+-])([A-Z]{2})(.*)', line.strip())
    if not match:
        return None, None, None
    
    operation, table, data = match.groups()
    
    # 解析数据部分
    data_parts = data.strip().split('|')
    
    # 根据表类型返回不同的数据
    if table == 'CU':  # Customer
        return operation, 'customer', {
            'c_custkey': int(data_parts[0]),
            'c_name': data_parts[1],
            'c_address': data_parts[2],
            'c_nationkey': int(data_parts[3]),
            'c_phone': data_parts[4],
            'c_acctbal': float(data_parts[5]),
            'c_mktsegment': data_parts[6],
            'c_comment': data_parts[7]
        }
    elif table == 'OR':  # Orders
        return operation, 'orders', {
            'o_orderkey': int(data_parts[0]),
            'o_custkey': int(data_parts[1]),
            'o_orderstatus': data_parts[2],
            'o_totalprice': float(data_parts[3]),
            'o_orderdate': data_parts[4],
            'o_orderpriority': data_parts[5],
            'o_clerk': data_parts[6],
            'o_shippriority': int(data_parts[7]),
            'o_comment': data_parts[8]
        }
    elif table == 'LI':  # Lineitem
        return operation, 'lineitem', {
            'l_orderkey': int(data_parts[0]),
            'l_partkey': int(data_parts[1]),
            'l_suppkey': int(data_parts[2]),
            'l_linenumber': int(data_parts[3]),
            'l_quantity': float(data_parts[4]),
            'l_extendedprice': float(data_parts[5]),
            'l_discount': float(data_parts[6]),
            'l_tax': float(data_parts[7]),
            'l_returnflag': data_parts[8],
            'l_linestatus': data_parts[9],
            'l_shipdate': data_parts[10],
            'l_commitdate': data_parts[11],
            'l_receiptdate': data_parts[12],
            'l_shipinstruct': data_parts[13],
            'l_shipmode': data_parts[14],
            'l_comment': data_parts[15]
        }
    
    return None, None, None

def process_tpch_data(input_file):
    """处理 TPC-H 格式的数据文件"""
    # 创建内存数据库
    conn = sqlite3.connect(':memory:')
    
    # 创建表
    conn.execute('''
    CREATE TABLE customer (
        c_custkey INTEGER PRIMARY KEY,
        c_name TEXT,
        c_address TEXT,
        c_nationkey INTEGER,
        c_phone TEXT,
        c_acctbal REAL,
        c_mktsegment TEXT,
        c_comment TEXT
    )
    ''')
    
    conn.execute('''
    CREATE TABLE orders (
        o_orderkey INTEGER PRIMARY KEY,
        o_custkey INTEGER,
        o_orderstatus TEXT,
        o_totalprice REAL,
        o_orderdate TEXT,
        o_orderpriority TEXT,
        o_clerk TEXT,
        o_shippriority INTEGER,
        o_comment TEXT,
        FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)
    )
    ''')
    
    conn.execute('''
    CREATE TABLE lineitem (
        l_orderkey INTEGER,
        l_partkey INTEGER,
        l_suppkey INTEGER,
        l_linenumber INTEGER,
        l_quantity REAL,
        l_extendedprice REAL,
        l_discount REAL,
        l_tax REAL,
        l_returnflag TEXT,
        l_linestatus TEXT,
        l_shipdate TEXT,
        l_commitdate TEXT,
        l_receiptdate TEXT,
        l_shipinstruct TEXT,
        l_shipmode TEXT,
        l_comment TEXT,
        PRIMARY KEY (l_orderkey, l_linenumber),
        FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey)
    )
    ''')
    
    # 读取并处理数据
    with open(input_file, 'r') as f:
        for line in f:
            operation, table, data = parse_tpch_line(line)
            if not operation or not table or not data:
                continue
            
            if operation == '+':
                # 插入数据
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['?' for _ in data])
                values = tuple(data.values())
                
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                conn.execute(query, values)
            elif operation == '-':
                # 删除数据
                if table == 'customer':
                    conn.execute("DELETE FROM customer WHERE c_custkey = ?", (data['c_custkey'],))
                elif table == 'orders':
                    conn.execute("DELETE FROM orders WHERE o_orderkey = ?", (data['o_orderkey'],))
                elif table == 'lineitem':
                    conn.execute("DELETE FROM lineitem WHERE l_orderkey = ? AND l_linenumber = ?", 
                                (data['l_orderkey'], data['l_linenumber']))
    
    # 执行查询
    query = """
    SELECT 
        l_shipmode, l_extendedprice, l_discount
    FROM 
        customer,
        orders,
        lineitem
    WHERE 
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND c_mktsegment == 'AUTOMOBILE'
        AND o_orderdate >= '1995-01-01' 
        AND o_orderdate < '1996-01-01'
        AND l_shipmode in ('RAIL', 'AIR', 'TRUCK')
    """
    
    result = pd.read_sql_query(query, conn)
    
    # 关闭连接
    conn.close()
    
    return result

if __name__ == "__main__":
    # 处理数据并执行查询
    result = process_tpch_data('input_data_all.csv')
    
    # 打印结果
    print(f"查询结果共 {len(result)} 条记录:")
    print(result.head(10))
    
    # 保存结果到 CSV 文件
    result.to_csv('query_result.csv', index=False)
    print("结果已保存到 query_result.csv") 