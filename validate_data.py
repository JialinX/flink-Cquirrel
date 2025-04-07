import sqlite3
import pandas as pd
import os

def create_database():
    # 创建SQLite数据库连接
    conn = sqlite3.connect('tpch.db')
    
    # 读取CSV文件
    print("正在读取CSV文件...")
    
    customer_records = []
    order_records = []
    lineitem_records = []
    
    with open('input_data_all.csv', 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            # 解析记录类型和表类型
            operation = line[0]  # '+' 或 '-'
            table_type = line[1:3]  # 'LI', 'OR', 'CU'
            data = line[3:]  # 实际数据
            
            # 只处理插入操作（operation为'+'的记录）
            if operation != '+':
                continue
                
            # 根据表类型分别处理
            if table_type == 'CU':
                customer_records.append(data)
            elif table_type == 'OR':
                order_records.append(data)
            elif table_type == 'LI':
                lineitem_records.append(data)
    
    print(f"读取到 {len(customer_records)} 条客户记录")
    print(f"读取到 {len(order_records)} 条订单记录")
    print(f"读取到 {len(lineitem_records)} 条订单项记录")
    
    # 处理Customer数据
    print("处理客户数据...")
    customer_data = []
    for record in customer_records:
        fields = record.split('|')
        if len(fields) >= 8:
            customer_data.append([
                fields[0],  # c_custkey
                fields[1],  # c_name
                fields[2],  # c_address
                fields[3],  # c_nationkey
                fields[4],  # c_phone
                fields[5],  # c_acctbal
                fields[6],  # c_mktsegment
                fields[7]   # c_comment
            ])
    
    # 处理Order数据
    print("处理订单数据...")
    order_data = []
    for record in order_records:
        fields = record.split('|')
        if len(fields) >= 9:
            order_data.append([
                fields[0],  # o_orderkey
                fields[1],  # o_custkey
                fields[2],  # o_orderstatus
                fields[3],  # o_totalprice
                fields[4],  # o_orderdate
                fields[5],  # o_orderpriority
                fields[6],  # o_clerk
                fields[7],  # o_shippriority
                fields[8]   # o_comment
            ])
    
    # 处理LineItem数据
    print("处理订单项数据...")
    lineitem_data = []
    for record in lineitem_records:
        fields = record.split('|')
        if len(fields) >= 16:
            lineitem_data.append([
                fields[0],  # l_orderkey
                fields[1],  # l_partkey
                fields[2],  # l_suppkey
                fields[3],  # l_linenumber
                fields[4],  # l_quantity
                fields[5],  # l_extendedprice
                fields[6],  # l_discount
                fields[7],  # l_tax
                fields[8],  # l_returnflag
                fields[9],  # l_linestatus
                fields[10], # l_shipdate
                fields[11], # l_commitdate
                fields[12], # l_receiptdate
                fields[13], # l_shipinstruct
                fields[14], # l_shipmode
                fields[15]  # l_comment
            ])
    
    # 创建DataFrame
    customer_df = pd.DataFrame(customer_data, columns=[
        'c_custkey', 'c_name', 'c_address', 'c_nationkey',
        'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'
    ])
    
    order_df = pd.DataFrame(order_data, columns=[
        'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
        'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority',
        'o_comment'
    ])
    
    lineitem_df = pd.DataFrame(lineitem_data, columns=[
        'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
        'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
        'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'
    ])
    
    # 转换数据类型
    print("转换数据类型...")
    # Customer表数据类型转换
    customer_df['c_custkey'] = pd.to_numeric(customer_df['c_custkey'])
    customer_df['c_nationkey'] = pd.to_numeric(customer_df['c_nationkey'])
    customer_df['c_acctbal'] = pd.to_numeric(customer_df['c_acctbal'])
    
    # Order表数据类型转换
    order_df['o_orderkey'] = pd.to_numeric(order_df['o_orderkey'])
    order_df['o_custkey'] = pd.to_numeric(order_df['o_custkey'])
    order_df['o_totalprice'] = pd.to_numeric(order_df['o_totalprice'])
    order_df['o_orderdate'] = pd.to_datetime(order_df['o_orderdate'])
    
    # LineItem表数据类型转换
    lineitem_df['l_orderkey'] = pd.to_numeric(lineitem_df['l_orderkey'])
    lineitem_df['l_partkey'] = pd.to_numeric(lineitem_df['l_partkey'])
    lineitem_df['l_suppkey'] = pd.to_numeric(lineitem_df['l_suppkey'])
    lineitem_df['l_linenumber'] = pd.to_numeric(lineitem_df['l_linenumber'])
    lineitem_df['l_quantity'] = pd.to_numeric(lineitem_df['l_quantity'])
    lineitem_df['l_extendedprice'] = pd.to_numeric(lineitem_df['l_extendedprice'])
    lineitem_df['l_discount'] = pd.to_numeric(lineitem_df['l_discount'])
    lineitem_df['l_tax'] = pd.to_numeric(lineitem_df['l_tax'])
    lineitem_df['l_shipdate'] = pd.to_datetime(lineitem_df['l_shipdate'])
    lineitem_df['l_commitdate'] = pd.to_datetime(lineitem_df['l_commitdate'])
    lineitem_df['l_receiptdate'] = pd.to_datetime(lineitem_df['l_receiptdate'])
    
    # 创建表
    print("创建数据库表...")
    customer_df.to_sql('customer', conn, if_exists='replace', index=False)
    order_df.to_sql('orders', conn, if_exists='replace', index=False)
    lineitem_df.to_sql('lineitem', conn, if_exists='replace', index=False)
    
    return conn

def run_debug_queries(conn):
    print("\n===== 调试查询 =====")
    
    # 1. 检查客户表中的市场细分
    print("\n1. 客户市场细分统计:")
    query1 = """
    SELECT c_mktsegment, COUNT(*) as count 
    FROM customer 
    GROUP BY c_mktsegment
    """
    result1 = pd.read_sql_query(query1, conn)
    print(result1)
    
    # 2. 检查AUTOMOBILE客户的订单
    print("\n2. AUTOMOBILE客户的订单数:")
    query2 = """
    SELECT COUNT(DISTINCT o.o_orderkey) as order_count
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    WHERE c.c_mktsegment = 'AUTOMOBILE'
    """
    result2 = pd.read_sql_query(query2, conn)
    print(result2)
    
    # 3. 检查符合日期条件的AUTOMOBILE客户订单
    print("\n3. 符合日期条件的AUTOMOBILE客户订单数:")
    query3 = """
    SELECT COUNT(DISTINCT o.o_orderkey) as order_count
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    WHERE c.c_mktsegment = 'AUTOMOBILE'
    AND date(o.o_orderdate) >= '1995-01-01'
    AND date(o.o_orderdate) < '1996-01-01'
    """
    result3 = pd.read_sql_query(query3, conn)
    print(result3)
    
    # 4. 检查这些订单的LineItem情况
    print("\n4. 符合条件的订单的LineItem统计:")
    query4 = """
    WITH valid_orders AS (
        SELECT DISTINCT o.o_orderkey
        FROM customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
        WHERE c.c_mktsegment = 'AUTOMOBILE'
        AND date(o.o_orderdate) >= '1995-01-01'
        AND date(o.o_orderdate) < '1996-01-01'
    )
    SELECT 
        l.l_shipmode,
        COUNT(*) as item_count,
        SUM(l_extendedprice * (1 - l_discount)) as revenue
    FROM valid_orders vo
    JOIN lineitem l ON vo.o_orderkey = l.l_orderkey
    WHERE l.l_shipmode IN ('RAIL', 'AIR', 'TRUCK')
    GROUP BY l.l_shipmode
    ORDER BY revenue DESC
    """
    result4 = pd.read_sql_query(query4, conn)
    print(result4)
    
    # 5. 显示一些示例数据
    print("\n5. 示例数据（前5条）:")
    query5 = """
    SELECT 
        c.c_custkey,
        c.c_mktsegment,
        o.o_orderkey,
        date(o.o_orderdate) as o_orderdate,
        l.l_shipmode,
        l.l_extendedprice,
        l.l_discount,
        l.l_extendedprice * (1 - l_discount) as revenue
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE c.c_mktsegment = 'AUTOMOBILE'
    AND date(o.o_orderdate) >= '1995-01-01'
    AND date(o.o_orderdate) < '1996-01-01'
    AND l.l_shipmode IN ('RAIL', 'AIR', 'TRUCK')
    LIMIT 5
    """
    result5 = pd.read_sql_query(query5, conn)
    print(result5)

def run_validation_query(conn):
    query = """
    SELECT 
        l_shipmode,
        SUM(l_extendedprice * (1 - l_discount)) as revenue
    FROM 
        customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE 
        c.c_mktsegment = 'AUTOMOBILE'
        AND date(o.o_orderdate) >= '1995-01-01'
        AND date(o.o_orderdate) < '1996-01-01'
        AND l_shipmode in ('RAIL', 'AIR', 'TRUCK')
    GROUP BY
        l_shipmode
    ORDER BY
        revenue DESC
    """
    
    # 执行查询并打印结果
    result = pd.read_sql_query(query, conn)
    print("\n验证查询结果：")
    print(result)
    
    # 计算总收入
    total_revenue = result['revenue'].sum()
    print(f"\n总收入: {total_revenue:.2f}")

def main():
    print("开始数据验证...")
    
    if not os.path.exists('input_data_all.csv'):
        print("错误：找不到input_data_all.csv文件")
        return
    
    try:
        print("正在创建数据库并导入数据...")
        conn = create_database()
        
        run_debug_queries(conn)
        
        print("\n===== 验证查询 =====")
        run_validation_query(conn)
        
        conn.close()
        print("\n验证完成！")
        
    except Exception as e:
        print(f"发生错误：{str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 