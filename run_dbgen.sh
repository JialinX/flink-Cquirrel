#!/bin/bash

# 检查是否提供了size参数
if [ $# -ne 1 ]; then
    echo "使用方法: $0 <size>"
    echo "例如: $0 1"
    exit 1
fi

# 获取size参数
size=$1


# 进入dbgen文件夹并执行dbgen
cd dbgen

# 删除所有.tbl文件
rm -f *.tbl
./dbgen -s $size


# 修改config_all.ini中的ScaleFactor
# sed -i '' "s/ScaleFactor = .*/ScaleFactor = $size/" config_all.ini

# 运行DataGenerator.py
python DataGenerator.py

# 复制生成的input_data_all.csv到flink目录
cp input_data_all.csv ../