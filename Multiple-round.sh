#!/bin/bash

# 定义测试参数
TEST_ROUNDS=10              # 运行测试的轮数
START_NODES=5               # 起始节点数量
BYZANTINE=1                 # 拜占庭节点数量
MESSAGE=2                   # 每轮信息数量
BROADCAST_NODE_NUM=1        # 发送节点数量
MSG_PER_NODE=1              # 每轮信息数量
CONNECTIVITY=3              # 网络连接性
OUTPUT_DIR="./mul_outputs"  # 输出文件存储目录

# 创建输出目录（如果不存在）
mkdir -p $OUTPUT_DIR

# 循环运行测试
for ((i=1; i<=TEST_ROUNDS; i++)); do
    CURRENT_NODES=$((START_NODES + i - 1))  # 每轮节点数量增加1
    OUTPUT_FILE="$OUTPUT_DIR/output_round_$i.txt"

    echo "Running test round $i with $CURRENT_NODES nodes..."
    echo "Output will be saved to $OUTPUT_FILE"

    # 生成拓扑文件
    python -m cs4545.system.util compose_dolev $CURRENT_NODES $BROADCAST_NODE_NUM $MSG_PER_NODE $BYZANTINE $CONNECTIVITY
#    python -m cs4545.system.util d2 topologies/dole topologies/dolev.yaml

    # 检查上一个命令是否成功
    if [ $? -ne 0 ]; then
        echo "Topology creation failed in round $i. Exiting..."
        exit 1
    fi

    # 启动 Docker 服务
    docker compose build
    docker compose up -d  # 使用 -d 使服务以守护进程方式运行

    # 等待 Docker 服务稳定
#    sleep $((5 + i))

    # 生成测试报告并保存到文件
#    python -m cs4545.system.util report $CURRENT_NODES $BYZANTINE $OUTPUT_FILE
    python -m cs4545.system.util report $CURRENT_NODES $BYZANTINE > $OUTPUT_FILE 2>&1
    if [ $? -ne 0 ]; then
        echo "Failed to generate report for round $i. Creating an empty output file."
        echo "Error: Report generation failed for round $i" > $OUTPUT_FILE
    fi

    # 打印测试日志
    echo "=== Output for round $i ==="
    cat $OUTPUT_FILE

    # 停止和清理 Docker 容器
    docker compose down
done

echo "All $TEST_ROUNDS rounds completed successfully!"
