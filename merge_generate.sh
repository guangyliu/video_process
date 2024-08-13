#!/bin/bash
root_path='/lustre/scratch/shared-folders/world-project/datasets/panda70m_streaming_all_1s_10s_16frame_ofm1.5_cutnum_aes4.5_forcaption_gpt_v5_dynamicfps_part2'
while true; do
    # 使用 grep 检查 squeue 命令的输出
    if ! squeue | grep "guangyi" | grep "panda" &> /dev/null; then
        # 如果 grep 返回为空，则运行 test.sh
        echo "run"
        python merge_index.py --root $root_path
        python generate_index_list.py --root $root_path
        break # 如果你只想运行一次 test.sh 后退出循环，加上这一行
    else
        # 否则，等待一分钟
        echo "wait"
        sleep 60
    fi
done



