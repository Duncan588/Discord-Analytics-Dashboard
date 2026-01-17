#此脚本用于合并下载好的论坛json文件
import os
import json
import glob
import re
import csv
import time

# ================= 配置区域 =================
# 1. 你的 CSV 文件路径
CSV_FILE_PATH = "all_threads.csv"

# 2. 你的 JSON 备份文件夹路径 (支持递归)
JSON_FILES_PATTERN = "\backup\**\*.json"

# 3. 输出文件名
OUTPUT_FILENAME = "merged_final_2025.json"

# 4. 你的目标分类 ID (用于构建假的频道信息)
TARGET_CATEGORY_ID = "1019924310665728022"


# ===========================================

def get_id_from_filename(filename):
    """从文件名中提取 ID，例如 '...[123456].json' -> '123456'"""
    # 匹配文件名末尾的 [数字].json
    match = re.search(r'\[(\d+)\]\.json$', filename)
    if match:
        return match.group(1)
    return None


def merge_with_csv_logic():
    print(f"🚀 启动！正在读取 CSV 索引: {CSV_FILE_PATH} ...")

    # --- 第一步：读取 CSV 构建白名单 ---
    valid_ids = set()
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('id'):
                    valid_ids.add(row['id'])
    except Exception as e:
        print(f"❌ 读取 CSV 失败: {e}")
        return

    print(f"📋 CSV 读取完毕，共包含 {len(valid_ids)} 个有效帖子 ID。")
    print("-" * 30)

    # --- 第二步：扫描文件并利用文件名快速过滤 ---
    print(f"🔍 正在扫描文件: {JSON_FILES_PATTERN} ...")
    all_files = glob.glob(JSON_FILES_PATTERN, recursive=True)

    print(f"📊 硬盘上共找到 {len(all_files)} 个 JSON 文件。")
    print("⚡ 开始文件名匹配与消息加载...")

    all_messages = []
    header_info = None  # 用于存储头部信息

    processed_count = 0
    skipped_by_csv = 0
    start_time = time.time()

    for index, filepath in enumerate(all_files):
        # 排除输出文件自己
        if filepath.endswith(OUTPUT_FILENAME): continue

        # 1. 从文件名提取 ID
        filename = os.path.basename(filepath)
        file_id = get_id_from_filename(filename)

        # 2. CSV 比对 (核心优化：文件名ID不在CSV里，直接跳过，不读文件内容)
        if file_id and file_id not in valid_ids:
            skipped_by_csv += 1
            # print(f"跳过: {filename} (不在CSV中)") # 调试时可打开
            continue

        # 3. 读取内容 (只有匹配成功的才读，节省时间)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

                # 提取头部信息(只做一次)
                if header_info is None and 'guild' in data:
                    merged_channel = data.get('channel', {})
                    merged_channel['name'] = f"2025合并存档-{merged_channel.get('category', 'All')}"
                    merged_channel['id'] = TARGET_CATEGORY_ID

                    header_info = {
                        "guild": data.get('guild', {}),
                        "channel": merged_channel,
                        "dateRange": {"after": None, "before": None},
                        "exportedAt": "2026-01-16T00:00:00.0000000+00:00"
                    }

                # 提取消息
                msgs = data.get('messages', [])
                if msgs:
                    all_messages.extend(msgs)

                processed_count += 1

        except Exception as e:
            # 遇到坏文件不报错，直接跳过
            pass

        if (index + 1) % 5000 == 0:
            print(f"⏳ 进度: 扫描 {index + 1} | 命中CSV并读取: {processed_count} | CSV跳过: {skipped_by_csv}")

    print(f"\n✅ 文件读取完成！")
    print(f"   - CSV 命中有效文件: {processed_count}")
    print(f"   - CSV 过滤无关文件: {skipped_by_csv}")
    print(f"   - 待排序消息总数: {len(all_messages)}")

    if not header_info:
        print("❌ 错误：没有读取到任何有效的 JSON 数据，请检查路径或 CSV ID 是否匹配。")
        return

    # --- 第三步：全量消息排序 (解决报错的核心) ---
    print(f"\n🔄 正在对 {len(all_messages)} 条消息按时间排序 (CPU 全力工作中)...")
    sort_start = time.time()

    # 按 timestamp 字符串排序
    all_messages.sort(key=lambda x: x.get('timestamp', ''))

    print(f"⚡ 排序耗时: {time.time() - sort_start:.2f} 秒")

    # --- 第四步：写入最终文件 ---
    print(f"💾 正在写入最终文件: {OUTPUT_FILENAME} ...")

    header_info['messages'] = all_messages
    header_info['messageCount'] = len(all_messages)

    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(header_info, f, ensure_ascii=False, indent=2)

    total_time = time.time() - start_time
    print("=" * 40)
    print(f"🎉 完美合并完成！")
    print(f"📂 输出文件: {os.path.abspath(OUTPUT_FILENAME)}")
    print(f"⏱️ 总耗时: {total_time:.2f} 秒")
    print("=" * 40)


if __name__ == "__main__":
    merge_with_csv_logic()