#使用DiscordChatExporter工具进行备份论坛帖子
import csv
import subprocess
import os
import sys
import logging
import threading
import queue
import time

# ================= 配置区域 =================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "all_threads.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "backup")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "processed_ids_2025.txt")
LOG_FILE = os.path.join(BASE_DIR, "backup_log.txt")

# 请确认路径是否正确
DCE_PATH = r"E:\DiscordChatExporter.Cli.win-x64\DiscordChatExporter.Cli.exe"

# 带有名称的 Token 配置 MTQ2MTI5请替换成自己的机器人密钥或者用户密钥
TOKENS = [
    {"name": "下载机器人1", "token": "MTQ2MTI5"},
    {"name": "下载机器人2", "token": "MTQ2MTI5"},
    {"name": "下载机器人3", "token": "MTQ2MTI5"},
    {"name": "下载机器人4", "token": "MTQ2MTI5"},
]

file_lock = threading.Lock()


# ================= 日志系统 =================
def setup_logging():
    logger = logging.getLogger("DiscordBackup")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

    # 文件日志：记录所有细节，方便事后查错
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 控制台日志：只显示 INFO 以上
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logging()


# ================= 辅助函数 =================

def sanitize_filename(name: str) -> str:
    """
    清洗文件名：
    1. 移除 Windows 非法字符
    2. 强制截断长度，防止路径过长导致写入失败 (特别是长标题帖子)
    """
    bad_chars = '<>:"/\\|?*\n\r\t'
    clean = "".join("_" if c in bad_chars else c for c in name)
    clean = clean.strip()
    # Windows 路径限制严格，限制标题长度为 60 字符
    if len(clean) > 60:
        clean = clean[:60] + "..."
    return clean if clean else "unknown_thread"


def load_processed_ids():
    ids = set()
    logger.info(f"正在读取断点文件: {CHECKPOINT_FILE}")
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    tid = line.strip()
                    if tid:
                        ids.add(tid)
            logger.info(f"✅ 断点文件读取成功，共发现 {len(ids)} 条已完成记录。")
        except Exception as e:
            logger.error(f"❌ 读取断点文件出错: {e}")
    return ids


def mark_as_processed(thread_id: str):
    with file_lock:
        try:
            with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
                f.write(f"{thread_id}\n")
        except Exception as e:
            logger.error(f"写入断点失败: {e}")


# ================= 工作线程逻辑 =================

def worker_thread(worker_id: int, token_data: dict, job_queue: queue.Queue):
    """消费者线程"""

    # 获取机器人名字和Token
    bot_name = token_data['name']
    token = token_data['token']

    # 设置线程名，方便日志查看
    thread_name = f"{bot_name}"
    threading.current_thread().name = thread_name

    logger.info(f"[{thread_name}] 准备就绪")

    while True:
        try:
            try:
                job = job_queue.get(timeout=2)
            except queue.Empty:
                break

            thread_id = job['id']
            raw_name = job['name']

            # 1. 准备路径
            safe_name = sanitize_filename(raw_name)
            filename = f"{safe_name} [{thread_id}].json"
            full_path = os.path.join(OUTPUT_DIR, filename)

            # 2. 检查是否存在
            if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                mark_as_processed(thread_id)
                job_queue.task_done()
                continue

            # 3. 构建命令
            cmd = [
                DCE_PATH, "export",
                "-t", token,
                "-c", thread_id,
                "-f", "Json",
                "-o", full_path
            ]

            logger.info(f"⬇️ [{bot_name}] 下载: {safe_name}")

            start_time = time.time()

            # 4. 执行 (关键修改: 捕获所有输出)
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',  # 强制 UTF-8 读取
                errors='replace',  # 遇到生僻字不报错，用 ? 代替
                env={**os.environ, "TERM": "dumb"}
            )
            duration = time.time() - start_time

            # 5. 结果判断
            if proc.returncode == 0:
                if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                    file_size_kb = os.path.getsize(full_path) / 1024
                    logger.info(f"✅ [{bot_name}] 成功 ({duration:.1f}s): {safe_name} | {file_size_kb:.1f}KB")
                    mark_as_processed(thread_id)
                else:
                    logger.warning(f"⚠️ [{bot_name}] 虚假成功: 文件未生成 (ID: {thread_id})")
                    # 记录详细日志以供排查
                    logger.debug(f"STDOUT: {proc.stdout}")
                    logger.debug(f"STDERR: {proc.stderr}")
            else:
                # === 错误处理逻辑升级 ===
                logger.error(f"❌ [{bot_name}] 失败 ({duration:.1f}s): {safe_name}")

                # 拼接 stdout 和 stderr，因为 DCE 有时把错误放 stdout
                full_output = (proc.stdout or "") + "\n" + (proc.stderr or "")

                # 尝试提取关键错误行
                error_summary = "未知错误"
                for line in full_output.split('\n'):
                    if "Error" in line or "Exception" in line or "401" in line or "403" in line or "429" in line:
                        error_summary = line.strip()
                        break

                logger.error(f"   原因: {error_summary}")
                # 将完整日志写入文件，不在控制台刷屏
                logger.debug(f"=== 完整错误日志 [{thread_id}] ===\n{full_output}\n===============================")

            job_queue.task_done()

        except Exception as e:
            logger.error(f"[{bot_name}] 线程异常: {e}")
            time.sleep(1)


# ================= 主程序 =================

def main():
    print(f"\n=== Discord 备份助手 (Win/优化版) ===\n")

    if not os.path.exists(DCE_PATH):
        logger.critical(f"找不到程序: {DCE_PATH}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    processed_ids = load_processed_ids()

    jobs = []
    logger.info("正在分析 CSV 任务...")
    try:
        with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('created_at', '').startswith('2025'):
                    tid = row.get('id', '')
                    if tid and tid not in processed_ids:
                        jobs.append({"id": tid, "name": row.get('name', 'Unknown')})
    except Exception as e:
        logger.critical(f"CSV 读取失败: {e}")
        return

    logger.info(f"待处理任务: {len(jobs)}")

    if not jobs:
        return

    q = queue.Queue()
    for job in jobs:
        q.put(job)

    # 启动线程
    threads = []
    # 遍历 TOKENS 列表 (列表里现在是字典了)
    for i, token_data in enumerate(TOKENS):
        t = threading.Thread(target=worker_thread, args=(i, token_data, q))
        t.daemon = True
        t.start()
        threads.append(t)

    try:
        while not q.empty():
            time.sleep(1)
        q.join()
    except KeyboardInterrupt:
        logger.warning("用户停止任务...")

    logger.info("所有任务结束")


if __name__ == "__main__":
    main()