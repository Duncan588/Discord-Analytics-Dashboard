"""旧版 clean_json.py 兼容入口。
现在不再需要先生成 merged_final_clean.json 再导入数据库。
Web 端上传 JSON 后会直接完成识别、清洗和 SQLite 导入。
命令行仍可直接把 JSON 导入指定数据库：
python discordDB.py merged_final_clean.json --db data/servers/SERVER_ID/discord_data.db
"""
from discordDB import main

if __name__ == "__main__":
    main()
