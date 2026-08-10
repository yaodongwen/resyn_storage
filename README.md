# 目录
```
stock_data_pipeline/


├── crawler/
│
│   └── naver/
│       ├── news_crawler.py
│       └── comment_crawler.py
│


├── outputs/
│
│   ├── 005930_삼성전자/
│   │
│   │   ├── news/
│   │   │    ├── 427059805.json
│   │   │    └── 427059806.json
│   │
│   │   └── comments/
│   │        ├── 888001.json
│   │        └── 888002.json
│
│
├── storage/


│   ├── storage_sync.py       # 主程序
│   │
│   ├── config.yaml           # 配置
│   │
│   ├── scanner.py            # 扫描文件
│   │
│   ├── normalizer.py         # 数据标准化
│   │
│   ├── parquet_writer.py     # parquet生成
│   │
│   ├── postgres.py           # PG操作
│   │
│   ├── uploader.py           # rsync
│   │
│   ├── cleaner.py            # 清理
│   │
│   └── schema.py             # 数据结构
│


├── requirements.txt


└── logs/
```
# 使用方式和介绍
这个项目是为了爬取新闻，评论，研报等结构性json数据，然后将本地零散json小文件打包成parquet文件，然后构建postgresql索引，然后复制到服务器进行持久化存储的。服务器端可以使用另一个项目进行数据的批量快速查询和获取。

# Windows支持
现在可以在Windows上运行。代码中的本地路径已经使用Python的`pathlib`处理，上传模块也兼容了Windows默认可用的OpenSSH `scp`。

需要注意：
- Windows 10/11需要启用或安装OpenSSH客户端，确认PowerShell里可以运行`ssh`和`scp`。
- 如果Windows机器安装了`rsync`，也可以继续使用`rsync`。
- `config.yaml`里的Windows路径建议写成`D:/stock_data_pipeline/outputs`这种正斜杠格式，避免反斜杠转义问题。
- 远端服务器仍然需要能通过SSH访问，并且PostgreSQL连接信息要能从当前机器访问。

# 快速使用
1. 创建并进入Python虚拟环境。
```bash
python -m venv .venv
```

Windows PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:
```bash
source .venv/bin/activate
```

2. 安装依赖。
```bash
pip install -r requirements.txt
```

3. 修改`config.yaml`。
```yaml
local:
  output_dir: "D:/stock_data_pipeline/outputs"
  warehouse_dir: "../warehouse"
  index_cache_dir: "../index_cache"

server:
  host: "192.168.1.33"
  user: "dwyao"
  data_dir: "/mnt/nas-intern/homes/dwyao/Data/stocklake"

sync:
  method: auto
  rsync:
    ssh_port: 22
```

`sync.method`说明：
- `auto`：默认推荐。Windows使用`scp`，macOS/Linux优先使用`rsync`。
- `scp`：强制使用OpenSSH `scp`，适合Windows。
- `rsync`：强制使用`rsync`，适合已经安装rsync的环境。

4. 测试SSH连接。
```bash
ssh -p 22 dwyao@192.168.1.33
```

5. 运行同步。
```bash
python storage_sync.py
```

# 数据流
```
Naver爬虫

       ↓

outputs/

       ↓

storage_sync.py

       ↓


1.读取json

       ↓

2.标准化

       ↓

3.生成parquet


/data/stocklake/

       ↓

4.写PostgreSQL索引


       ↓

5.rsync上传服务器


       ↓

6.删除outputs
```

持续同步模式：
```
python storage_sync.py --watch
```

它会持续扫描 `config.yaml` 里的 `local.output_dir`，只处理最后修改时间超过
`watch.quiet_seconds` 的稳定 JSON；当稳定文件数量达到 `watch.min_files`，
或稳定文件总大小达到 `watch.min_bytes`，就打包、上传并清理这一批。

每批 Parquet 生成后会先写入 `.sync_pending/` 恢复点。如果进程在上传、
写 PostgreSQL 或清理 JSON 期间中断，下次启动会先恢复这些未完成批次。

如果怀疑增量上传有遗漏，可以手动全量扫描上传一次本地 warehouse：
```
python storage_sync.py --full-upload
```


# 服务器启动postgre后操作
使用命令进入postgre
```
~/pgsql/bin/psql -U postgres
~/pgsql/bin/psql -U dwyao -d postgres
```
然后运行
```
CREATE USER stock
WITH PASSWORD 'Stock2026Secure!';

CREATE DATABASE stock_data
OWNER stock;

\c stock_data

GRANT ALL ON SCHEMA public TO stock;

CREATE TABLE stocks
(
    code VARCHAR(20) PRIMARY KEY,
    name TEXT,
    market VARCHAR(20),
    created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE data_files
(
    id BIGSERIAL PRIMARY KEY,

    data_type VARCHAR(50),

    stock_code VARCHAR(20),

    data_date DATE,

    file_path TEXT,

    record_count BIGINT,

    file_size BIGINT,

    created_at TIMESTAMP DEFAULT now()
);


CREATE INDEX idx_data_files_lookup
ON data_files
(
    data_type,
    stock_code,
    data_date
);
CREATE TABLE news_index
(
    id BIGSERIAL PRIMARY KEY,

    news_id VARCHAR(100) UNIQUE,

    stock_code VARCHAR(20),

    publish_time TIMESTAMP,

    title TEXT,

    file_id BIGINT,

    row_number BIGINT
);


CREATE INDEX idx_news_stock_time
ON news_index
(
stock_code,
publish_time
);
CREATE TABLE comments_index
(
    id BIGSERIAL PRIMARY KEY,

    comment_id VARCHAR(100) UNIQUE,

    news_id VARCHAR(100),

    stock_code VARCHAR(20),

    publish_time TIMESTAMP,

    file_id BIGINT,

    row_number BIGINT
);


CREATE INDEX idx_comments_stock_time
ON comments_index
(
stock_code,
publish_time
);
CREATE TABLE sync_log
(
    id BIGSERIAL PRIMARY KEY,

    filename TEXT,

    md5 VARCHAR(64),

    status VARCHAR(20),

    created_at TIMESTAMP DEFAULT now()
);
```

# 格式
## 新闻格式
```
{
    "news_id": "427059805",

    "stock_code": "005930",

    "stock_name": "삼성전자",

    "source": "naver",

    "publish_time": "2026-08-04 10:20:00",

    "title": "新闻标题",

    "content": "正文",

    "url": "",

    "crawl_time": "",

    "hash": ""

}
```

## 评论格式
```
{
    "comment_id":"888001",

    "news_id":"427059805",

    "stock_code":"005930",

    "stock_name":"삼성전자",

    "source":"naver",

    "publish_time":"2026-08-04 10:20:00",

    "user_id":"",

    "content":"评论内容",

    "likes":0,

    "hash":""

}
```
