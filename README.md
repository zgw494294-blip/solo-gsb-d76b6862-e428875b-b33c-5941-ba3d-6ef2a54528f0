# gzip NDJSON 批次接收 API

接收单个 **gzip 流**，解压为 **NDJSON**：每行必须是恰好包含非空字符串
`id`、`message` 两个键的 JSON 对象，批内 `id` 唯一。只有在 gzip 流完整
结束、CRC 校验通过且整批成功落库后，批次才可查询。

此外提供跨所有已提交批次的日志检索接口
（`GET /logs/search`，见下文）：按 `message` 做大小写敏感的字面子串匹配，
快照分页，游标签名持久化、重启后可续用。

- 语言/框架：Python 3.12 + FastAPI + Uvicorn
- 存储：SQLite（WAL 模式），数据位于容器内 `/data`（命名卷持久化）

## 字节限制（均按字节计）

| 限制 | 阈值 | 超限结果 |
|---|---|---|
| gzip 压缩请求体 | 2 MiB | `413`，立即停止读取 |
| 解压后数据总量 | 8 MiB | `413`，立即停止读取 |
| 单行正文（不含换行符） | 64 KiB | `413`，立即停止读取 |

其他失败：gzip 损坏 / 被截断 / CRC 错误 / gzip 后有尾随数据、非法 UTF-8、
空行、非法 JSON、记录缺键/多键/类型错误/空串、批内 id 重复、批次为空等，
一律返回 `422`。

接收逐块进行，内存中任何时刻只保留一个读块、一段 zlib 输出、一条未满行和
一小批（约 256 KiB）待落盘记录，不会缓存完整请求体或完整解压内容。上传
期间记录先写入该批次独占的临时暂存库文件（`/data/staging/`），与主库写锁
完全无关；全部校验通过后用单个短事务 `ATTACH` 导入主库原子提交。接收中断、
校验失败或入库失败都会删除暂存文件，不会留下任何可查询批次；慢上传因此
不会阻塞其他批次提交，也不会阻塞已完成批次的读取。

## 启动

需要 Docker（含 Compose 插件）。单条命令完成依赖安装、数据库初始化并启动：

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `gzip_ndjson_data` 中。
数据库在容器启动时通过 `python -m app.init_db` 初始化（幂等，且会清理
上次崩溃可能残留的暂存文件），随后启动 Uvicorn。

停止：`docker compose down`（数据卷保留；加 `-v` 同时删除数据）。

## 接口

### `POST /batches` — 上传 gzip 批次

请求体就是 gzip 字节本身（不是 multipart 表单）。成功返回 `201`：

```json
{ "batch_id": "9f2c…(32位十六进制)", "line_count": 3 }
```

### `GET /batches?limit=100&offset=0` — 列出已提交批次

返回 `total` 与 `items`，每项含 `batch_id`、`line_count`、`created_at`，
按提交顺序分页。

### `GET /batches/{batch_id}/lines?limit=100&offset=0` — 分页读行

保持上传时的原始顺序，每行含 `seq`（从 1 开始）、`id`、`message`。
未知批次 ID 返回 `404`。

### `GET /logs/search` — 跨批次按 message 字面子串检索

在**所有已提交批次**中检索 `message` 包含查询词的日志行（暂存中的批次
不参与）。匹配规则：

- **大小写敏感的字面子串匹配**：`%`、`_` 等一律是普通字符，不是通配符；
  查询词中的空白（含首尾空格、连续空格）原样保留；长度 **1～200 个字符**
  （空串或超长返回 `422`）。
- 结果按**批次提交顺序**、批内 `seq` 升序排列；同一条 `id` 跨批次出现时
  每行各自保留。
- 每个 item 含 `batch_id`、`seq`、`id`、`message`，外加 `next_cursor`
  （末页为 `null`）。

分页与快照：

- **首请求**带 `q`（查询词）和 `limit`（页大小 1～100）。服务在该时刻
  固定“已提交批次范围”（快照上界），查询词与范围都编入不透明游标。
- **续页**只带 `cursor` 和 `limit`，沿用首请求的查询词与快照范围。
  翻页期间即使上传提交了新批次，也不会混入旧查询；而发起一次**新查询**
  （新的首请求）立即可见这些新批次。
- 游标使用 HMAC 签名，签名密钥保存在数据目录（`/data/search_cursor.key`，
  命名卷持久化），**服务重启后游标仍可续用**；游标损坏、被篡改、字段不合法
  一律 `422`。无任何匹配时返回 `{"items":[],"next_cursor":null}`。
- 检索是短只读语句（autocommit，语句结束即释放），不跨请求持有事务，
  每次只取一页多一行，不会缓存全量命中行，也不会阻塞上传提交。

## curl 示例

### 正常：上传并读回

```bash
# 生成一个合法的 gzip NDJSON（末行可以没有换行符）
printf '%s\n' \
  '{"id":"a","message":"hello"}' \
  '{"id":"b","message":"world"}' \
  '{"id":"c","message":"你好"}' \
  | gzip -n > batch.json.gz

# 上传（-x 让 gzip 原样作为请求体发送）
curl -sS -X POST http://localhost:8000/batches \
  --data-binary @batch.json.gz -H 'Content-Type: application/gzip'
# 201 -> {"batch_id":"…","line_count":3}

# 列出批次
curl -sS http://localhost:8000/batches

# 按批次 ID 读行（把 BID 换成上面返回的 batch_id）
BID=…
curl -sS "http://localhost:8000/batches/$BID/lines?limit=10&offset=0"
```

### 失败示例

```bash
# 413：压缩体 > 2 MiB（或解压后 > 8 MiB、单行 > 64 KiB）
dd if=/dev/urandom bs=1M count=3 2>/dev/null | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：gzip 损坏
printf '\x1f\x8b\x08not-gzip' | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：空行
printf '{"id":"a","message":"m"}\n\n' | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：批内 id 重复
printf '%s\n' '{"id":"a","message":"1"}' '{"id":"a","message":"2"}' | gzip -n | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 422：gzip 后尾随数据
{ printf '%s\n' '{"id":"a","message":"m"}' | gzip -n; printf 'X'; } | \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://localhost:8000/batches --data-binary @-

# 404：读取未知批次
curl -sS -o /dev/null -w '%{http_code}\n' \
  http://localhost:8000/batches/does-not-exist/lines
```

任何失败的上传都不会产生可查询批次：随后 `GET /batches` 中不会出现它。

### 检索：特殊字符、翻页期间上传、重启后续页

先准备两批带特殊字符的数据：

```bash
printf '%s\n' \
  '{"id":"a1","message":"100% done_next a_b 状态"}' \
  '{"id":"a2","message":"hello World"}' | gzip -n | \
  curl -sS -X POST http://localhost:8000/batches --data-binary @- \
    -H 'Content-Type: application/gzip'

printf '%s\n' '{"id":"b1","message":"100% again_next x_b"}' | gzip -n | \
  curl -sS -X POST http://localhost:8000/batches --data-binary @- \
    -H 'Content-Type: application/gzip'
```

**特殊字符检索**：`%`、`_`、空格都是字面字符（用 `--get --data-urlencode`
做 URL 编码，空格会变成 `%20`、`%` 变成 `%25`、`+` 变成 `%2B`）：

```bash
# 字面匹配 “100% done_next a_b”
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode 'q=100% done_next a_b' --data-urlencode 'limit=10'
# -> items 只含 a1（大小写敏感，b1 的 again_next 不匹配；% _ 不是通配符）

# 大小写敏感：小写 world 无结果
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode 'q=world'
# -> {"items":[],"next_cursor":null}
```

**翻页期间上传新批次**：先取首页游标，再提交新批次，续页不会混入它；
重新发起首请求才能看到：

```bash
# 首页（假设命中较多，需要翻页；这里用小页大小演示）
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode 'q=100%' --data-urlencode 'limit=1'
# -> {"items":[...],"next_cursor":"v1.ey...（不透明游标）"}
CUR='把上面返回的 next_cursor 原样粘进来'

# 翻页途中上传一个新批次（也含 100%）
printf '%s\n' '{"id":"c1","message":"100% late"}' | gzip -n | \
  curl -sS -X POST http://localhost:8000/batches --data-binary @- \
    -H 'Content-Type: application/gzip'

# 旧查询续页：快照固定，c1 不会出现，末页 next_cursor 为 null
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR" --data-urlencode 'limit=10'

# 新查询（重新带 q）：c1 立即可见
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode 'q=100% late'
```

**重启后续页**：游标已签名且密钥持久化在命名卷中，`docker compose restart`
（或 `down` 后 `up`，只要不加 `-v` 删卷）后旧游标照常使用：

```bash
docker compose restart api

# 重启前拿到的游标可直接继续翻页
curl -sS --get http://localhost:8000/logs/search \
  --data-urlencode "cursor=$CUR"

# 游标损坏或被篡改 -> 422
curl -sS -o /dev/null -w '%{http_code}\n' --get \
  http://localhost:8000/logs/search --data-urlencode 'cursor=v1.garbage.sig'
```

参数/游标错误示例（均为 `422`）：

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  http://localhost:8000/logs/search                 # 既无 q 也无 cursor
curl -sS -o /dev/null -w '%{http_code}\n' \
  'http://localhost:8000/logs/search?q=x&cursor=v1.a.b'  # q 与 cursor 同时给
curl -sS -o /dev/null -w '%{http_code}\n' \
  'http://localhost:8000/logs/search?q=&limit=5'    # 空查询词
curl -sS -o /dev/null -w '%{http_code}\n' \
  'http://localhost:8000/logs/search?q=x&limit=101' # 页大小超出 1..100
```

## 本地开发（不使用 Docker）

```bash
pip install -r app/requirements.txt
export DB_PATH=./data/batches.db
python -m app.init_db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 项目结构

```
app/
  config.py     # 字节限制等配置
  db.py         # SQLite 主库 + 每批次独立暂存库、原子发布、检索查询
  cursor.py     # 检索翻页游标的 HMAC 签名与编解码（重启可续用）
  ingest.py     # 流式 gzip/NDJSON 解析器（有界内存、立即超限、整批原子）
  main.py       # FastAPI 路由
  init_db.py    # 数据库初始化入口
Dockerfile
docker-compose.yml
tests/check_api.py  # 端到端检查（需先启动服务）
```
