# 续传超时修复与 Render 部署

## 2026-10-26 上游适配

已合并上游 `main` 的 `b3ca74a`（2026-10-23），接入多 Express Key 轮换及独立项目绑定、凭证隔离、可选跨通道兜底、长思考请求超时、预填充去重、工具参数分片与控制台更新。

- 保留本 Fork 的 900 秒整轮时限、原子替换缓冲、旧任务身份校验和不输出正文的生命周期日志。SDK 的 600 秒单次调用超时与整轮限制同时生效。
- 跨通道兜底默认关闭，已有持久设置中的显式选择仍优先。需要使用时在控制台开启并配置备用凭证。
- 首包前最后一个读者断开后保留 1.5 秒重连窗口；重连复用原任务及原截止时间。宽限期过后取消上游并结束错误缓冲；已有有效产出后仍继续接收，直到完成或整轮超时。
- 正文、思考、工具调用才算有效产出；角色空帧和保活不算。注释与假流式保活都不占续传序号。`Last-Event-ID: k` 从 k+1 开始，`resume_from=k` 包含 k。
- 错误流不标记成功交付，失败或无有效输出的任务可以重新生成。部分产出后上游失败会发送错误帧及终止帧，不自动重发已输出的内容。
- 适配只更新 Fork 源码；VPS 需按运维说明另行构建部署，现有持久设置和模型专属配置继续沿用。

## 修复内容

原实现中，上游读流长时间不结束时，后台缓冲会持续为 `running`。相同请求重发继续命中这份缓冲，有读者时清理器还会跳过它，表现为酒馆一直等待回复。

本次修复包括：

- 每轮生成使用同一个总截止时间，覆盖上游建流、读取及非流式生成。
- 超时结束缓冲，向正在读取的客户端发送 OpenAI 格式错误和 `[DONE]`；尚未建立 SSE 或非流式调用返回 HTTP 504。
- 后续相同请求可以新建任务；正常在途请求仍合并，已完成但未交付的正常回复仍可续传。
- 清理器不再因为有读者而跳过生成截止时间。
- 旧任务退出、旧占位撤销不会删除同指纹的新任务。
- SSE 连接立即发送注释保活，之后每 15 秒保活，不占用续传事件序号，也不延长生成时限。

仅修改本地源码，不会自动更新当前 Render 服务。本修复约束本代理的等待与任务生命周期，不保证 Google 在收到请求后立即停止远端计算，也没有修改预设或工具参数的显示方式。

## 超时配置

默认每轮总时限为 **900 秒（15 分钟）**。Render 的 Environment 中可以设置：

```text
RESUMABLE_UPSTREAM_TIMEOUT_SECONDS=900
```

如果正常的长回复确实需要更久，可以改为 `1800`。环境变量须填写数字；运行态设置中的无效值，以及非正数或非有限数值，会回退到 900 秒。开始一轮生成时读取配置，重连不会重新计时。

该配置作用于启用了续传缓冲的请求，即默认 `resumable_enabled: true`；关闭续传的原有直通路径不在此次修改范围内。

## 如何看请求日志

生命周期日志带有请求指纹 ID 和短轮次号。同一轮续传复用这两个标识；超时后重新生成时，即使指纹相同，轮次号也会变化。每行提供流式或非流式模式、当前阶段、状态、已用时间、剩余时限、缓冲事件数及 SSE 缓冲读者数。流式和非流式等待均计入当前读者数；状态结束后已用时间固定为本轮生成耗时。

流式请求通常依次出现：

```text
[请求开始] req-… | 轮次=… | 阶段=等待上游响应 | … | 新建上游调用
[上游建流] req-… | 轮次=… | 阶段=等待首个上游事件 | …
[首个上游事件] req-… | 轮次=… | 阶段=读取上游事件 | …
[响应落缓冲] req-… | 轮次=… | 状态=已完成 | …
[下游发送结束] req-… | 轮次=… | … | 终止帧=已输出
```

- 只有“上游建流”，随后反复“上游等待”：上游生成器已建立，但尚未收到数据事件。
- “首个上游事件”之后等待：观察事件数及“无事件”秒数，判断是否长时间没有后续数据。清理器默认每分钟检查，连续 60 秒无事件后至多每分钟提示一次；保活不计为事件，也不会逐条打印。
- “续传命中”或“重放命中”：跟读、等待或重放原轮次，不会新建上游调用。非流式等待中的命中会明确写“等待同一轮后台任务”。
- “上游超时”：记录超时所在阶段、耗时、事件数及 HTTP 504，缓冲已结束；底层 SDK 的超时也可能早于整轮总时限。
- “响应落缓冲”但没有“下游发送结束”：代理已接收结束，应继续排查下游连接。“下游读取中断”表示当前 SSE 消费被关闭或取消，仍在运行的后台任务保留至截止时间。

“响应落缓冲”在后台接收完成时记录，即使前端已断开也会输出；非流式回复交给 ASGI 时另记“下游响应返回”。这些日志都不能证明浏览器已收到或酒馆已显示。首个数据帧可能是思考、工具参数或错误；流传输正常结束也不等于模型生成成功。

本 Fork 的生命周期日志只记录元数据，不记录聊天正文、请求体、凭据或原始异常正文。既有上游 SDK、密钥选择等日志仍沿用原有输出；本次未全面改造日志系统。

## 将修改版部署到 Render

当前本地仓库的 `origin` 指向 `bad-woman/vertex2openai`。要部署自己的改动，应先放入自己可写的 GitHub 仓库，再让 Render 从自己的仓库构建。

本次使用的 Fork 是 [ptJax/vertex2openai-fix-](https://github.com/ptJax/vertex2openai-fix-)，部署分支为 `main`。

### 1. 建立自己的 Fork 并放入修复

在原 GitHub 仓库页面点击 **Fork**，创建到自己的账户。随后在自己的 Fork 中上传或通过 Git 提交这些运行时文件，保持原目录：

```text
app/config.py
app/resumable.py
app/routes/chat_api.py
```

也建议同步提交本次新增的 `tests/`、本说明以及 README 修改，便于以后维护。不要只上传一个 ZIP 文件：GitHub 不会自动把 ZIP 解压成仓库代码。

若使用本机 Git，可在本仓库目录运行以下 PowerShell 命令。以下地址对应本次 Fork；其他用户应替换为自己的仓库地址：

```powershell
git remote add mine https://github.com/ptJax/vertex2openai-fix-.git
git add app/config.py app/resumable.py app/routes/chat_api.py README.md docs/resumable-timeout-render.md tests/test_resumable.py tests/test_chat_api_resumable.py
git commit -m "修复续传任务超时及悬挂缓冲恢复"
git push mine HEAD:main
```

如果名为 `mine` 的 remote 已存在，请先检查它的地址。若推送因远端分支已前进而被拒绝，应先协调合并，不能使用强制推送覆盖远端修改。若修复已出现在 Fork 的 `main` 分支，无需重复执行提交和推送。

### 2. 从自己的 Fork 创建 Render 服务

推荐保留旧服务，先验证新服务：

1. Render 控制台选择 **New → Web Service**。
2. 通过 **Git Provider** 连接自己的 GitHub 账户，选择 `ptJax/vertex2openai-fix-` 和 `main` 分支。
3. 选择仓库现有的 **Docker** 运行方式；Dockerfile 路径为仓库根目录的 `Dockerfile`。仓库现有 Dockerfile 已包含构建和启动命令。
4. 在 **Environment** 中设置旧服务所需的环境变量，尤其是 `API_KEY`、`VERTEX_EXPRESS_API_KEY`，并按需设置新的超时变量。
5. 点击 **Create Web Service**，等待构建和部署完成。

不要把 Render 配置成直接运行原作者的预构建镜像，例如 `ghcr.io/bad-woman/vertex2openai:latest`；那样使用的仍是原镜像，不会包含 Fork 的源码修改。本仓库的 docker-compose.yml 默认也是原作者镜像，不用于上述 Render 源码构建步骤。

Render 默认文件系统是临时的。控制台保存的 Key 列表、模型设置等位于 `web_state.json`，不能假定新服务会自动继承。如已使用持久磁盘和 `STATE_DIR`，应保留对应的持久化安排；否则在新服务控制台重新配置必要信息。凭据只放在 Render 环境变量或其持久状态中，不上传到 GitHub。

### 3. 切换酒馆连接

新服务可用后，将酒馆的自定义 API 地址改为新服务地址加 `/v1`，例如：

```text
https://你的新服务.onrender.com/v1
```

酒馆填写的 API Key 必须与新服务的 `API_KEY` 一致。先在下一次正常使用时确认成功回复，再处理旧服务，不必提前删除它。

以后更新自己的 Fork 后，可等待自动部署，或在 Render 的 **Manual Deploy → Deploy latest commit** 手动部署。仅重启旧版本服务不会把本地修改带上去。

## 本地验证

测试只使用假上游，不读取部署密钥或调用 Google。续传核心测试只需要 Python 标准库；运行全部测试须先在虚拟环境安装 `app/requirements.txt` 中的现有依赖，路由和调度测试分别使用 FastAPI、Pydantic 及 Google SDK 的本地类型。

```powershell
$env:PYTHONUTF8 = "1"
python -m unittest discover -s tests -p "test_*.py" -v
```

覆盖正常并发去重、断线续传、超时错误结束、活动读者唤醒、重试原子替换、旧任务回调隔离、非流式调用及实际路由行为，并检查日志阶段、等待限频、断线后后台落缓冲记录及敏感正文不输出。

## 官方操作参考

- [Render：创建 Web Service](https://render.com/docs/web-services)
- [Render：连接 Git Provider](https://render.com/docs/git-provider)
- [Render：配置环境变量](https://render.com/docs/configure-environment-variables)
- [Render：部署与临时文件系统](https://render.com/docs/deploys)
