# 飞书审批与对话

## 1. 创建应用

1. 打开 [飞书开放平台](https://open.feishu.cn/app) → **创建企业自建应用**
2. 启用 **机器人** 能力
3. 权限：`im:message`、`im:message:send_as_bot`
4. 记录 **App ID**、**App Secret**

## 2. 事件订阅

**请求地址**（需公网可达，或内网穿透）：

```text
https://YOUR_HOST/api/feishu/webhook
```

订阅事件：

- `card.action.trigger` — 卡片按钮审批
- `im.message.receive_v1` — 飞书发消息 → Cursor Agent 回复（可选）

填写 **Verification Token** 到 `.env` 的 `FEISHU_VERIFICATION_TOKEN`。

**IM 权限（对话可选）**：

- `im:message`
- `im:message:send_as_bot`
- `im:message.group_at_msg:readonly`（群内须 @机器人）
- `im:message.p2p_msg:readonly`（私聊机器人）

## 3. Cursor Agent 登录（可选）

飞书对话走本机 `cursor-agent` CLI：

```bash
curl https://cursor.com/install -fsS | bash
export PATH="$HOME/.local/bin:$PATH"
cursor-agent login
```

`.env` 关键项见 `env.example` 中 `FEISHU_AGENT_*`、`CURSOR_AGENT_*`。

## 4. 接收群 / 用户

将机器人拉入审批群，获取 `chat_id`，写入：

```bash
FEISHU_RECEIVE_ID=oc_xxxx
FEISHU_RECEIVE_ID_TYPE=chat_id
```

列出可见群：

```bash
python scripts/list_feishu_chats.py
```

## 5. 启动

```bash
node frontend/server.js          # 含 /api/feishu/webhook
python feishu/notifier.py        # 新 pending → 推卡片
```

手动推送单条：

```bash
python scripts/push_proposal_feishu.py --id PROPOSAL_ID
```

## 6. 卡片按钮

| 按钮 | decision |
|------|----------|
| 批准实盘 | `approved_live` |
| 批准观察 | `approved_observe` |
| 驳回 | `rejected` |

回调由 `frontend/server.js` 写入 Redis，与 Web 审批相同。

## 7. 本地调试

```bash
bash scripts/tunnel-feishu.sh
# 将生成的 https URL + /api/feishu/webhook 填到飞书后台
```
