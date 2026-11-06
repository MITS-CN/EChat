# Echat

Echat 是一个为 Minecraft 打造的**跨服即时通讯系统**，由一个自研模组客户端和一个独立聊天服务端组成。
它把“聊天”从 Minecraft 游戏逻辑里剥离出来，做成可独立部署的服务，让不同服务器、不同世界的玩家能够在同一个频道里交流，而不必依赖代理层、Bukkit 插件或原版服务器的内置聊天。

## 🏗️ 组成

| 组件 | 语言 / 技术 | 说明 |
| :--- | :--- | :--- |
| **客户端（Echat Mod）** | Java · Minecraft 模组 | 玩家在游戏内通过 `/Echat ...` 命令使用全部功能。支持 Fabric / Forge / NeoForge 三端。客户端只负责命令交互与界面展示，不承载业务状态。 |
| **服务端（Echat Server）** | Rust · Tokio · Axum(WebSocket) · SQLite | 独立部署的聊天服务。管理用户注册、会话状态、好友群聊、跨服消息转发与封禁名单。默认监听 `0.0.0.0:1145`，WebSocket 端点 `/echat`。 |

两者之间用 **WebSocket + JSON** 通信：客户端发送纯文本命令（如 `/Echat server list`），服务端以结构化 JSON 响应与事件推送，协议简单、可读、易调试。

## ✨ 核心能力

- **跨服互联**：客户端可添加多个 Echat 服务器地址，一键连接在线服务器，消息自动汇聚到同一频道。
- **现代账户体系**：注册/登录/登出，Argon2 密码哈希，支持接入密码（Auth 帧）与大小写不敏感。
- **社交通讯**：完整的双向好友系统、上线提醒、私聊（500条历史），以及千人群聊、四种入群模式（自由/审批/禁止/答题）、群权限管理。
- **会话隔离与保护**：消息分发支持全局、好友、群聊过滤（`/Echat show`）；防刷屏与自动化清理（限制历史条数，每日自动清理 30 天前的旧数据）。
- **权限管理**：客户端隐藏命令 `/Echat OP Login` 三重校验提权；支持 kick / ban / ban-ip / pardon。
- **零运维部署**：服务端单二进制 + SQLite 单文件，无外部依赖，控制台内置全套管理命令。
- **多端支持**：客户端模组计划覆盖三大主流加载器，用 `Java-WebSocket` 即可建立连接，无需处理任何 MC 底层协议细节。

## 🚦 项目状态

- ✅ **服务端一期**：地址簿、用户注册、服务器互联、OP 管理、控制台命令。
- ✅ **服务端二期**：接入密码、WebSocket 认证帧、tell 私聊、test 加密探测、TLS 支持。
- ✅ **服务端三期**：好友系统、上线提醒、消息持久化、群聊系统（建群/权限/审批）、消息显示过滤、后台自动清理。
- 🚧 **客户端模组**：开发中（Fabric / Forge / NeoForge 目录已就绪，正在对接服务端协议）。

## 📁 仓库结构

```text
EChat/
├── .github/workflows/     # GitHub Actions (Rust CI 自动构建)
├── echat-server/          # 服务端代码 (Rust)
│   ├── src/
│   │   ├── commands/      # 指令路由：user, friend, group, op, show, server 等
│   │   ├── db.rs          # SQLite 库表结构、连接池与自动建表
│   │   ├── main.rs        # 入口、全局状态、定时清理任务
│   │   ├── protocol.rs    # 服务端与客户端通信 JSON 结构
│   │   └── session.rs     # WebSocket 会话与认证状态管理
│   └── Cargo.toml
├── echat-client/          # 客户端模组 (Java)
│   ├── Fabric/            # Fabric 适配层
│   ├── Forge/             # Forge 适配层
│   └── NeoForge/          # NeoForge 适配层
├── LICENSE
└── README.md
```

## 💡 为什么不用现成方案

Minecraft 的跨服聊天通常依赖 BungeeCord/Velocity 代理或各服自建插件，部署复杂且绑定服务端版本。
Echat 走另一条路：**聊天与游戏解耦**，服务端不感知方块、实体、世界，只处理消息与会话；客户端只作为命令入口。这让整个系统可以独立演进，不受 Minecraft 版本升级影响。

---

## 🛠️ 服务端运行指南

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ECHAT_BIND` | `0.0.0.0:1145` | 监听地址 |
| `ECHAT_SERVER_NAME` | `Echat` | 服务器名（注册时使用） |
| `ECHAT_OP_PASSWORD` | `admin123` | 默认 OP 密码（仅首次启动有效，之后请用控制台修改） |
| `ECHAT_DB_PATH` | `echat.db` | SQLite 数据库路径 |
| `ECHAT_TLS_CERT` | 空 | TLS 证书路径（配置后开启 wss） |
| `ECHAT_TLS_KEY` | 空 | TLS 私钥路径 |

### 编译与运行
```bash
cd echat-server
cargo run --release
```

---

## 📖 附录：指令大全（格式为 `/Echat ...`）

### 1. 用户与登录
- `/Echat user reg name [名称] Echat`：注册第一步
- `/Echat user reg password [密码] [重复密码] Echat`：注册第二步（自动登录）
- `/Echat user login [名称] [密码]` / `/Echat user logout`

### 2. 好友与提醒
- `/Echat online friends list` / `search [名称]` / `[名称] add|del|Y|N`
- `/Echat online take on|off`：是否允许别人收到你的上线提醒
- `/Echat online friends-go-online-Reminder on|off [名称]`

### 3. 群聊系统
- `/Echat online groups new [群名]`：建群
- `/Echat online groups search name [群名]` / `join [群标识]`
- `/Echat gsay [群标识] [内容]`：群聊发言
- `/Echat online groups setting [群标识] ...`：群管理设置（admin, everyone, rename 等）
- `/Echat online groups in agree [名称] [群标识]`：管理员审批加群

### 4. 聊天与显示
- `/Echat say [内容]`：全局广播
- `/Echat tell [名称] [内容]`：私聊
- `/Echat show ALL | LAN | friends [名称] | groups [群标识]`

### 5. 隐藏管理命令 (OP)
- `/Echat op login [名称] [密码] [OP密码]`
- `/Echat op ban / ban-ip / pardon / kick / list / banlist`
- `/Echat op friends del / get` / `/Echat op grups del / get`

### 6. 服务端控制台命令（无需前缀）
直接在服务器终端输入：`help`, `status`, `list`, `kick`, `ban`, `pardon`, `op`, `friends`, `grups`, `say`, `password`, `password-op`, `log`, `exit`。

---

## 🔌 通信协议简述
客户端连接 `ws://host:1145/echat` 后：
1. 若服务端设置了接入密码，客户端需先发送：
   ```json
   {"type":"auth","password":"接入密码"}
   ```
2. 认证成功后，发送普通文本 `/Echat ...` 即可。
3. 服务端返回的数据均为标准 JSON (`resp` 结果、`event` 事件广播、`data` 数据块)。
```