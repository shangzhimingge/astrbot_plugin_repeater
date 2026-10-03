# 更新日志

本文件记录 `astrbot_plugin_repeater` 的所有重要变更。
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [1.1.1] - 2026-10-03

### 修复

- **修复插件数据持久化路径不符合规范的问题**：群状态 `state.json` 改为写入 AstrBot 官方插件数据目录 `data/plugin_data/astrbot_plugin_repeater/`（通过 `StarTools.get_data_dir()` 获取），不再写入 `data/astrbot_plugin_repeater/`，以保证用户数据迁移与审计的一致性。

## [1.1.0] - 2026-10-03

### 修复

- **修复接龙未被打断时反复复读的问题**：旧版在触发复读后会清空当前内容指纹（`current_text = None`），导致同一波**未被其它消息打断**的接龙每累计够阈值就再复读一次。现在一轮接龙（内容保持不变）只复读一次，只有内容被打断（变成别的消息）后才重新计数、可以再次复读。
- **修复复读丢失 QQ 内置小黄脸表情（Face）的问题**：旧版复读内容只取 `event.message_str`（仅含文本，表情不写入其中），表情既不参与判重、复读时也会被丢弃。现在会把表情 id 一并纳入内容指纹，并在复读消息中以 `Face` 组件原样发送；纯表情消息现在也能参与接龙与复读。

### 变更

- 接龙判定由「纯文本」升级为「文本 + QQ 小黄脸表情」，内容指纹与复读消息链统一从原始消息链构造。

### 行为变化（升级须知）

- 一次未被打断的接龙只复读一次：旧版每累计够阈值就复读一次，新版在同一内容未被打断前不再重复复读。

## [1.0.0] - 2026-08-20

- 首个版本：当群内连续 N 个不同的人发送相同消息时，机器人自动跟一条相同消息；支持按群独立开关、自定义触发人数（默认 3）与触发冷却时间；适配 NapCatQQ（OneBot v11 / aiocqhttp）。

[1.1.1]: https://github.com/yoser-s/astrbot_plugin_repeater/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/yoser-s/astrbot_plugin_repeater/compare/v1.0.0...v1.1.0