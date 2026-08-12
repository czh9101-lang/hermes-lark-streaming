# 本地定制说明（local-customizations 分支）

基于上游 **v0.12.0** 的本地改动。**升级上游会覆盖，需重应用。**

## 1. 面板折叠（2026-08-12，堡侠确认生效）

💭 思考过程 + 🛠️ 工具步骤默认折叠（点击标题可展开，**不是关闭推理**）。

- `cardkit/builder.py`：
  - `_build_tool_panel` 默认参数 `expanded: True → False`
  - `build_streaming_card_v2` reasoning 面板 `expanded=True → False`
- `streaming/segment_helper.py`：
  - `build_add_segment_action` REASONING 段 `expanded=True → False`

机制：飞书 CardKit `collapsible_panel` 的 `expanded` 只是初始状态，用户点击 header 可展开/收起，内容不丢。

## 2. quota footer（2026-08-12）

footer 显示 tavily/firecrawl 余量（`T 751/1000 · FC 873/1000`）。
数据来自 cron 脚本（fetch_quota.py）写 cache/quota.json，controller._read_quota() 本地读缓存。

## 3. answer-fix（2026-08-04）

`controller.py` `_apply_completion_payload` 强制补全最终回答（修复"卡片已完成但回答没发完"）。

---
文件备份：`.bak-*` 在 hermes-toolbox 仓库（xiaog/lark-customizations/）。
