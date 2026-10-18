# 同步 master 并升级项目本地 Pi

## Goal
升级项目层级 pi，同时 pull GitHub master 最新代码，保留本地工作，不修改全局安装。

## Findings
- 初始工作区干净，当前分支 chore/upgrade-project-pi-1.0.1。
- HEAD 落后 origin/master 23 个提交，无独有提交；已 fast-forward 到 af4e505。
- npm 当前 pi-coding-agent 最新稳定版 1.3.0，pi-server 1.1.0。

## Requirements
- 同步 origin/master（已完成）。
- 升级项目本地 Pi 到最新稳定版，检查根目录与 .pi/npm 安装配置，保持相关包兼容。
- 不修改全局 Pi、不 push、不覆盖用户更改。
- 执行项目检查与本地版本验证。

## Acceptance Criteria
- [x] origin/master 是 HEAD 的祖先。
- [x] 用户确认升级范围（仅项目安装，不动全局）。
- [x] 项目本地 Pi 升级并验证（npm ci；Pi 1.3.0 / pi-server 1.1.0）。
- [x] 项目检查通过：隔离继承环境与临时目录后 360/360 测试及全部检查通过。详见 verification.md。

## Out of Scope
全局升级、功能开发、远端推送。

## Open Questions
无。用户同意升级所有项目本地 Pi 安装及必要兼容依赖；升级后检查兼容性与优化机会，必要兼容修复纳入任务，非必要优化仅报告建议。
