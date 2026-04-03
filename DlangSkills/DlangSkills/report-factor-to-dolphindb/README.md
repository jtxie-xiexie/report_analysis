# report-factor-to-dolphindb

最小可运行框架，用来把研报因子任务跑成：

- 一个 `.dos` 因子脚本
- 一个 `result.json` 结论报告
- 一个 `evaluation.json` 单因子评价结果

## 目录说明

- `SKILL.md`: 技能说明和工作流约束
- `agents/openai.yaml`: 技能配置
- `demos/return_volatility20_task.json`: 最小 demo 输入
- `dos/factor_pipeline_core.dos`: DolphinDB 侧主业务流程
- `scripts/run_factor_pipeline.py`: Python 壳层入口
- `scripts/normalize_input.py`: 输入归一化
- `scripts/write_dos.py`: `.dos` 文件落地
- `references/`: 规则和设计说明

## 运行方式

在工作区根目录执行：

```powershell
C:\Users\jtxie\miniconda3\envs\report-dolphindb\python.exe DlangSkills\DlangSkills\report-factor-to-dolphindb\scripts\run_factor_pipeline.py --task-json DlangSkills\DlangSkills\report-factor-to-dolphindb\demos\return_volatility20_task.json --output-dir DlangSkills\DlangSkills\report-factor-to-dolphindb\outputs
```

## 运行逻辑

Python 只负责：

- 读取任务 JSON
- 连接 DolphinDB
- 上传参数
- 加载 `factor_pipeline_core.dos`
- 调用 `runFactorPipeline(...)`
- 把返回结果写成文件

DolphinDB 负责：

- `getAllAvailableData()` 数据源发现
- 字段和样例探查
- `mrEligible` 模式判定
- 因子代码生成
- 因子执行
- 单因子评价
- 结果汇总

## 输出文件

输出目录下会生成：

- `<factor>.dos` 或 `<factor>_mr.dos`
- `result.json`
- `evaluation.json`
- `demo_summary.json`

## 当前状态

- 当前 `.dos` 核心已经可被 Python 壳真实调用
- 当前内置模板以 `volatility` 家族为主
- 高频或更多因子家族可以在 `factor_pipeline_core.dos` 里继续扩展
