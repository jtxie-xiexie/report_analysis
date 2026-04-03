# reportAnalysisDemo

Python 版 DolphinDB 研报分析 Agent。

这套实现复用 `demo/agent_client.py` 的 Agent Runtime 客户端，但把业务逻辑独立放在 `reportAnalysisDemo` 下：

- `agent_main.py`: 注册 Agent Runtime agent，发送任务 JSON，驱动多轮工具调用
- `contracts.py`: 任务输入、结果输出、产物命名契约
- `prompt.py`: UTF-8 中文提示词与任务消息构造
- `tools.py`: 本地工具执行层，直接连接 DolphinDB

## 任务输入

```json
{
  "content": "研报全文",
  "factor": "Return_Volatility_Ratio",
  "dataSources": [
    {
      "dbName": "dfs://stockMinKSH",
      "tbName": "stockMinKSH_v2"
    }
  ],
  "forceMR": null
}
```

## 运行方式

先编辑 `reportAnalysisDemo/agent_main.py` 顶部的 `LLM_API_KEY` 常量。

```powershell
conda activate report-dolphindb
cd "d:\Desktop\研报分析\report_analysis"
python reportAnalysisDemo\agent_main.py --task-json reportAnalysisDemo\sample_task.json
```

## 默认连接

- Agent Runtime: `http://192.168.100.208:8985`
- DolphinDB: `192.168.100.43:7301`
- 用户名: `admin`
- 密码: `123456`

可通过环境变量覆盖：

- `REPORT_ANALYSIS_RUNTIME_HTTP_BASE`
- `REPORT_ANALYSIS_RUNTIME_USER`
- `REPORT_ANALYSIS_RUNTIME_PASSWORD`
- `REPORT_ANALYSIS_DDB_HOST`
- `REPORT_ANALYSIS_DDB_PORT`
- `REPORT_ANALYSIS_DDB_USER`
- `REPORT_ANALYSIS_DDB_PASSWORD`
- `REPORT_ANALYSIS_LLM_PROVIDER`
- `REPORT_ANALYSIS_LLM_MODEL`
- `REPORT_ANALYSIS_LLM_ENDPOINT`
- `REPORT_ANALYSIS_LLM_API_KEY`

如果你已经把 key 直接写在 `agent_main.py` 里，就不需要再额外配置环境变量。

## 输出产物

默认输出目录为 `reportAnalysisDemo/outputs/<factor>/`，会生成：

- `<factor>.dos` 或 `<factor>_mr.dos`
- `result.json`
- `evaluation.json`
- `summary.json`
