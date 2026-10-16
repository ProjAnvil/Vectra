# 测试数据集

## email-Eu-core.txt.gz

来源：Stanford SNAP（SNAP: A General Purpose Network Analysis and Graph Mining Library）。

- 下载：<https://snap.stanford.edu/data/email-Eu-core.txt.gz>
- 引用：Jure Leskovec and Andrej Krevl. SNAP Datasets: Stanford Large Network Dataset
  Collection, June 2014. <https://snap.stanford.edu/data>
- 原始论文：Hao Yin, Austin R. Benson, Jure Leskovec, and David F. Gleich.
  "Local Higher-order Graph Clustering." In Proceedings of the 23rd ACM SIGKDD
  International Conference on Knowledge Discovery and Data Mining. 2017.

有向邮件网络：欧洲某研究机构成员间的邮件往来，u→v 表示 u 给 v 发过至少一封邮件。
格式：每行 `src dst`，节点 ID 0..1004。

**公开统计值**（SNAP 数据页 / SuiteSparse Matrix Collection，用作集成测试的
外部 ground-truth）：

| 指标 | 值 |
|---|---|
| 节点数 | 1005 |
| 边数 | 25571 |
| 强连通分量总数 | 203 |
| 最大 SCC 节点数 | 803 |
| 最大 WCC 节点数 | 986 |

由"1005 节点 / 203 个 SCC / 最大 803"可推出：其余 202 个 SCC 全为单例
（202 个 SCC 至少 202 个节点，恰好只剩 202 个）。数据文件另有 642 个
自环节点（`awk '$1==$2' | sort -u | wc -l` 直接核对），因此集成测试断言
`Cycles()` = 1 个大小为 803 的 SCC 环 + 642 个自环 = 643 个。
