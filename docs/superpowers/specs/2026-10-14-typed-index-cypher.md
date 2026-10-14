# 类型化属性索引 + Cypher 子集查询语言 设计文档

日期：2026-10-14
状态：已定稿（用户授权开始实现；两处关键决策由 AI 按调研报告推荐代选，见「自主决策」）
来源调研：[docs/research/2026-10-14-graph-db-capability-survey.md](../../research/2026-10-14-graph-db-capability-survey.md) Q2/Q3

## 目标

1. **Q2**：属性索引从"仅 string 等值"扩展为类型化索引（string / number / bool），支持数值范围查询。
2. **Q3**：新增 openCypher/GQL 子集查询语言（下称 **Cypher 子集**），编译到现有 CSR 遍历原语，接入 Go API、REST、MCP 三个面。

非目标（明确不做）：
- Q1 边衰减（用户确认不做）。
- 向量能力、聚合函数（count/sum）、WITH / 多段 MATCH / OPTIONAL MATCH、路径枚举语义、图修改语句（CREATE/MERGE/DELETE）。
- 存储格式变更（v3 格式不动：索引声明持久化 key 列表，索引本体在 Load/Seal 时重建）。

## 自主决策（用户未及时作答，按报告推荐代选）

| 决策 | 选择 | 理由 |
|---|---|---|
| D1 变长路径匹配语义 | **节点对/可达性语义**（BFS 风格）：变长段中间路径折叠，行按绑定元组去重（行数 = 组合数，非路径数）；不支持 RETURN/引用变长段上的边变量 | 直接映射现有 BFS 原语（快、可复用）；依赖图场景（corpit/scv）所需即此语义；与现有 `BFS`/`Reachable` 语义一致 |
| D2 交付面 | Go API + REST + MCP（含 TS/Python 客户端镜像）一次交付 | 用户原话"Q3 的接入"；报告建议挂载点即 REST 超集 + MCP 工具 |

## Part 1：类型化属性索引

### 现状与问题

- `indexEntry{val string, id uint32}`（internal/engine/engine.go:39），Seal 时只抽取 `Props[key].(string)`。
- 数值/布尔属性无法检索，无数值范围查询。
- JSON 回读整数变 float64（store.go 已注释的老问题），即使做 int 索引也无法与回读数据对齐。

### 设计

**索引结构**（internal/engine）：

```go
// 每个 property key 一个 propIndex，三个类型桶，各桶内按 (val, id) 排序。
type propIndex struct {
    strs  []strEntry   // {val string; id uint32}
    nums  []numEntry   // {val float64; id uint32}
    bools []boolEntry  // {val bool; id uint32}
}
```

**Seal 抽取规则**：对每个 indexKey 扫描节点 props，按 Go 类型分桶：
- `string` → strs
- 所有数值类型（int/int8…int64/uint…uint64/float32/float64/json.Number）→ **归一化为 float64** → nums
- `bool` → bools
- 其余（nil、slice、map 等）跳过（不索引，不报错）

归一化解决 JSON 回读不一致：写侧 int 42 与读侧 float64 42 落同一桶同值。

**engine.Snapshot 查询 API**：
- `LookupIndex(key, value string)`（保留，string 等值）
- `LookupNumber(key string, value float64)`（数值等值）
- `LookupNumberRange(key string, lo, hi *float64)`（范围，nil = 该侧无界；实现为二分下界/上界）
- `LookupBool(key string, value bool)`

**根包 facade**（返回 `([]Node, bool)`，与 `LookupByProperty` 同约定；ok=false = key 未索引）：
- `LookupByNumber(key string, v float64)`
- `LookupByNumberRange(key string, min, max *float64)`
- `LookupByBool(key string, v bool)`

**MCP `lookup_property`**：`propValue` 从 string 改为 `any`（string/number/bool），按 JSON 类型分派到对应桶。

**持久化**：无格式变更。文档补记：数值按 float64 索引，int64 超过 2^53 精度损失（JSON 数值模型的固有限制）。

## Part 2：Cypher 子集查询语言

### 语法（EBNF 摘要）

```
query     := MATCH pattern (',' pattern)* [WHERE expr]
            RETURN ( '*' | item (',' item)* )
            [ORDER BY item [ASC|DESC] (',' item [ASC|DESC])*] [LIMIT uint]
pattern   := nodePat (edgePat nodePat)*
nodePat   := '(' [var] [':' label] [propMap] ')'
edgePat   := '-[' [var] [':' label ('|' label)*] [hops] ']-' [ '>' | '<' ]   // 方向缺省 = 双向
            | '--' | '<--' | '-->'               // 无细节形式（'--' 双向）
hops      := '*' | '*' uint | '*' uint '..' [uint] | '*' '..' uint   // 下界缺省 1，上界缺省无限
propMap   := '{' prop (',' prop)* '}' ; prop := ident ':' literal
item      := var | var '.' prop
expr      := orExpr ; orExpr := andExpr (OR andExpr)* ; andExpr := notExpr (AND notExpr)*
notExpr   := NOT notExpr | cmpExpr
cmpExpr   := operand (('=', '<>', '<', '<=', '>', '>=') operand
                     | 'STARTS WITH' str | 'ENDS WITH' str | 'CONTAINS' str
                     | IN listLit)?
operand   := literal | var '.' prop | var '.' pseudo | '(' expr ')'
pseudo    := 节点: key | label | id ; 边: label | id
literal   := string | number | true | false | null
```

语义要点：
- 变长段 `[r:CALLS*1..3]` 允许写边变量 `r`，但 **RETURN/WHERE 引用它报编译错**（D1 决策的推论）。
- 定长单跳（无 hops）的边变量可绑定：并行边各产生一行。
- `id` 伪属性 = NodeID/EdgeID 数值；`label` = label 字符串；节点 `key` = 主键字符串。
- 无 hops 且无方向的 `--`：一步双向扩展（forward+reverse 邻接并集）。
- 每个路径模式内 node var 重复出现视为同绑定（环模式）；不同路径以逗号连接时按共享 var join，无共享 var 则笛卡尔积后由 WHERE 过滤。

### 执行器

**包布局**：
- `internal/cypher/`：lexer、递归下降 parser、AST、executor（直接操作 `*engine.Snapshot`，同 module 内 internal 可见）。
- 根包 `cypher.go`：`func (g *Graph) Cypher(query string) (*CypherResult, error)` facade（要求已 Seal，错误用 `ErrNotSealed`；解析错/语义错返回带位置的 `CypherError`）。

**执行算法（行式展开）**：
1. **锚定**：取第一个路径的首节点模式为锚。有 `key` 等值或 propMap 中某属性已索引（含数值范围单一化）→ 走索引求起点集；否则全节点扫描按节点谓词过滤。其余节点模式的谓词在展开到位时过滤。
2. **逐段展开**：行 = 变量绑定元组。定长单跳：对每行，从当前位置按方向 + labelMask 枚举邻接（CSR 行），绑定边 ID（若具名）与下一节点（若具名/重复 var 校验一致）。变长 n..m：对每行做定制 BFS（labelMask 过滤边、**中间节点不施加目标谓词**、收集深度 ∈ [n,m] 的终点去重），终点过目标谓词后生成新行；变长段不绑定边。
3. **行去重**：按全部具名变量绑定元组去重（map 以序列化绑定为 key）。
4. **WHERE 过滤** → **RETURN 投影** → **ORDER BY**（排序键必须在 RETURN 项中；数值/字符串可比，混型/含 null 按固定全序：null < bool < number < string < node/edge(按 key/label 序)）→ **LIMIT**。

**CypherResult**（JSON 友好）：

```go
type CypherResult struct {
    Columns []string           // RETURN 项显示名："a"、"b.key"、"*" 展开为全部 var
    Rows    []map[string]any   // 列名 -> Node | Edge | string | float64 | bool | nil
}
```

Node/Edge 已有 json tag，server/MCP 直接序列化。

### 接入面

- **REST**：`POST /v1/cypher`，body `{"query": "MATCH ..."}`；响应 `{"columns": [...], "rows": [...], "count": n}`；沿用 readiness gate / `X-Vectra-Snapshot` / queryCount 语义；解析/语义校验错 400（`code:"PARSE"`），执行错 500。不额外截断（与 `/v1/query` 一致）。
- **MCP**：新工具 `cypher`（readOnlyHint），params `{query string, limit int?}`；默认 limit 1000，`total` 报真实总数（沿用 listOut 风格）；同步更新 server Instructions；mcp/ts 与 mcp/python 的工具清单镜像同步。
- **文档**：README.md / README.zh-CN.md 增补「Cypher 子集」一节（语法表 + 示例 + 语义说明：节点对语义、变长段限制）；属性索引一节改为类型化描述。

### 错误处理

- 解析错误：`line:col + 期望/实际 token`，一层足够。
- 语义错误（RETURN 未绑定 var、ORDER BY 不在 RETURN、引用变长段边变量、重复 var 不一致）：执行前校验，全部静态报错。
- 未索引属性作为锚：回退全扫（不报错）；WHERE 求值遇 null 属性 → 该行过滤（Cypher 三值逻辑简化：null 参与比较即 false，NOT null = false…采用「null 传播为 false」的简化并文档化）。

## 实现期偏差记录（相对本 spec 原文，均已同步 README）

1. **ORDER BY 放宽**：原 spec 要求排序键必须出现在 RETURN 项中；实现放宽为"引用已声明的变量即可"（贴近 Cypher，`RETURN a ORDER BY b.w` 合法），排序值在绑定上直接求值。
2. **propMap 伪属性**：节点模式的 `{key:'x'}` / `{label:'X'}` 匹配节点主键/label 字段（遮蔽同名属性）——Vectra 的 key/label 是一等字段，这样写最自然；它们不走索引、由节点谓词兜底。
3. **行去重键**：具名绑定 + 前沿位置 pos（保证匿名终点区分行）；多路径逗号 join 时 pos 指向最后一个路径的终点，更早路径中仅匿名节点不同的行会被折叠（v1 已知边界，文档化）。

## 测试计划

- engine：混合类型 props 的 Seal 抽取、三桶排序、LookupIndex/Number/NumberRange/Bool、未索引 key、非可索引类型跳过、Load 重建（v3 往返 + int→float64 归一化断言）。
- cypher 包：lexer/parser 表驱动（含错误位置）；执行器——锚定索引命中/回退、链式模式、混合方向、`--` 双向、变长 1..n / n..m / n.. / ..m / *、变长中间节点不过滤的语义用例、WHERE 全操作符、ORDER BY/LIMIT、行去重、并行边多行、变长边变量引用报错、共享 var join、笛卡尔积。
- 根包 facade：未 Seal 报错、结果类型。
- server：`/v1/cypher` handler 测试（对照现有 handler 测试风格）。
- mcp：工具注册 + 查询返回结构。
- 回归：`go test ./...`、`go vet`。

## 影响面

新增：internal/cypher（lexer.go/parser.go/ast.go/exec.go + 测试）、根包 cypher.go、若干测试文件。
修改：internal/engine/engine.go（propIndex）、vectra.go（三个 Lookup facade）、server/api.go（路由 + handler）、mcp/server.go（工具 + Instructions + lookup_property propValue 类型）、mcp/ts、mcp/python（工具镜像）、README ×2。
不动：store 格式（v3）、raft/fsm、遍历算法。
