# QEE-S³ 后端工程手册（Final Specification，含 QEE-S³.1 极端边界加固）

> **时间制度**：系统内所有时间统一使用 **北京时间 Asia/Shanghai**，输出格式为 `YYYY-MM-DD HH:MM:SS.mmm`（含毫秒时保留 3 位）。  
> **主权级原则**：不依赖外部组件的“善意”，只依赖系统定义的确定性规则、证据链与治理制度。  
> **同花顺数据源约束**：所有 API 统一采用同花顺体系，工程上分为 `IFIND_HTTP / IFIND_SDK / DATAFEED` 三通道。

---

## 0. 核心哲学与不可变公理（Invariants）

### 0.1 主权级公理（永不妥协）
1. **确定性（Determinism）**：同一输入事件流 + 同一版本集合 ⇒ 输出必须可复现。  
2. **可审计（Auditability）**：任何交易相关状态改变必须可追溯到结构化事件与签名（若涉及人工）。  
3. **可回放（Replayability）**：任意时刻都能从事件流重建 Portfolio / Order 状态。  
4. **抗外部不可靠（Adversarial Externality）**：券商/行情/API 可能截断、乱序、缺失、延迟、重复、抖动——系统将其视为常态。  
5. **宪法与政策隔离**：策略“合约边界（StrategyContract）”是宪法；权重/阈值是政策。政策可迭代，宪法变更需冷启动。

### 0.2 三层真相（Truth Model）
- **原子事实（Atomic Facts）**：Fills/回报/原始行情事件。永远优先于派生状态。  
- **派生状态（Derived State）**：订单状态、持仓状态、指标状态——必须可由原子事实重建。  
- **叙事解释（Narrative）**：Reason_Code/Decision_Bundle，用于审计与理解，但不得篡改原子事实。  

---

## 1. 系统总体架构（模块与责任边界）

### 1.1 五官分层（脑/足/手/眼/卫）
- **脑（Reasoning Engine）**：特征提取、评分、置信度、联邦投票、生成信号（Signal）。  
- **足（Persistence & Integrity）**：单写者落库、事件溯源、版本化快照、可回放状态重建。  
- **手（Execution Engine）**：幂等状态机、外部订单映射、多锚点对账、Fills-first 恢复。  
- **眼（Observability）**：SystemEvents 黑匣子、指标、漂移告警、调度漂移记录。  
- **卫（Risk & Governance Guard）**：多因子风控等级制、宪法级参数、PANIC_HALT、多签复位、三权分立。  

### 1.2 同花顺数据源统一策略（三通道）
> 约束：**所有 API 采用同花顺 API**。工程落地为三类 Source：  
> - **IFIND_HTTP**：iFinD HTTP API（token 模式），适合服务化与审计（完整记录 request/response）。  
> - **IFIND_SDK**：iFinD SDK（本地库登录态），适合研究机/离线批处理或特定环境。  
> - **DATAFEED**：同花顺 DataFeed 实时推送，用于交易时段实时 L1/L2（尤其 L2 逐笔/委托队列）。  
>
> 能力边界：实时逐笔/委托队列等 L2 **必须走 DataFeed**；iFinD（SDK/HTTP）更适合历史与非超高频查询。

---

## 2. 数据与时间纪律（The Lineage Layer）

### 2.1 事件入舱统一包（MarketEvent Envelope）
所有数据（L1/L2/L3）入库前必须封装成统一事件。

**字段（必须）**
- `api_schema_version`
- `source`：IFIND_HTTP / IFIND_SDK / DATAFEED
- `source_clock_quality`：EXCHANGE / AGGREGATED / SOURCE_CLOCK_UNKNOWN
- `channel_id`：按数据通道区分
- `channel_seq`：通道序号（强制单调递增）
- `symbol`
- `time`：
  - `data_ts`（AsOfTime：市场事件发生时刻）
  - `ingest_ts`（系统收到时刻）
- `payload`
- `quality`：
  - `data_status`：VALID / MISSING / INVALID / STALE
  - `latency_ms`
  - `completion_rate`
- `fingerprints`：`payload_sha256`
- `trace`：`request_id` / `producer_instance`

**同花顺血统扩展（必须）**
- `ths_product`：IFIND_HTTP / IFIND_SDK / DATAFEED
- `ths_function`：iFinD 函数名或 endpoint 名
- `ths_indicator_set`：指标集合（规范化排序）
- `ths_params_canonical`：参数规范化字符串（排序、去空白、固定分隔符）
- `ths_errorcode`：原样记录（成功也要 0/OK）
- `ths_quota_context`：账户/限额/权限摘要（用于区分“没信号”vs“拿不到数据”）

### 2.2 双轴时间戳与“可见性”规则（Anti-Leakage Core）
系统任意回放/影子评估都遵循：  
- 仅允许看到满足：`data_ts <= T` 且 `ingest_ts <= T + ε` 的事件

其中 ε 为**动态窗口**：  
- `ε = max(CurrentP99Latency(channel_id), ε_min)`  
- `ε_min` 默认 200ms（可配置，写入 RuleSet/Contract）

### 2.3 Ingest_TS 单调性纪律（Channel Sequencer）
对每个 `channel_id` 维护 `ChannelCursor`：  
- 规则：  
  - `channel_seq_new > channel_seq_last` 必须成立  
  - 同源同通道：`ingest_ts_new >= ingest_ts_last` 必须成立  
- 若违反：  
  - 标记为 `Channel_Jitter`  
  - 该事件降级 `ResearchOnly`  
  - 下调 DataQuality 分数并写入 `SystemEvents(DATA_DEGRADED)`

### 2.4 双维可用性（Double-Track Usability）
每条事件必须落两个布尔标记：  
- `realtime_flag`：是否满足实时决策时延门槛  
- `audit_flag`：是否满足审计/研究/进化使用门槛（晚到可用，错到不可用）

---

## 3. 真实等效性（Realtime_Equivalent）与训练输入隔离

### 3.1 反目标泄漏协议（Anti-Leakage Protocol）
建立 `Training_Feature_Store`，并强制：  
- 训练引擎只允许读取满足：  
  - `audit_flag == true`  
  - `realtime_equivalent == true`

`realtime_equivalent` 的判定必须基于当时的可见性规则（T 与 ε），并锁定版本：  
- 若某特征使用了盘后回填/事后修正输入 ⇒ `realtime_equivalent=false`（训练不可用）

> 标签（收益/涨跌）允许来自盘后官方数据；**但输入特征绝不允许被盘后替换**。

### 3.2 数据血统（Lineage）硬绑定
任何特征行必须能追溯：  
- 原始事件 `payload_sha256`
- 事件 `data_ts / ingest_ts`
- `source_clock_quality`
- `channel_id/channel_seq`
- 生成特征的 `FeatureExtractorVersion`
- 当时的 `RuleSetVersionHash` 与 `StrategyContractHash`

---

## 4. 执行与一致性（The Integrity Layer）

### 4.1 策略合约（StrategyContractHash）——宪法
合约包含且必须哈希冻结：  
- Universe（标的域规则）
- 交易频率上限（每标的每日 N 次入场/加仓限制）
- 允许的订单类型集合（LIMIT/MARKET/…）
- 风险底线（最大仓位、最大回撤阈值、最大滑点容忍、禁止条件等）
- 关键治理约束（如 Guard 不可由进化修改）

变更合约 ⇒ 视为**新物种**：  
- 必须进入冷启动流程（Shadow Live M 天）  
- 与旧合约的权重不可直接比较替换

### 4.2 信号 CID（唯一指令标识）
CID 必须包含“意图完整性”：  
`CID = Hash(TradingDay, Symbol, StrategyID, SignalTS, Nonce, Side, IntendedQtyOrNotional)`

- Nonce：同日同标的多次触发的序列因子  
- CID 为系统内主键，贯穿 Signals/Orders/DecisionBundle/TradeLog

### 4.3 OrderIdentity：外部幂等映射（Mapping Nail）
强绑定链路：`CID <-> ClientOrderId <-> BrokerOrderId`

- `ClientOrderId = "QEE-" + CID`（若券商截断，仍靠多锚点归位）
- 记录 `RequestUUID / AckHash / RawResponseHash` 等冗余锚点

### 4.4 规范化锚点协议（Canonicalization Protocol）
为消灭价格/数量语义歧义，Metadata_Hash 输入必须先规范化。

#### 4.4.1 Tick/Lot 离散化规则绑定
每次生成订单锚点时，必须绑定：  
- `TickRuleVersion`
- `LotRuleVersion`

并从规则表得到：  
- `tick_size`
- `lot_size`

#### 4.4.2 价格与数量规范化
- Price：按 tick 离散化后转换为 `Int64 = round(price * 10000)`  
  - 市价单：LimitPrice_Int64 置 0 或特定 Enum（参与 hash）
- Qty：按 lot 约束离散化后转换为整数手/股，并用固定字节序（Big-Endian）序列化
- OrderType/Side：使用固定 Enum 编码

#### 4.4.3 Metadata_Hash 二进制序列化标准
`Metadata_Hash = SHA256( Serialize( CID, OrderType, LimitPrice_Int64, Qty_Int, Side, TickRuleVersion, LotRuleVersion ) )`

> 序列化标准必须版本化：`CanonicalizationVersion`。

### 4.5 幂等状态机（至死不渝）

#### 4.5.1 状态集合
- CREATED
- SUBMITTED
- PENDING
- PARTIALLY_FILLED
- FILLED（终态）
- CANCELLED（终态）
- REJECTED（终态）
- EXPIRED（终态）
- AMBIGUOUS（挂起终态，需裁决）
- PANIC_HALT（系统级终态，需复位）

#### 4.5.2 跃迁保护（双保险）
- 乐观锁：`version_id` 递增  
- 幂等跃迁令牌：`transition_id`（UUID），并加 Unique Constraint 防重复写  

写入条件：  
`WHERE CID=:cid AND version_id=:current_version AND last_transition_id!=:transition_id`

### 4.6 AMBIGUOUS 解歧路径（冲突挂起机制）
触发条件：多锚点匹配出现 1 对多候选（或不可唯一归位）  
- 订单进入 `AMBIGUOUS`
- 触发 Veto：停止该标的后续指令链（防扩散）
- 生成“离线快照”：候选集合固定化（候选订单列表 + 匹配分数 + 回报哈希）
- 必须产生带签名的事件：`RECONCILE_DECISION`
  - 裁决是“写事件”，不是“偷偷改状态”
  - 允许二次裁决覆盖，但必须链式引用上一次裁决（不可抹去历史）

### 4.7 Fills-First 对账与崩溃恢复（Deterministic Recovery）
恢复启动顺序：  
1) **先查外部**：QueryOrders / QueryFills（当日）  
2) 以 `ClientOrderId` 优先匹配；若不可靠，用（Symbol+Side+Metadata_Hash+时间窗口）辅助匹配  
3) **先落成交原子事实**：写入 `Trade_Fills`  
4) 基于 fills 累计量推导订单状态（逆向重构）  
5) 如 DB 与外部状态不一致：写 `RECONCILE_COMPENSATION` 事件并进行补偿跃迁  

> 永远以 fills 为准，不以订单状态为准。

---

## 5. 进化与沙盒（The Evolution Sandbox）

### 5.1 Shadow Validation + Shadow Live
- Shadow Validation：在历史窗口（例如 10 个交易日）做回放评估  
- Shadow Live：新权重上线后 M 天“只算不下单”，并与现役权重对比

### 5.2 全要素评估模型（防刷分）
必须同时计算：  
- Net_Return（扣除成本）
- Sharpe / WinRate（基础）
- Max_Drawdown / TailLoss（最坏 5% 样本）
- Opportunities_Capture_Rate（信号捕获率）
- Trade_Rate_Delta（机会下降阈值，如 >20% 触发 Strategy_Shrinkage）

### 5.3 滑点阶梯成本模型（Slippage Ladder）
根据：  
- `Avg_Volume`（成交量/成交额）
- `Volatility`（波动）

构造分段摩擦成本：  
- 流动性差或波动大：成本上浮（可至 0.5%）
- 逼迫系统对“大成交量承接”形成偏好

> 成本模型版本化：`CostModelVersion` 写入 ModelSnapshot。

### 5.4 权重更新闸门与回滚
- 样本量门槛（置信度加权 N 笔）
- 评估门槛（多指标同时通过）
- 版本快照（UUID）
- 默认加载 Stable
- 支持回滚与灰度切换（事件记录）

---

## 6. 风险与治理（The Governance Layer）

### 6.1 Guard 多因子等级制度（响应矩阵）
输出：  
- `guard_level`：0/1/2/3（Green/Yellow/Orange/Red）
- `veto`：是否否决买入/或全禁
- `veto_code`：规则 ID
- `market_score`：多因子合成值

等级响应：  
- Green：正常
- Yellow：减仓、提高阈值、或仅允许低风险单
- Orange：停止买入，只允许卖出与风控动作
- Red：PANIC_HALT：撤销挂单、系统挂起

### 6.2 宪法级参数隔离
Guard 阈值参数**禁止**由 Evolution 自动修改。  
修改必须走配置变更审计流（写事件、签名、版本）。

### 6.3 三权分立与多签复位（Multi-Sig RESET）
角色与权限：  
- Role A（DevOps）：部署/环境配置（无权改 Guard 宪法）
- Role B（Risk Officer）：Guard 参数与复位签署
- Role C（Compliance）：只读审计、回放验证

复位流程：  
1) A 发起 RESET 申请（生成 Challenge_Code）  
2) B 使用不同 Key 签署（多签）  
3) 系统执行自检（DB 完整性、网络延迟、账户平衡、事件链完整）  
4) 自检报告哈希（SHA-256）写入永恒表  
5) C 审计回放：无自检哈希则拒绝装载交易指令集  

### 6.4 密钥管理（Key Registry）
- Key 版本号、过期时间
- 撤销列表（Blacklist）
- 轮换机制（Rotation）
- 权限最小化（签 RESET 与改 Guard 参数可拆分）

---

## 7. 审计黑匣子（The Audit Flight Recorder）

### 7.1 SystemEvents（永恒表）
记录：  
- 数据降级/乱序/超时
- API 报错与限额
- 调度漂移
- 权重更新/切换/回滚
- Guard 状态变化
- PANIC_HALT/RESET 相关事件
- 冲突挂起与裁决（RECONCILE_DECISION）

### 7.2 Decision Bundle（结构化证据包）
每次决策必须输出：  
- `CID`（若未下单可为空，但必须有 DecisionId）
- `decision`：BUY/SELL/HOLD
- `reason_code`（版本化）
- `params`（关键阈值与观测值）
- `guard_status`
- `data_quality`
- `versions`：
  - `RuleSetVersionHash`
  - `ModelSnapshotUUID`
  - `StrategyContractHash`
  - `FeatureExtractorVersion`
  - `CostModelVersion`
- `lineage_ref`：引用原始事件 fingerprint

### 7.3 Reason_Code 语义冻结 + RuleSetVersionHash
- 同一 reason_code 永远代表同一逻辑含义
- 逻辑变更必须迭代 code 版本
- Decision Bundle 必须关联 `RuleSetVersionHash`，确保“当日规则集不可被悄悄改写”

### 7.4 可搜索知识图谱（Index Strategy）
把 JSON 里关键参数抽取成物理列（Features_Index）：  
- 支持秒级查询：如 `WHERE slope > 2.0 AND vol_ratio > 3.0`
- 便于审计与策略体检

---

## 8. 数据库逻辑模型（核心表清单，不遗漏）
> 表可以用 SQLite（单写者）落地，但生产建议使用支持并发/事务更强的数据库。无论使用何种存储，**事件语义与约束必须一致**。

### 8.1 原始事件与通道
- `RawMarketEvents`（MarketEvent Envelope）
- `ChannelCursor`（每 channel_id 的 last_seq/last_ingest_ts/quality_score）
- `InstrumentRuleCache`（tick/lot 规则版本与参数）

### 8.2 决策与信号
- `Signals`（大脑产生的原始脉冲，含 CID）
- `DecisionBundles`（审计证据包）
- `RuleSets`（RuleSetVersionHash -> 规则定义快照）
- `StrategyContracts`（StrategyContractHash -> 合约定义）

### 8.3 执行与对账
- `Orders`（CID、ClientOrderId、BrokerOrderId、Metadata_Hash、state、version_id、last_transition_id）
- `Trade_Fills`（fill_id 去重键、成交时间、价量、关联 BrokerOrderId/CID）
- `ReconcileSnapshots`（冲突候选集合固定快照）
- `ReconcileDecisions`（带签名裁决事件引用）

### 8.4 资产与组合
- `PortfolioPositions`（可由事件重建，但可缓存）
- `TradeLog`（交易瞬间特征快照 + Correlation_ID + Execution_State）
- `T1Constraints`（T+1 约束状态）

### 8.5 进化与评估
- `Training_Feature_Store`（只存 audit_flag & realtime_equivalent 的可训练输入）
- `ModelSnapshots`（权重版本、评估报告、eco-metrics、cost model version）
- `ShadowRuns`（Shadow Validation / Shadow Live 的对比结果）

### 8.6 治理与密钥
- `GuardianKeys`（Key_ID、版本、过期、撤销状态）
- `SystemStatus`（PANIC_HALT 锁死状态与复位门禁）
- `SystemEvents`（永恒表）

---

## 9. 运行时工作流（端到端闭环）

### 9.1 交易日盘前（T-0 / T+1 前）
- 拉取概率/候选 → 生成 WatchList（写入事件）
- 冻结当日版本集合：
  - RuleSetVersionHash
  - StrategyContractHash
  - Stable ModelSnapshotUUID
  - CostModelVersion
  - CanonicalizationVersion

### 9.2 盘中轮询/推送（09:30-15:00）
- DATAFEED/IFIND 获取 → MarketEvent 入舱  
- Sequencer 校验 → usability 打标  
- Feature Processor → 生成特征（带血统）  
- Reasoning Engine → Signal/CID（可能 HOLD）  
- Guard → 等级与 veto  
- Execution Engine（仅在允许时）→ OrderIdentity → 下单  
- 回报/成交 → fills 入库 → 状态重建  

### 9.3 盘后进化（Post-Market）
- 对账：盘中快照 vs 盘后回填，异常样本剔除  
- Shadow Validation → 候选权重入池  
- Shadow Live（观察期）→ 通过才切换  
- 所有变更写事件，可回滚  

---

## 10. 同花顺接入适配器规范（必须遵守）

### 10.1 IFIND_HTTP Adapter
- 请求与响应必须落审计：`request_json`, `response_json`, `http_status`, `ths_errorcode`
- token 生命周期与版本记录
- IP 绑定/配额上下文写入 `ths_quota_context`
- rate limiting：按函数/账号/全局三层限流
- 重试：只允许对**幂等查询**重试；写入类操作由内部幂等机制保障

### 10.2 IFIND_SDK Adapter
- 记录调用上下文：函数名、指标、参数、登录态摘要
- SDK 环境信息（版本、平台、实例 ID）写入 trace
- 任何异常必须写 SystemEvents（包括“重复登录/绑定限制/初始化失败”）

### 10.3 DATAFEED Adapter
- 每条推送携带 `channel_seq`（若源不提供，必须由本地通道定序器补齐）
- 乱序/重复/丢包必须显式记录为事件（不要悄悄“补”）
- 对 L2 事件必须支持去重键（例如交易所序号/本地哈希）

---

## 11. 工程检查清单（可执行）

### Integrity Layer
- [ ] CID 生成包含 Nonce，避免同日同标的多次信号冲突  
- [ ] OrderIdentity 三段映射完整落库  
- [ ] CanonicalizationVersion 固化，tick/lot 规则绑定入 hash  
- [ ] version_id + transition_id 双保护  
- [ ] AMBIGUOUS 必挂起并生成离线快照  
- [ ] Fills-first 恢复：先 fills 后状态  

### Lineage Layer
- [ ] data_ts / ingest_ts 双轴齐全  
- [ ] channel_seq 强制递增  
- [ ] 动态 ε 来自 P99 + ε_min  
- [ ] realtime_flag / audit_flag 双维可用性完整  

### Evolution Sandbox
- [ ] 训练只用 realtime_equivalent=true 的输入  
- [ ] eco-metrics 完整（capture rate、tail loss、trade rate delta）  
- [ ] cost model 版本化  
- [ ] shadow live 观察期后才切换  

### Governance
- [ ] Guard 宪法参数不可被进化修改  
- [ ] PANIC_HALT 锁死，必须多签复位  
- [ ] 三权分立权限隔离  
- [ ] Key 轮换/撤销机制可用  

### Audit
- [ ] SystemEvents 永恒表完整覆盖异常与关键行为  
- [ ] DecisionBundle 结构化证据包齐全  
- [ ] reason_code 版本化 + RuleSetVersionHash 绑定  
- [ ] 关键参数索引化可秒级查询  

---

## 12. QEE-S³.1 极端边界加固条款（必须启用）
以下三项为“极端边界压力测试”引入的制度化增强，属于 **强制约束**（不可选）。

### 12.1 冷启动期间的决策漂移防线：Differential Audit（差异对冲审计）

#### 12.1.1 触发条件
- `StrategyContractHash` 发生变更 ⇒ 进入 Shadow Live（M 天）期间 **强制开启** Differential Audit。

#### 12.1.2 核心规则
在 Shadow Live 期间，新旧合约对同一标的、同一时刻窗口产生决策：  
- 若出现 **方向冲突**（`BUY` vs `HOLD/SELL`，`SELL` vs `HOLD/BUY`）  
- 或同方向但 **风险/仓位显著不同**（`position_delta` > 阈值、或 `guard_level_delta` 显著）  
⇒ 必须写入 `CONTRA_DECISION_EVENT` 至 `SystemEvents` 永恒表。

#### 12.1.3 事件定义：CONTRA_DECISION_EVENT（SystemEvents）
**最小字段集合**
- `event_type = "CONTRA_DECISION_EVENT"`
- `symbol`
- `data_window`: `{start_ts, end_ts}`
- `old_contract_hash`, `new_contract_hash`
- `old_decision_bundle_ref`, `new_decision_bundle_ref`
- `diff_summary`：
  - `decision_direction_diff`（ENUM）
  - `confidence_delta`（float）
  - `position_delta`（float）
  - `guard_level_delta`（int）
  - `data_quality_delta`（object）
- `severity`：INFO / WARN / ERROR
- `time`

#### 12.1.4 冷启动通过门槛（Exit Criteria）增补
Shadow Live “可切换上线”必须同时满足：  
- 原有多指标评估门槛（Net_Return / TailLoss / Eco-Metrics 等）  
- 新增：  
  - `CONTRA_DECISION_RATE <= THRESHOLD`  
  - 所有 **ERROR 级 CONTRA** 必须完成审计（Role C 只读回放确认）

### 12.2 三通道数据真假对账：Cross-Source Reconciliation + Source Fidelity Score

#### 12.2.1 触发条件
- Post-Market 进化环节：每日收盘后必执行（训练卫生协议的一部分）。

#### 12.2.2 对账对象（按标的、按分钟）
针对同一 `symbol`、同一 `data_ts`（例如 1m bar close），采集并对账：  
- `DATAFEED`（盘中推送）  
- `IFIND_HTTP`（盘后补录/查询）  
- `IFIND_SDK`（若启用则作为第三参考源）

#### 12.2.3 Source Fidelity Score（跨源一致性评分）
为每个 `channel_id` 计算 `SourceFidelityScore`（0~1 或 0~100）：  
- 对同一分钟收盘价 `close`：  
  - 计算 `abs_diff = |close_A - close_B|`  
  - 在排除可解释差异（tick 离散化、时间边界、口径差异）后：  
    - 若 `abs_diff > diff_threshold` ⇒ 记一次 mismatch  
- 日终统计 mismatch rate，并映射为 Score  

**建议阈值**
- `diff_threshold_price = max(tick_size, tick_size * K)`（K 可配置）  
- 对成交量/成交额允许更宽误差区间

#### 12.2.4 动作：自动调降通道 DataQuality 权重
- 若 `SourceFidelityScore` 下降超过阈值：  
  - 写 `SystemEvents(SOURCE_FIDELITY_EVENT / SOURCE_FIDELITY_DEGRADED)`  
  - 下调 `quality_weight`  
  - 若连续 N 日低于下限 ⇒ 强制降级 `ResearchOnly` 或提高惩罚延迟（对 `SOURCE_CLOCK_UNKNOWN`）

#### 12.2.5 事件定义：SOURCE_FIDELITY_EVENT（SystemEvents）
**最小字段集合**
- `event_type = "SOURCE_FIDELITY_EVENT"`
- `symbol`
- `data_ts`
- `channel_id_a`, `channel_id_b`
- `metric`: `{close_a, close_b, abs_diff, threshold}`
- `fidelity_score_before`, `fidelity_score_after`
- `action_taken`：NONE / DEGRADE / WEIGHT_DOWN / RESEARCH_ONLY
- `time`

### 12.3 Fills-First 恢复的幽灵成交处理：ORPHAN_FILL_ALARM

#### 12.3.1 Orphan Fill（孤儿成交）判定
在恢复或对账中发现 fill：  
- 无法匹配到任何：  
  - `ClientOrderId`  
  - `BrokerOrderId` 映射  
  - `CID`  
  - `Signals`（意图链）  
⇒ 判定为 `ORPHAN_FILL`

#### 12.3.2 动作：立即进入 GUARD_ORANGE
发现 ORPHAN_FILL：  
- Guard 立刻切换至 `ORANGE`  
- **停止所有自动化动作**（停止买入、停止进化切换、停止策略热更新）  
- 仅允许：  
  - 合约授权的风控性卖出（可选）  
  - 只读审计与人工处置流程  

#### 12.3.3 事件定义：ORPHAN_FILL_ALARM（SystemEvents 永恒表）
**最小字段集合**
- `event_type = "ORPHAN_FILL_ALARM"`
- `severity = "CRITICAL"`
- `account_id`
- `fill_fingerprint`
- `fill_detail`: `{symbol, side, price, qty, fill_ts}`
- `missing_links`: `{client_order_id_found, broker_order_id_found, cid_found, signal_found}`
- `guard_transition`: `{from_level, to_level, veto_code}`
- `time`

---

## 13. 架构可视化补充（Mermaid）

### 13.1 数据流转血统图（Data Lineage Flow）
flowchart LR
  A[THS DataFeed Byte Stream] -->|decode + seq| B[MarketEvent Envelope]
  C[iFinD HTTP Response] -->|normalize| B
  D[iFinD SDK Result] -->|normalize| B

  B --> E{Channel Sequencer\n(channel_seq & ingest monotonic)}
  E -->|PASS| F[Usability Flags\nrealtime_flag/audit_flag]
  E -->|JITTER| G[ResearchOnly + SystemEvents(DATA_DEGRADED)]

  F --> H[Feature Processor]
  H --> I[Training_Feature_Store]
  I --> J{Realtime_Equivalent?}
  J -->|true| K[Train/Evolve Allowed]
  J -->|false| L[Audit Only\n(no training)]

### 13.2 订单状态迁移图（Order State + Reconcile）
stateDiagram-v2
  [*] --> CREATED
  CREATED --> SUBMITTED
  SUBMITTED --> PENDING
  PENDING --> PARTIALLY_FILLED
  PARTIALLY_FILLED --> FILLED
  PENDING --> FILLED
  PENDING --> CANCELLED
  SUBMITTED --> REJECTED
  PENDING --> EXPIRED

  state "Reconcile & Recovery" as R {
    [*] --> RECONCILE_START
    RECONCILE_START --> FETCH_EXTERNAL : QueryOrders/QueryFills
    FETCH_EXTERNAL --> UPSERT_FILLS : Fills First
    UPSERT_FILLS --> DERIVE_STATE : derive order state
    DERIVE_STATE --> COMPENSATION : if mismatch\nwrite RECONCILE_COMPENSATION
    DERIVE_STATE --> DONE : consistent
  }

  state "Ambiguous" as A {
    [*] --> AMBIGUOUS
    AMBIGUOUS --> OFFLINE_SNAPSHOT : freeze candidates
    OFFLINE_SNAPSHOT --> RECONCILE_DECISION : signed decision event
    RECONCILE_DECISION --> COMPENSATION : apply deterministic transition
  }

  PENDING --> AMBIGUOUS : 1-to-many match
  PARTIALLY_FILLED --> AMBIGUOUS : collision
  FILLED --> [*]
  CANCELLED --> [*]
  REJECTED --> [*]
  EXPIRED --> [*]

### 13.3 三权分立：PANIC_HALT / RESET 时序
sequenceDiagram
- ** participant A as Role A (DevOps)
- ** participant B as Role B (Risk Officer)
- ** participant C as Role C (Compliance)
- ** participant S as QEE System

- ** S->>S: Guard triggers RED\nenter PANIC_HALT\nlock SystemStatus
- ** A->>S: Request RESET (generate Challenge_Code)
- ** S-->>A: Challenge_Code
- ** B->>S: Sign Challenge_Code (multi-sig)
- ** S->>S: Run System_Self_Check\n(DB integrity, latency, balance, event-chain)
- ** S->>S: Write Self_Check_Report_Hash to SystemEvents (immutable)
- ** C->>S: Audit replay (read-only)\nverify report hash & event chain
- ** C-->>S: Audit Approved (out-of-band)
- ** S->>S: Unlock SystemStatus\nLoad trading instruction set

## 14. 工程检查清单（最终合并版，不遗漏）

### 14.1 Integrity Layer
- ** CID 生成包含 Nonce，避免同日同标的多次信号冲突
- ** OrderIdentity 三段映射完整落库（CID ↔ ClientOrderId ↔ BrokerOrderId）
- ** CanonicalizationVersion 固化；TickRuleVersion/LotRuleVersion 绑定入 Hash
- ** version_id + transition_id 双保护（含 Unique Constraint）
- ** AMBIGUOUS 必挂起并生成离线快照；裁决写 RECONCILE_DECISION 事件
- ** Fills-first 恢复：先 fills 后状态；差异补偿写 RECONCILE_COMPENSATION 事件
- ** ORPHAN_FILL 检测可用；触发后进入 GUARD_ORANGE 并写 ORPHAN_FILL_ALARM

### 14.2 Lineage Layer
- ** data_ts / ingest_ts 双轴齐全
- ** channel_seq 强制递增；乱序触发 Channel_Jitter 降级
- ** 动态 ε = max(P99, ε_min)；用于回放可见性门控
- ** realtime_flag / audit_flag 双维可用性完整
- ** Cross-Source Reconciliation 盘后必跑；SourceFidelityScore 驱动权重降级

### 14.3 Evolution Sandbox
- ** 训练只用 realtime_equivalent=true 的输入（Anti-Leakage Protocol）
- ** eco-metrics 完整（capture rate、tail loss、trade rate delta）
- ** cost model 版本化；slippage ladder 生效
- ** shadow live 观察期后才切换
- ** Differential Audit 生效：记录 CONTRA_DECISION_EVENT；冲突率纳入上线门槛

### 14.4 Governance
- ** Guard 宪法参数不可被进化修改
- ** PANIC_HALT 锁死，必须多签复位
- ** 三权分立权限隔离（A申请/B签署/C审计）
- ** Key 轮换/撤销机制可用

### 14.5 Audit
- ** SystemEvents 永恒表覆盖所有异常与关键行为
- ** DecisionBundle 结构化证据包齐全（含 versions 与 lineage_ref）
- ** reason_code 版本化 + RuleSetVersionHash 绑定，语义冻结
- ** 关键参数索引化（Features_Index），可秒级查询与回放定位

## 结案
QEE-S³（含 S³.1 加固）已完成从“技术工具”到“制度机器”的跨越：
- ** 确定性：状态机 + 幂等键 + 规范化锚点 + 成交原子事实优先
- ** 真实性：双轴时间纪律 + 动态可见性 ε + 实盘等效训练隔离
- ** 可控性：Differential Audit + Cross-Source Reconciliation + Orphan Fill 触发降级
- ** 可审计：黑匣子事件流 + 证据包 + 版本冻结 + 三权分立多签治理