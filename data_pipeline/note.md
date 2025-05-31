# surrogate key 代理键
作用：将多个自然键（如 l_orderkey + l_linenumber）组合生成一个唯一的、确定性的哈希值作为代理键。

假设原始数据中有：

l_orderkey	l_linenumber	...
1001	1	...
1001	2	...
如果直接用 l_orderkey 作为主键：

无法唯一标识每一行（因为同一订单可能有多个行项目）。

如果源系统键值变更（如订单号格式变化），会破坏历史数据关联。

代理键的解决方案
生成唯一的 order_item_key：

order_item_key (代理键)	l_orderkey	l_linenumber	...
a1b2c3d4	1001	1	...
e5f6g7h8	1001	2	...


代理键特性
特性	说明	示例
唯一性	确保每条记录有唯一标识符	a1b2c3d4 ≠ e5f6g7h8
稳定性	不依赖业务系统的键值变化	即使 l_orderkey 格式变更也不影响
无意义性	仅作为技术键，不包含业务信息	哈希值本身无业务含义
跨系统一致性	统一不同来源的键值（如合并两个系统的数据）	解决不同系统的ID冲突

# fact table

A fact table is the central table in a star schema or snowflake schema data warehouse model that contains:

1. Core Components
Component	Description	Example from Your lineitem Model
Measures/Facts	Numeric, additive business metrics (quantities, prices, etc.)	quantity, extended_price, discount_percentage, tax_rate
Foreign Keys	Links to dimension tables (usually surrogate keys)	order_key, part_key, order_item_key (your surrogate key)
Degenerate Dimensions	Transactional IDs stored directly in fact tables (no dimension table)	l_linenumber (if not linked to a dimension)
2. Key Characteristics
Grain: Defines the level of detail (one row per line item in your case)

Sparse: Typically has many rows but few columns

Additive: 90%+ of columns should be summable/averagable metrics