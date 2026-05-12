# Stock FlowHunt 会员码记录

> 仅作人工查询记录，方便在仓库里快速找到。
> 生产环境真正生效的位置仍然是 Vercel Environment Variables。
>
> - `MEMBER_LICENSE_CODES`：正式会员码，逗号分隔
> - `MEMBER_TRIAL_CODES`：有效期 / 试用会员码，逗号分隔
> - `MEMBER_TRIAL_DAYS`：试用有效期天数，当前代码默认 30 天
>
> 更新时间：2026-05-13 06:36 +0800

## 正式会员码

| 会员码 | 类型 | 状态 | 备注 |
|---|---|---|---|
| `PRO-K9G9-WYNS-5YDB` | 正式码 | 已配置生产环境 | 原有码，保留 |
| `PRO-9MR3-RAT9-DHAB` | 正式码 | 已配置生产环境 | 新增 |
| `PRO-H5VF-SPXG-5LVF` | 正式码 | 已配置生产环境 | 新增 |
| `PRO-RSJJ-37BK-FUP5` | 正式码 | 已配置生产环境 | 新增 |
| `PRO-6C9W-3P49-K8V5` | 正式码 | 已配置生产环境 | 新增 |
| `PRO-8L6U-VMEG-HR59` | 正式码 | 已配置生产环境 | 新增 |

## 有效期码 / 试用码

| 会员码 | 类型 | 有效期 | 状态 | 备注 |
|---|---|---|---|---|
| `TRIAL-30D-9TU6-7RNG-SH6E` | 有效期码 / 试用码 | 首次兑换后 30 天 | 仅记录，待写入 `MEMBER_TRIAL_CODES` 后生效 | 需要同时确认 `MEMBER_TRIAL_DAYS=30` |

## 快速复制

正式码逗号分隔：

```text
PRO-K9G9-WYNS-5YDB,PRO-9MR3-RAT9-DHAB,PRO-H5VF-SPXG-5LVF,PRO-RSJJ-37BK-FUP5,PRO-6C9W-3P49-K8V5,PRO-8L6U-VMEG-HR59
```

有效期码 / 试用码：

```text
TRIAL-30D-9TU6-7RNG-SH6E
```

## 生效方式

正式码已写入生产环境 `MEMBER_LICENSE_CODES`。

有效期码如果要正式生效，需要写入 Vercel Production 环境变量：

```bash
MEMBER_TRIAL_CODES=TRIAL-30D-9TU6-7RNG-SH6E
MEMBER_TRIAL_DAYS=30
```

写入后需要重新部署生产环境，新的部署才会读取到环境变量。
