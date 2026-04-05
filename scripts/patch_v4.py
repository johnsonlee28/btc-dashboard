#!/usr/bin/env python3
"""
patch_v4.py — BTC Dashboard 全面修正
修复 7 个问题：
1. 满分标注 100→105
2. 前端优先从 data.json 读价格（CoinGecko 降级为备用）
3. 使用指南评级阈值同步
4. 布局重组：加权逻辑说明移到评级框后面
5. 稳定币阈值调整（180B→250B）
6. OI 评分注释说明局限性
7. 一票否决移到模块后面（先看结论和评分，再看风控细节）
"""
import os, sys

HTML_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'index.html')

with open(HTML_FILE, 'r', encoding='utf-8') as f:
    html = f.read()

patches = []

# ================================================================
# FIX 1: 满分标注 "综合得分 / 100" → "综合得分 / 105"
# ================================================================
patches.append(('FIX1 满分标注',
    '<div class="total-score-label">综合得分 / 100</div>',
    '<div class="total-score-label">综合得分 / 105</div>'
))

# ================================================================
# FIX 3: 使用指南评级阈值同步
# ================================================================
patches.append(('FIX3a 指南A分',
    """<div class="guide-grade-row"><span class="guide-grade grade-A">A ≥80分</span><span>值得投入 — 可分3-5批建仓，不追单日大阳线</span></div>""",
    """<div class="guide-grade-row"><span class="guide-grade grade-A">A+ ≥90分</span><span>历史级机会 — 重仓3-5批建仓，总仓位80-100%</span></div>
          <div class="guide-grade-row"><span class="guide-grade grade-A">A ≥78分</span><span>积极建仓 — 分批建仓60-80%，不追单日大阳线</span></div>"""
))

patches.append(('FIX3b 指南B分',
    """<div class="guide-grade-row"><span class="guide-grade grade-B">B 65-79分</span><span>轻仓定投 — 小仓位试探，不重仓追高</span></div>""",
    """<div class="guide-grade-row"><span class="guide-grade grade-B">B 65-77分</span><span>轻仓定投 — 仓位30-50%，不重仓追高</span></div>"""
))

patches.append(('FIX3c 指南C分',
    """<div class="guide-grade-row"><span class="guide-grade grade-C">C 50-64分</span><span>观察期 — 等待信号更明确</span></div>""",
    """<div class="guide-grade-row"><span class="guide-grade grade-C">C 50-64分</span><span>观望等待 — 维持底仓10-20%，暂停新增</span></div>"""
))

patches.append(('FIX3d 指南D分',
    """<div class="guide-grade-row"><span class="guide-grade grade-D">D &lt;50分</span><span>不建议入场 — 赔率差或有极端风险信号</span></div>""",
    """<div class="guide-grade-row"><span class="guide-grade grade-D">D &lt;50分</span><span>清仓观望 — 减至底仓或空仓，等多维度改善</span></div>"""
))

# ================================================================
# FIX 5: 稳定币阈值 — fetch_data.py 里和前端 JS 都要改
# 前端 JS 里的 stablecoin 阈值不在 index.html（那是后端 fetch_data.py 的事），
# 但前端 calcScores 里 stbl 评分是 expand/flat/shrink 字符串，阈值在后端。
# 这里先改后端的，后面单独 patch fetch_data.py
# ================================================================

# ================================================================
# FIX 4: 布局重组 — 把加权逻辑说明从矿机后面移到评级框后面
# 策略：先删除原位置的 SCORING LOGIC 块，再在 verdict-box 后面插入
# ================================================================

# 提取 SCORING LOGIC 块
scoring_start = '  <!-- SCORING LOGIC -->'
scoring_end_marker = '  <!-- GUIDE -->'

if scoring_start in html and scoring_end_marker in html:
    idx_start = html.index(scoring_start)
    idx_end = html.index(scoring_end_marker)
    scoring_block = html[idx_start:idx_end]
    # 删除原位置
    html = html[:idx_start] + html[idx_end:]
    
    # 在一票否决检查之前插入（也就是 verdict-box 后面）
    veto_marker = '  <!-- ONE VETO CHECK -->'
    if veto_marker in html:
        html = html.replace(veto_marker, scoring_block + '\n' + veto_marker)
        print("✅ FIX4: 加权逻辑说明已移到评级框后面")
    else:
        print("❌ FIX4: 找不到 ONE VETO CHECK 锚点")
else:
    print(f"❌ FIX4: 找不到 SCORING LOGIC 块 (start={scoring_start in html}, end={scoring_end_marker in html})")

# ================================================================
# FIX 7: 把一票否决和监控移到四模块后面
# 当前顺序：verdict → veto → modules → manual → monitor
# 目标顺序：verdict → scoring_logic → modules → veto → manual → monitor
# 策略：把 veto-box 从当前位置提取出来，插到 modules-grid 后面
# ================================================================

veto_start = '  <!-- ONE VETO CHECK -->'
veto_end_search = '  <!-- MODULES -->'

if veto_start in html and veto_end_search in html:
    idx_vs = html.index(veto_start)
    idx_ve = html.index(veto_end_search)
    veto_block = html[idx_vs:idx_ve]
    # 删除原位置
    html = html[:idx_vs] + html[idx_ve:]
    
    # 插到 manual input 前面
    manual_marker = '  <!-- MANUAL INPUT -->'
    if manual_marker in html:
        html = html.replace(manual_marker, veto_block + '\n' + manual_marker)
        print("✅ FIX7: 一票否决移到四模块后面")
    else:
        print("❌ FIX7: 找不到 MANUAL INPUT 锚点")
else:
    print(f"❌ FIX7: 找不到 veto 块 (start={veto_start in html}, end={veto_end_search in html})")

# ================================================================
# FIX 2: 前端优先从 data.json 读价格
# 在 fetchDataJson 里加上价格读取，在 fetchCoingecko 降级
# ================================================================

# 2a: fetchDataJson 里读取 price
old_fetch_data_end = """    // TIPS 实际利率
    if (d.tips !== null && d.tips !== undefined) {
      state.tips = d.tips;
      const el = document.getElementById('inp_tips');
      if (el) el.value = state.tips;
    }

  } catch(e) {
    console.log('data.json not available yet:', e.message);
  }
}"""

new_fetch_data_end = """    // TIPS 实际利率
    if (d.tips !== null && d.tips !== undefined) {
      state.tips = d.tips;
      const el = document.getElementById('inp_tips');
      if (el) el.value = state.tips;
    }

    // 后端价格（优先使用，绕过 CoinGecko 前端限流）
    if (d.price && d.price.price) {
      state.price = d.price.price;
      if (d.price.change24h !== null) state.change24h = d.price.change24h;
      console.log('[data.json] 使用后端价格: $' + state.price);
    }

  } catch(e) {
    console.log('data.json not available yet:', e.message);
  }
}"""
patches.append(('FIX2a data.json读价格', old_fetch_data_end, new_fetch_data_end))

# 2b: fetchCoingecko 变为补充数据源（只补 data.json 没有的字段）
old_cg_assign = """    Object.assign(state, fresh);
    cacheSet('cg_btc_data', fresh);"""

new_cg_assign = """    // 只补充 data.json 没提供的字段（价格优先用后端）
    if (!state.price) state.price = fresh.price;
    if (!state.change24h) state.change24h = fresh.change24h;
    // 这些字段后端不提供，CG 独有
    if (fresh.athPct) state.athPct = fresh.athPct;
    if (fresh.p30d) state.p30d = fresh.p30d;
    if (fresh.dominance) state.dominance = fresh.dominance;
    if (fresh.ma200val) { state.ma200val = fresh.ma200val; state.ma200dev = fresh.ma200dev; }
    cacheSet('cg_btc_data', fresh);"""
patches.append(('FIX2b CG降级为补充', old_cg_assign, new_cg_assign))

# ================================================================
# FIX 6: OI 评分加注释说明
# ================================================================
old_oi_comment = '  // 未平仓合约 OI (3pts) — 杠杆程度判断'
new_oi_comment = '  // 未平仓合约 OI (3pts) — 杠杆程度判断（注：用绝对量近似，理想应用OI/市值比，后续迭代）'
patches.append(('FIX6 OI注释', old_oi_comment, new_oi_comment))

# Apply string replacement patches
for name, old, new in patches:
    if old in html:
        html = html.replace(old, new, 1)
        print(f"✅ {name}")
    else:
        print(f"❌ {name} — NOT FOUND")
        # Debug: show nearby context
        key = old[:60].strip()
        idx = html.find(key[:30])
        if idx >= 0:
            print(f"   Near match at {idx}: {repr(html[idx:idx+80])}")

with open(HTML_FILE, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"\n✅ index.html 写入完成 ({len(html)} bytes)")
