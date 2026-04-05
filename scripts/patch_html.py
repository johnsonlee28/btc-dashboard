#!/usr/bin/env python3
"""
patch_html.py — BTC Dashboard index.html 补丁脚本
新增指标：保证金借贷年化利率 + 未平仓合约(OI)
"""
import sys, os

HTML_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'index.html')

with open(HTML_FILE, 'r', encoding='utf-8') as f:
    html = f.read()

patches = []

# ---- PATCH 1: Module 2 HTML — 资金费率行后加两行新指标 ----
p1_old = '''        <div class="metric-row">
          <div class="metric-name">资金费率（手动）</div>
          <div class="metric-value" id="m2fr">--</div>
          <div class="metric-bar-wrap"><div class="metric-bar" id="m2fr_bar" style="width:0%"></div></div>
        </div>
      </div>
    </div>

    <!-- MODULE 3: ONCHAIN -->'''

p1_new = '''        <div class="metric-row">
          <div class="metric-name">资金费率（手动）</div>
          <div class="metric-value" id="m2fr">--</div>
          <div class="metric-bar-wrap"><div class="metric-bar" id="m2fr_bar" style="width:0%"></div></div>
        </div>
        <div class="metric-row">
          <div class="metric-name">借贷年化利率 <span style="font-size:10px;color:var(--muted)">●自动</span></div>
          <div class="metric-value loading" id="m2ml">--%</div>
          <div class="metric-bar-wrap"><div class="metric-bar" id="m2ml_bar" style="width:0%"></div></div>
        </div>
        <div class="metric-row">
          <div class="metric-name">未平仓合约(OI) <span style="font-size:10px;color:var(--muted)">●自动</span></div>
          <div class="metric-value loading" id="m2oi">-- BTC</div>
          <div class="metric-bar-wrap"><div class="metric-bar" id="m2oi_bar" style="width:0%"></div></div>
        </div>
      </div>
    </div>

    <!-- MODULE 3: ONCHAIN -->'''
patches.append(('PATCH1 Module2 HTML', p1_old, p1_new))

# ---- PATCH 2: Module 2 标题分值 25→30 ----
p2_old = '''      <div class="module-header">
        <h3>💰 资金流与市场结构</h3>
        <div class="module-score score-loading loading" id="score2">--/25</div>'''
p2_new = '''      <div class="module-header">
        <h3>💰 资金流与市场结构</h3>
        <div class="module-score score-loading loading" id="score2">--/30</div>'''
patches.append(('PATCH2 Module2 header 25→30', p2_old, p2_new))

# ---- PATCH 3: calcScores Module 2 加借贷+OI分数 ----
p3_old = (
    '  // 资金费率 (4pts) — 衍生品市场温度\n'
    '  let fr_score = 2;\n'
    '  if (state.fr !== null) {\n'
    '    if (state.fr < 0) fr_score = 4;              // 负费率=空头过多=反弹信号\n'
    '    else if (state.fr <= 0.01) fr_score = 4;     // 极低=冷静\n'
    '    else if (state.fr <= 0.03) fr_score = 3;     // 正常偏低\n'
    '    else if (state.fr <= 0.06) fr_score = 1;     // 偏高=多头拥挤\n'
    '    else fr_score = 0;                            // 极高=过热\n'
    '  }\n'
    '  s2 = Math.min(25, dom_score + etf_score + stbl_score + fr_score);'
)
p3_new = (
    '  // 资金费率 (4pts) — 衍生品市场温度\n'
    '  let fr_score = 2;\n'
    '  if (state.fr !== null) {\n'
    '    if (state.fr < 0) fr_score = 4;              // 负费率=空头过多=反弹信号\n'
    '    else if (state.fr <= 0.01) fr_score = 4;     // 极低=冷静\n'
    '    else if (state.fr <= 0.03) fr_score = 3;     // 正常偏低\n'
    '    else if (state.fr <= 0.06) fr_score = 1;     // 偏高=多头拥挤\n'
    '    else fr_score = 0;                            // 极高=过热\n'
    '  }\n'
    '\n'
    '  // 借贷年化利率 (3pts) — 机构借BTC做空成本，利率突升=做空压力预警\n'
    '  let ml_score = 1;\n'
    '  if (state.ml_rate !== null) {\n'
    '    if (state.ml_rate < 0.3) ml_score = 3;       // 极低=无明显做空需求\n'
    '    else if (state.ml_rate < 0.5) ml_score = 2;  // 正常偏低\n'
    '    else if (state.ml_rate < 0.8) ml_score = 1;  // 偏高=有借空需求\n'
    '    else ml_score = 0;                            // 高利率=大量借BTC做空预警\n'
    '  }\n'
    '\n'
    '  // 未平仓合约 OI (3pts) — 杠杆程度判断\n'
    '  let oi_score = 1;\n'
    '  if (state.oi_btc !== null) {\n'
    '    if (state.oi_btc < 60000) oi_score = 3;      // 低OI=低杠杆=健康\n'
    '    else if (state.oi_btc < 90000) oi_score = 2; // 中等OI=正常\n'
    '    else if (state.oi_btc < 120000) oi_score = 1; // 偏高OI=杠杆偏多\n'
    '    else oi_score = 0;                             // 极高OI=过度杠杆\n'
    '  }\n'
    '\n'
    '  s2 = Math.min(30, dom_score + etf_score + stbl_score + fr_score + ml_score + oi_score);'
)
patches.append(('PATCH3 calcScores Module2', p3_old, p3_new))

# ---- PATCH 4: 评级阈值调整（总满分105，阈值上调） ----
p4_old = (
    '  if (vetoTriggered) {\n'
    "    grade = 'D'; total = Math.min(total, 45);\n"
    "    title = '⛔ 一票否决 — 立即停止买入';\n"
    "    desc = '⚠️ 检测到极端风险信号（链上严重过热 / 主动加息周期 / NUPL狂热）。操作建议：停止新增仓位，已有仓位减至底仓20-30%，等待信号解除。';\n"
    "    verdictClass = 'verdict-D';\n"
    '  } else if (total >= 82) {\n'
    "    grade = 'A+'; title = '🚀 重仓机会 — 历史级低估';\n"
    "    desc = '四维指标高度共振，出现历史级买入机会（通常对应大熊市底部）。操作建议：可重仓3-5批建仓，总仓位可达计划上限的80-100%，每跌5%加一批。';\n"
    "    verdictClass = 'verdict-A';\n"
    '  } else if (total >= 72) {\n'
    "    grade = 'A'; title = '✅ 积极建仓 — 赔率良好';\n"
    "    desc = '宏观、资金、链上、赔率四维均较为友好，整体偏向低估。操作建议：分3-5批建仓，总仓位60-80%，不追单日大阳线，逢回调加仓。';\n"
    "    verdictClass = 'verdict-A';\n"
    '  } else if (total >= 60) {\n'
    "    grade = 'B'; title = '🟡 轻仓参与 — 定投为主';\n"
    "    desc = '信号偏正面但存在不确定性，市场处于中性偏多区间。操作建议：定投为主，仓位控制在30-50%，不追高，等待更好的入场点。';\n"
    "    verdictClass = 'verdict-B';\n"
    '  } else if (total >= 45) {\n'
    "    grade = 'C'; title = '⏸ 观望等待 — 赔率不足';\n"
    "    desc = '当前信号混杂，估值偏贵或宏观不明朗。操作建议：维持底仓（10-20%），暂停新增，等待MVRV/NUPL回落或宏观改善再行动。';\n"
    "    verdictClass = 'verdict-C';\n"
    '  } else {\n'
    "    grade = 'D'; title = '❌ 清仓观望 — 风险大于机会';\n"
    "    desc = '宏观逆风 + 估值偏贵 + 资金面弱，多重不利信号叠加。操作建议：减仓至底仓或空仓，等待至少2-3个维度出现明显改善后再重新评估。';\n"
    "    verdictClass = 'verdict-D';\n"
    '  }\n'
    '\n'
    '  return { s1, s2, s3, s4, total, grade, title, desc, verdictClass,\n'
    '           vetoTriggered, monthsSinceHalving,\n'
    '           fg_score, fed_score, dxy_score, tips_score,\n'
    '           dom_score, etf_score, stbl_score, fr_score,\n'
    '           mvrv_score, nupl_score, puell_score, lth_score, lthmvrv_score,\n'
    '           ath_score, ma200_score, halving_score, p30_score };'
)
p4_new = (
    '  if (vetoTriggered) {\n'
    "    grade = 'D'; total = Math.min(total, 50);\n"
    "    title = '⛔ 一票否决 — 立即停止买入';\n"
    "    desc = '⚠️ 检测到极端风险信号（链上严重过热 / 主动加息周期 / NUPL狂热）。操作建议：停止新增仓位，已有仓位减至底仓20-30%，等待信号解除。';\n"
    "    verdictClass = 'verdict-D';\n"
    '  } else if (total >= 90) {\n'
    "    grade = 'A+'; title = '🚀 重仓机会 — 历史级低估';\n"
    "    desc = '五维指标高度共振（含借贷结构健康+低杠杆），出现历史级买入机会。操作建议：可重仓3-5批建仓，总仓位可达计划上限的80-100%，每跌5%加一批。';\n"
    "    verdictClass = 'verdict-A';\n"
    '  } else if (total >= 78) {\n'
    "    grade = 'A'; title = '✅ 积极建仓 — 赔率良好';\n"
    "    desc = '宏观、资金、借贷结构、链上、赔率多维均较友好，整体偏向低估。操作建议：分3-5批建仓，总仓位60-80%，不追单日大阳线，逢回调加仓。';\n"
    "    verdictClass = 'verdict-A';\n"
    '  } else if (total >= 65) {\n'
    "    grade = 'B'; title = '🟡 轻仓参与 — 定投为主';\n"
    "    desc = '信号偏正面但存在不确定性，市场处于中性偏多区间。操作建议：定投为主，仓位控制在30-50%，不追高，等待更好的入场点。';\n"
    "    verdictClass = 'verdict-B';\n"
    '  } else if (total >= 50) {\n'
    "    grade = 'C'; title = '⏸ 观望等待 — 赔率不足';\n"
    "    desc = '当前信号混杂，估值偏贵或宏观不明朗。操作建议：维持底仓（10-20%），暂停新增，等待MVRV/NUPL回落或宏观改善再行动。';\n"
    "    verdictClass = 'verdict-C';\n"
    '  } else {\n'
    "    grade = 'D'; title = '❌ 清仓观望 — 风险大于机会';\n"
    "    desc = '宏观逆风 + 估值偏贵 + 资金面弱 + 借贷结构异常，多重不利信号叠加。操作建议：减仓至底仓或空仓，等待至少2-3个维度出现明显改善后再重新评估。';\n"
    "    verdictClass = 'verdict-D';\n"
    '  }\n'
    '\n'
    '  return { s1, s2, s3, s4, total, grade, title, desc, verdictClass,\n'
    '           vetoTriggered, monthsSinceHalving,\n'
    '           fg_score, fed_score, dxy_score, tips_score,\n'
    '           dom_score, etf_score, stbl_score, fr_score, ml_score, oi_score,\n'
    '           mvrv_score, nupl_score, puell_score, lth_score, lthmvrv_score,\n'
    '           ath_score, ma200_score, halving_score, p30_score };'
)
patches.append(('PATCH4 grade thresholds', p4_old, p4_new))

# ---- PATCH 5a: render score2 label 25→30 ----
p5a_old = "  setText('score2', sc.s2+'/25'); setClass('score2', 'module-score '+scoreColor(sc.s2,25));"
p5a_new = "  setText('score2', sc.s2+'/30'); setClass('score2', 'module-score '+scoreColor(sc.s2,30));"
patches.append(('PATCH5a render score2', p5a_old, p5a_new))

# ---- PATCH 5b: render — m2fr 后加 m2ml/m2oi ----
p5b_old = (
    "  setText('m2fr', state.fr!==null? state.fr+'%/8h ('+sc.fr_score+'/5)' : '--');\n"
    "  setBar('m2fr_bar', barWidth(sc.fr_score,5), barColor(sc.fr_score,5));"
)
p5b_new = (
    "  setText('m2fr', state.fr!==null? state.fr+'%/8h ('+sc.fr_score+'/4)' : '--');\n"
    "  setBar('m2fr_bar', barWidth(sc.fr_score,4), barColor(sc.fr_score,4));\n"
    "  if(state.ml_rate!==null){\n"
    "    setText('m2ml', state.ml_rate.toFixed(3)+'%/年 ('+sc.ml_score+'/3)');\n"
    "    setBar('m2ml_bar', barWidth(sc.ml_score,3), barColor(sc.ml_score,3));\n"
    "    document.getElementById('m2ml').classList.remove('loading');\n"
    "  }\n"
    "  if(state.oi_btc!==null){\n"
    "    const oiK = (state.oi_btc/1000).toFixed(1);\n"
    "    setText('m2oi', oiK+'K BTC ('+sc.oi_score+'/3)');\n"
    "    setBar('m2oi_bar', barWidth(sc.oi_score,3), barColor(sc.oi_score,3));\n"
    "    document.getElementById('m2oi').classList.remove('loading');\n"
    "  }"
)
patches.append(('PATCH5b render m2ml/m2oi', p5b_old, p5b_new))

# ---- PATCH 6: fetchDataJson — 读取 margin_lending 字段 ----
p6_old = (
    "    // 资金费率\n"
    "    if (d.funding_rate !== null && d.funding_rate !== undefined) {\n"
    "      state.fr = d.funding_rate;\n"
    "      const el = document.getElementById('inp_fr');\n"
    "      if (el) el.value = state.fr;\n"
    "    }"
)
p6_new = (
    "    // 资金费率\n"
    "    if (d.funding_rate !== null && d.funding_rate !== undefined) {\n"
    "      state.fr = d.funding_rate;\n"
    "      const el = document.getElementById('inp_fr');\n"
    "      if (el) el.value = state.fr;\n"
    "    }\n"
    "\n"
    "    // 借贷利率 + 未平仓合约\n"
    "    if (d.margin_lending) {\n"
    "      if (d.margin_lending.btc_annual_rate !== null && d.margin_lending.btc_annual_rate !== undefined)\n"
    "        state.ml_rate = d.margin_lending.btc_annual_rate;\n"
    "      if (d.margin_lending.open_interest !== null && d.margin_lending.open_interest !== undefined)\n"
    "        state.oi_btc = d.margin_lending.open_interest;\n"
    "    }"
)
patches.append(('PATCH6 fetchDataJson margin_lending', p6_old, p6_new))

# ---- PATCH 7: state 初始化加 ml_rate / oi_btc ----
p7_old = "    etf5d: null, stbl: null, fr: null,\n    mvrv: null"
p7_new = "    etf5d: null, stbl: null, fr: null,\n    ml_rate: null, oi_btc: null,\n    mvrv: null"
patches.append(('PATCH7 state init', p7_old, p7_new))

# Apply all patches
results = []
for name, old, new in patches:
    if old in html:
        html = html.replace(old, new, 1)
        results.append(f"✅ {name}")
    else:
        results.append(f"❌ {name} — NOT FOUND")

for r in results:
    print(r)

with open(HTML_FILE, 'w', encoding='utf-8') as f:
    f.write(html)

print("\n✅ 写入完成")
