import json

SURFACE   = "#fcfcfb"
INK       = "#0b0b0b"
INK2      = "#52514e"
INK3      = "#8a8983"
GRID      = "#e8e7e3"
BLUE      = "#2a78d6"   # categorical slot 1
ORANGE    = "#eb6834"   # categorical slot 2
CRITICAL  = "#d03b3b"   # status: critical
FONT = "'Apple SD Gothic Neo','AppleGothic','Noto Sans KR',-apple-system,sans-serif"

def esc(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def txt(x, y, s, size=13, fill=INK2, weight=400, anchor="start"):
    return (f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
            f'fill="{fill}" font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')

def panel(x0, y0, w, h, values, ymax, color, ylabels, xevery, xoffset=1,
          highlight=None, label_point=None, kind="line"):
    """하나의 패널(격자 + 축 + 마크)을 그린다."""
    out = []
    n = len(values)
    def px(i): return x0 + (w * i / (n - 1)) if n > 1 else x0
    def pxb(i): return x0 + w * (i + 0.5) / n
    def py(v): return y0 + h - (h * v / ymax)

    # 격자: 실선 1px, 표면에서 한 단계
    for gv in ylabels:
        y = py(gv)
        out.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x0+w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(txt(x0 - 12, y + 4, f"{gv:,}", 12, INK3, anchor="end"))
    # x축
    out.append(f'<line x1="{x0}" y1="{y0+h}" x2="{x0+w}" y2="{y0+h}" stroke="{GRID}" stroke-width="1"/>')
    for i in range(n):
        label = i + xoffset
        if label % xevery == 0 or i == 0:
            xx = pxb(i) if kind == "bar" else px(i)
            out.append(txt(xx, y0 + h + 20, str(label), 12, INK3, anchor="middle"))

    if kind == "line":
        pts = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(values))
        out.append(f'<polyline points="{pts}" fill="none" stroke="{color}" '
                   f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
    else:
        bw = min(18, w / n - 2)   # 인접 막대 사이 2px 표면 간격
        for i, v in enumerate(values):
            if v <= 0: continue
            bh = h * v / ymax
            out.append(f'<rect x="{pxb(i)-bw/2:.1f}" y="{py(v):.1f}" width="{bw:.1f}" '
                       f'height="{bh:.1f}" rx="4" fill="{color}"/>')

    # 강조 지점: 8px 마커 + 2px 표면 링
    for i in (highlight or []):
        cx, cy = (pxb(i) if kind == "bar" else px(i)), py(values[i])
        out.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="5" fill="{CRITICAL}" '
                   f'stroke="{SURFACE}" stroke-width="2"/>')
    # 선택적 직접 레이블 (하나만)
    if label_point is not None:
        i, text, dy, anchor = label_point
        cx = (pxb(i) if kind == "bar" else px(i))
        dx = -8 if anchor == "end" else (8 if anchor == "start" else 0)
        out.append(txt(cx + dx, py(values[i]) + dy, text, 12, INK, 600, anchor))
    return "\n".join(out)

def page(title, subtitle, body, footnote, source, width, height):
    return f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<title>{esc(title)}</title>
<style>html,body{{margin:0;padding:0;background:{SURFACE};}}</style></head>
<body><svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
<rect width="{width}" height="{height}" fill="{SURFACE}"/>
{txt(48, 46, title, 22, INK, 700)}
{txt(48, 72, subtitle, 13, INK2)}
{body}
{txt(48, height-46, footnote, 12, INK3)}
{txt(48, height-24, source, 11, INK3)}
</svg></body></html>"""

# ---------------------------------------------------------------- 차트 2
e6 = json.load(open('/tmp/exp6.json'))
kf = e6['kafka']['msgPerSecByChunk']
rb = e6['rabbitmq']['msgPerSecByChunk']
stalls = [8, 15, 30, 38, 45, 52]          # 0-based (구간 9,16,31,39,46,53)

W, H = 1200, 700
X0, PW = 130, 1010
body = []
body.append(txt(48, 116, "Kafka", 15, INK, 700))
body.append(txt(112, 116, "중앙값 384,615 msg/s · 급락 0회", 13, INK2))
body.append(panel(X0, 132, PW, 170, kf, 450000, BLUE,
                  [0, 150000, 300000, 450000], 10))
body.append(txt(48, 392, "RabbitMQ", 15, INK, 700))
body.append(txt(142, 392, "중앙값 72,464 msg/s · 급락 6회 · 최저 894 msg/s", 13, INK2))
body.append(panel(X0, 408, PW, 170, rb, 120000, ORANGE,
                  [0, 40000, 80000, 120000], 10,
                  highlight=stalls))
body.append(txt(X0 + PW/2, 626, "발행 구간 (1만 건 단위)", 12, INK3, anchor="middle"))

open('docs/images/exp6-backlog.html','w').write(page(
    "적체가 쌓일 때 발행 처리량이 유지되는가",
    "컨슈머 없이 1만 건씩 60회, 총 60만 건 발행 · 512바이트 메시지 · 내구성 우선 설정 (Kafka acks=all · RabbitMQ persistent + confirms)",
    "\n".join(body),
    "급락 = 해당 브로커 중앙값의 1/5 미만인 구간.  RabbitMQ 는 메모리 임계치 300MB 에서 memory alarm 발생, 브로커가 보고한 차단 사유 \"low on memory\".  두 패널의 y축 눈금이 다르다.",
    "측정 2026-09-20 · Kafka 3.9.0 (KRaft) / RabbitMQ 3.13.7 · 단일 노드 로컬 · github.com/qjatn4793/rabbitmq-vs-kafka-lab",
    W, H))

# ---------------------------------------------------------------- 차트 3
k7 = json.load(open('/tmp/exp7-kafka.json'))
r7 = json.load(open('/tmp/exp7-rabbit.json'))
klat = [x['maxLatencyMs'] for x in k7['timeline']]
rfail = [x['failed'] for x in r7['timeline']]

W3, H3 = 1200, 700
body = []
body.append(txt(48, 116, "Kafka", 15, INK, 700))
body.append(txt(112, 116, "실패 0건 — 대신 지연이 튄다 (초당 최대 지연, ms)", 13, INK2))
body.append(panel(X0, 132, PW, 170, klat, 1600, BLUE,
                  [0, 800, 1600], 5, xoffset=0,
                  highlight=[10],
                  label_point=(10, "1,484ms", 4, "end")))
body.append(txt(48, 392, "RabbitMQ", 15, INK, 700))
body.append(txt(142, 392, "지연은 낮지만 예외가 올라온다 (초당 발행 실패, 건)", 13, INK2))
body.append(panel(X0, 408, PW, 170, rfail, 40, ORANGE,
                  [0, 20, 40], 5, xoffset=0, kind="bar",
                  label_point=(10, "37건", -12, "middle")))
# 사건 표시선
for sec, lab in ((10, "리더 정지"), (25, "복구")):
    for (yy, hh) in ((132, 170), (408, 170)):
        x = X0 + PW * sec / (len(klat) - 1)
        body.append(f'<line x1="{x:.1f}" y1="{yy}" x2="{x:.1f}" y2="{yy+hh}" stroke="{INK3}" stroke-width="1"/>')
    body.append(txt(X0 + PW * sec / (len(klat) - 1) + 7, 150, lab, 12, INK2, 600, "start"))
body.append(txt(X0 + PW/2, 626, "경과 시간 (초)", 12, INK3, anchor="middle"))

open('docs/images/exp7-failover.html','w').write(page(
    "리더 브로커를 강제로 죽이면 무슨 일이 일어나는가",
    "3노드 클러스터 · 초당 약 40건 발행 중 10초에 리더 노드 docker stop, 25초에 복구 · 양쪽 모두 유실 0건, 발행 중단 0초",
    "\n".join(body),
    "두 패널은 서로 다른 지표다. 같은 장애가 Kafka 에서는 지연으로, RabbitMQ 에서는 예외(AlreadyClosedException)로 나타났기 때문이다.  0초 지점의 390ms 는 JVM 워밍업.",
    "측정 2026-09-20 · Kafka RF=3 / min.insync.replicas=2 · RabbitMQ quorum queue 3노드 · 클라이언트는 양쪽 모두 리더 노드에 접속",
    W3, H3))
print("생성 완료")
