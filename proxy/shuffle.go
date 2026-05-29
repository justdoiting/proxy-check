package proxies

import (
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2"
)

var asnDB *maxminddb.Reader

// SetASNDB 设置 ASN 数据库
func SetASNDB(db *maxminddb.Reader) {
	asnDB = db
}

type ShuffleConfig struct {
	Threshold  float64 // 相邻相似度阈值，IPv4 /24 ≈ 0.75
	Passes     int     // 改善轮数
	MinSpacing int     // 同一 IPv4 /24 的最小间距
	ScanLimit  int     // 冲突向前扫描的最大距离
}

type serverMeta struct {
	raw      string
	isIPv4   bool
	octets   [4]byte
	prefix24 uint32
	prefixOK bool
	asn      uint32
	asnOK    bool
}

// SmartShuffleByServer 对 items 就地打乱，避免相邻相似，并尽量满足最小间距
func SmartShuffleByServer(items []map[string]any, cfg ShuffleConfig) {
	n := len(items)
	if n < 2 {
		return
	}

	// ==================== 分桶交错编织（支持 ASN） ====================
	buckets := make(map[string][]map[string]any)

	for i := range items {
		var serverStr string
		if s, ok := items[i]["server"].(string); ok {
			serverStr = strings.TrimSpace(s)
		}

		key := serverStr

		if ip := net.ParseIP(serverStr); ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				key = fmt.Sprintf("v4-%d.%d.%d", ip4[0], ip4[1], ip4[2])
			} else {
				key = "v6-" + ip.Mask(net.CIDRMask(64, 128)).String()
			}

			// 解析 ASN 并加入 key（严格适配 v2 的 netip.Addr）
			if asnDB != nil {
				if addr, err := netip.ParseAddr(serverStr); err == nil {
					var record struct {
						ASN uint32 `maxminddb:"autonomous_system_number"`
					}
					// maxminddb/v2 正确解法：Lookup 仅返回 1 个 maxminddb.Result 对象
					res := asnDB.Lookup(addr)
					// 通过 Decode 传入指针解包，并在此处捕获和判断 error
					if err := res.Decode(&record); err == nil && record.ASN != 0 {
						key += fmt.Sprintf("|as%d", record.ASN)
					}
				}
			}
		} else if serverStr != "" {
			// 域名处理
			parts := strings.Split(serverStr, ".")
			if len(parts) > 2 {
				key = strings.Join(parts[len(parts)-2:], ".")
			}
		}

		buckets[key] = append(buckets[key], items[i])
	}

	// 桶内随机打乱
	for k := range buckets {
		rand.Shuffle(len(buckets[k]), func(i, j int) {
			buckets[k][i], buckets[k][j] = buckets[k][j], buckets[k][i]
		})
	}

	// 交错合并
	type bucketInfo struct {
		key   string
		nodes []map[string]any
	}
	var activeBuckets []bucketInfo
	for k, v := range buckets {
		activeBuckets = append(activeBuckets, bucketInfo{key: k, nodes: v})
	}

	interleavedResult := make([]map[string]any, 0, n)
	for len(activeBuckets) > 0 {
		var nextBuckets []bucketInfo
		for _, b := range activeBuckets {
			if len(b.nodes) > 0 {
				interleavedResult = append(interleavedResult, b.nodes[0])
				if len(b.nodes) > 1 {
					nextBuckets = append(nextBuckets, bucketInfo{key: b.key, nodes: b.nodes[1:]})
				}
			}
		}
		activeBuckets = nextBuckets
	}

	copy(items, interleavedResult)

	// ==================== 原有微调逻辑 ====================
	if cfg.Passes <= 0 {
		cfg.Passes = 2
	}
	if cfg.Threshold <= 0 {
		cfg.Threshold = 0.75
	}
	if cfg.ScanLimit <= 0 {
		cfg.ScanLimit = 64
	}

	// 预解析服务器元数据
	metas := make([]serverMeta, n)
	for i := range items {
		if s, ok := items[i]["server"].(string); ok {
			metas[i] = parseServerMeta(s)
		}
	}

	checkSpacing := func(lastPos map[uint32]int, idx int, m serverMeta) bool {
		if cfg.MinSpacing <= 0 || !m.prefixOK {
			return true
		}
		if last, ok := lastPos[m.prefix24]; !ok || idx-last > cfg.MinSpacing {
			return true
		}
		return false
	}

	for pass := 0; pass < cfg.Passes; pass++ {
		changed := false
		lastPos := make(map[uint32]int, 64)
		if len(metas) > 0 && metas[0].prefixOK {
			lastPos[metas[0].prefix24] = 0
		}

		for i := 0; i < n-1; i++ {
			m1, m2 := metas[i], metas[i+1]

			conflict := similarity(m1, m2) >= cfg.Threshold ||
				(cfg.MinSpacing > 0 && same24(m1, m2)) ||
				sameASN(m1, m2)

			if conflict {
				bestJ, bestScore := -1, 2.0
				searchEnd := i + 2 + cfg.ScanLimit
				if searchEnd > n {
					searchEnd = n
				}

				for j := i + 2; j < searchEnd; j++ {
					mj := metas[j]
					if !checkSpacing(lastPos, i+1, mj) {
						continue
					}
					score := similarity(m1, mj)
					if score < cfg.Threshold {
						swap(items, metas, i+1, j)
						m2 = mj
						changed = true
						break
					}
					if score < bestScore {
						bestScore, bestJ = score, j
					}
				}

				if !changed && bestJ != -1 {
					if checkSpacing(lastPos, i+1, metas[bestJ]) {
						swap(items, metas, i+1, bestJ)
						changed = true
						m2 = metas[i+1]
					}
				}
			}

			if m2.prefixOK {
				lastPos[m2.prefix24] = i + 1
			}
		}

		if !changed {
			break
		}
	}
}

func parseServerMeta(s string) serverMeta {
	m := serverMeta{raw: s}
	if ip := net.ParseIP(s); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			m.isIPv4 = true
			copy(m.octets[:], ip4)
			m.prefix24 = uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8
			m.prefixOK = true
		}

		// ASN 解析（严格适配 v2 的 netip.Addr）
		if asnDB != nil {
			if addr, err := netip.ParseAddr(s); err == nil {
				var record struct {
					ASN uint32 `maxminddb:"autonomous_system_number"`
				}
				// maxminddb/v2 正确解法：将唯一返回的 Result 赋值给变量，再解包
				res := asnDB.Lookup(addr)
				if err := res.Decode(&record); err == nil && record.ASN != 0 {
					m.asn = record.ASN
					m.asnOK = true
				}
			}
		}
	}
	return m
}

func same24(a, b serverMeta) bool {
	return a.prefixOK && b.prefixOK && a.prefix24 == b.prefix24
}

func sameASN(a, b serverMeta) bool {
	return a.asnOK && b.asnOK && a.asn == b.asn
}

func similarity(a, b serverMeta) float64 {
	if a.isIPv4 && b.isIPv4 {
		eq := 0
		for i := 0; i < 4; i++ {
			if a.octets[i] == b.octets[i] {
				eq++
			} else {
				break
			}
		}
		return float64(eq) / 4.0
	}

	na, nb := len(a.raw), len(b.raw)
	if na == 0 || nb == 0 {
		return 0
	}
	n := min(na, nb)
	i := 0
	for i < n && a.raw[i] == b.raw[i] {
		i++
	}
	maxLen := max(na, nb)
	return float64(i) / float64(maxLen)
}

func swap(items []map[string]any, metas []serverMeta, i, j int) {
	items[i], items[j] = items[j], items[i]
	metas[i], metas[j] = metas[j], metas[i]
}

func ThresholdToCIDR(th float64) string {
	switch th {
	case 1.0:
		return "/32"
	case 0.75:
		return "/24"
	case 0.5:
		return "/16"
	case 0.25:
		return "/8"
	default:
		prefix := int(th*4) * 8
		if prefix <= 0 {
			prefix = 24
		} else if prefix > 32 {
			prefix = 32
		}
		return fmt.Sprintf("/%d", prefix)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
