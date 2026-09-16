package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/sagernet/sing-box/experimental/v2rayapi"
	"github.com/sagernet/sing-box/log"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// sing-box 自带的 `api` 子命令走的是 clash_api(HTTP)，读不到按用户计的流量；而 v2ray_api 的
// StatsService 虽然是 gRPC，服务名却是 sing-box 自己的 experimental.v2rayapi.StatsService，
// v2ray/xray 的 CLI 都查不了(实测 xray api statsquery 报 unknown service)。所以补这一条命令，
// 省得为了读个数在节点上再装一个别的内核。
//
// 输出刻意对齐 `xray api statsquery` 的 JSON 形态({"stat":[{"name","value"}]})：上报侧
// (cron.py read_singbox_user_traffic) 的解析逻辑就能和 xray 那条共用一套。
var commandStats = &cobra.Command{
	Use:   "stats",
	Short: "Query traffic stats from the V2Ray API service",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := queryStats(); err != nil {
			log.Fatal(err)
		}
	},
}

var (
	commandStatsServer  string
	commandStatsPattern string
	commandStatsReset   bool
)

func init() {
	commandStats.Flags().StringVar(&commandStatsServer, "server", "127.0.0.1:8080", "V2Ray API service address")
	commandStats.Flags().StringVar(&commandStatsPattern, "pattern", "", "counter name pattern, empty for all")
	// reset 是读即清零：上报侧按增量累加，不 reset 会把同一段流量反复上报
	commandStats.Flags().BoolVar(&commandStatsReset, "reset", false, "reset counters after reading")
	mainCommand.AddCommand(commandStats)
}

func queryStats() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// passthrough:/// 绕开 grpc 默认的 dns resolver——同一个二进制里 sing-box 注册了自己的 DNS 实现，
	// 走 dns scheme 解析 127.0.0.1:port 会落到别处，表现为连上了却报 unknown service
	conn, err := grpc.NewClient("passthrough:///"+commandStatsServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	// 不能用 v2rayapi.NewStatsServiceClient：sing-box 在 experimental/v2rayapi/stats.go 的 init 里
	// 把 StatsService_ServiceDesc.ServiceName 改成了 v2ray 的标准名，好让 v2ray 生态的客户端能查，
	// 但生成的客户端常量(/experimental.v2rayapi.StatsService/...)没跟着改，自带的桩调不通自己的
	// 服务端(实测 unknown service)。按服务端实际注册的名字直接 Invoke。
	const queryStatsMethod = "/v2ray.core.app.stats.command.StatsService/QueryStats"
	response := new(v2rayapi.QueryStatsResponse)
	err = conn.Invoke(ctx, queryStatsMethod, &v2rayapi.QueryStatsRequest{
		Pattern: commandStatsPattern,
		Reset_:  commandStatsReset,
	}, response)
	if err != nil {
		return err
	}
	type stat struct {
		Name  string `json:"name"`
		Value int64  `json:"value"`
	}
	stats := make([]stat, 0, len(response.GetStat()))
	for _, it := range response.GetStat() {
		stats = append(stats, stat{Name: it.GetName(), Value: it.GetValue()})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false) // 计数器名里全是 '>'，转义成 \u003e 只会让日志没法看
	return encoder.Encode(map[string]any{"stat": stats})
}
