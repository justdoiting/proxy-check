// Package assets 嵌入的node、sub-store、MaxMind数据库等资产
package assets

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"encoding/json"
	"github.com/klauspost/compress/zstd"
	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/sinspired/subs-check/config"
	"github.com/sinspired/subs-check/save/method"
	"github.com/sinspired/subs-check/utils"
)

type githubRelease struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

// OpenMaxMindDB 使用指定路径或默认路径打开 MaxMind 数据库（国家库）
func OpenMaxMindDB(dbPath string) (*maxminddb.Reader, error) {
	if dbPath != "" {
		return openDBWithArch(dbPath)
	}
	mmdbPath, err := resolveDBPath("GeoLite2-Country.mmdb")
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(mmdbPath); os.IsNotExist(err) {
		if err := decompressEmbeddedMMDB(mmdbPath); err != nil {
			return nil, err
		}
	}
	return openDBWithArch(mmdbPath)
}

// OpenASNDB 打开 ASN 数据库（若不存在则自动在线下载）
func OpenASNDB(customPath string) (*maxminddb.Reader, error) {
	if customPath != "" {
		return openDBWithArch(customPath)
	}

	// 默认路径
	mmdbPath, err := resolveDBPath("GeoLite2-ASN.mmdb")
	if err != nil {
		return nil, err
	}

	// 如果不存在，直接触发自动下载
	if _, err := os.Stat(mmdbPath); os.IsNotExist(err) {
		slog.Warn("GeoLite2-ASN.mmdb 不存在，正在为您自动从云端下载...", "path", mmdbPath)
		if err := UpdateGeoLite2DB(); err != nil {
			return nil, fmt.Errorf("自动下载 ASN 数据库失败: %w", err)
		}
	}

	return openDBWithArch(mmdbPath)
}

func openDBWithArch(path string) (*maxminddb.Reader, error) {
	if runtime.GOARCH == "386" {
		return openFromBytes(path)
	}
	db, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("maxmind数据库打开失败: %w", err)
	}
	return db, nil
}

func decompressEmbeddedMMDB(targetPath string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("创建数据库目录失败: %w", err)
	}
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("zstd解码器创建失败: %w", err)
	}
	defer zstdDecoder.Close()

	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("maxmind数据库文件创建失败: %w", err)
	}
	defer file.Close()

	zstdDecoder.Reset(bytes.NewReader(EmbeddedMaxMindDB))
	if _, err := io.Copy(file, zstdDecoder); err != nil {
		return fmt.Errorf("maxmind数据库文件解压失败: %w", err)
	}
	return nil
}

func resolveDBPath(filename string) (string, error) {
	saver, err := method.NewLocalSaver()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(saver.OutputPath) {
		saver.OutputPath = filepath.Join(saver.BasePath, saver.OutputPath)
	}
	if err := os.MkdirAll(saver.OutputPath, 0755); err != nil {
		cwd, _ := os.Getwd()
		saver.OutputPath = filepath.Join(cwd, "output")
		os.MkdirAll(saver.OutputPath, 0755)
	}

	maxminddbDir := filepath.Join(saver.OutputPath, "MaxMindData")
	if err := os.MkdirAll(maxminddbDir, 0755); err != nil {
		return "", fmt.Errorf("无法创建 MaxMind 输出目录: %w", err)
	}
	return filepath.Join(maxminddbDir, filename), nil
}

func openFromBytes(path string) (*maxminddb.Reader, error) {
	runtime.GC()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取文件到内存失败: %w", err)
	}
	reader, err := maxminddb.OpenBytes(data)
	if err != nil {
		return nil, fmt.Errorf("从字节数组创建reader失败: %w", err)
	}
	return reader, nil
}

// UpdateGeoLite2DB 同时检查并更新 Country 库和 ASN 数据库
func UpdateGeoLite2DB() error {
	countryPath, err := resolveDBPath("GeoLite2-Country.mmdb")
	if err != nil {
		return fmt.Errorf("解析国家库路径失败: %w", err)
	}
	asnPath, err := resolveDBPath("GeoLite2-ASN.mmdb")
	if err != nil {
		return fmt.Errorf("解析ASN库路径失败: %w", err)
	}

	apiURL := "https://api.github.com/repos/mojolabs-id/GeoLite2-Database/releases/latest"

	resp, err := http.Get(apiURL)
	if err != nil {
		return fmt.Errorf("获取 release 信息失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GitHub API 状态码: %d", resp.StatusCode)
	}

	var rel githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return fmt.Errorf("解析 release JSON 失败: %w", err)
	}

	var countryURL, asnURL string
	isGhProxy := utils.GetGhProxy()
	for _, asset := range rel.Assets {
		if asset.Name == "GeoLite2-Country.mmdb" {
			countryURL = asset.BrowserDownloadURL
			if isGhProxy {
				countryURL = config.GlobalConfig.GithubProxy + asset.BrowserDownloadURL
			}
		}
		if asset.Name == "GeoLite2-ASN.mmdb" {
			asnURL = asset.BrowserDownloadURL
			if isGhProxy {
				asnURL = config.GlobalConfig.GithubProxy + asset.BrowserDownloadURL
			}
		}
	}

	// 1. 处理国家库更新
	if countryURL != "" {
		if err := safeDownload(countryURL, countryPath); err != nil {
			slog.Error("GeoLite2-Country.mmdb 下载失败", "err", err)
		} else {
			slog.Info("GeoLite2-Country.mmdb 更新完成")
		}
	}

	// 2. 处理 ASN 库更新
	if asnURL != "" {
		if err := safeDownload(asnURL, asnPath); err != nil {
			slog.Error("GeoLite2-ASN.mmdb 下载失败", "err", err)
			return fmt.Errorf("ASN 库下载失败: %w", err)
		} else {
			slog.Info("GeoLite2-ASN.mmdb 更新完成")
		}
	} else {
		return errors.New("云端未找到 GeoLite2-ASN.mmdb 下载地址")
	}

	version := rel.TagName
	utils.SendNotifyGeoDBUpdate(version)
	return nil
}

// 带备份和重试的安全下载辅助函数
func safeDownload(url, path string) error {
	bakPath := path + ".bak"
	if _, err := os.Stat(path); err == nil {
		if err := os.Rename(path, bakPath); err != nil {
			return fmt.Errorf("备份原文件失败: %w", err)
		}
	}

	success := false
	for i := range 3 {
		if err := downloadFile(url, path); err != nil {
			fmt.Printf("下载失败 (%d/3): %v\n", i+1, err)
			time.Sleep(1 * time.Second)
			continue
		}
		success = true
		break
	}

	if !success {
		if _, err := os.Stat(bakPath); err == nil {
			_ = os.Rename(bakPath, path)
		}
		return errors.New("多次尝试下载均失败，已回退原文件")
	}

	_ = os.Remove(bakPath)
	return nil
}

func downloadFile(url, path string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP 状态码 %d", resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
