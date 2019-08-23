package common

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/Deansquirrel/goToolCommon"
	"github.com/Deansquirrel/goZ9MdDataTransV2/global"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	"io"
	"os"
)

import log "github.com/Deansquirrel/goToolLog"

const SysConfigFile = "config.toml"

//根据命令行参数更新系统参数
func UpdateParams() {

}

//加载系统配置文件
func LoadSysConfig() {
	path, err := goToolCommon.GetCurrPath()
	if err != nil {
		log.Error("获取程序所在路径失败")
		return
	}
	fileFullPath := path + "\\" + SysConfigFile
	b, err := goToolCommon.PathExists(fileFullPath)
	if err != nil {
		log.Error(fmt.Sprintf("检查配置文件是否存在时遇到错误：%s，Path：%s", err.Error(), fileFullPath))
		return
	}
	if b {
		configFile, err := os.Open(fileFullPath)
		defer func() {
			_ = configFile.Close()
		}()
		if err != nil {
			log.Error(fmt.Sprintf("打开配置文件时遇到错误：%s，Path：%s", err.Error(), fileFullPath))
			return
		}
		buf := make([]byte, 3)
		_, err = io.ReadFull(configFile, buf)
		if err != nil {
			log.Error(fmt.Sprintf("读取配置文件时遇到错误：%s，Path：%s", err.Error(), fileFullPath))
			return
		}
		if bytes.Equal(buf, []byte{0xEF, 0xBB, 0xBF}) == false {
			_, err = configFile.Seek(0, 0)
			if err != nil {
				log.Error(fmt.Sprintf("读取配置文件时遇到错误：%s，Path：%s", err.Error(), fileFullPath))
				return
			}
		}
		var c object.SystemConfig
		_, err = toml.DecodeReader(configFile, &c)
		if err != nil {
			log.Error(fmt.Sprintf("读取配置文件时遇到错误：%s，Path：%s", err.Error(), fileFullPath))
			return
		}

		global.SysConfig = &c

	} else {
		log.Warn(fmt.Sprintf("未找到配置文件 %s", fileFullPath))
	}
}

//根据配置文件刷新系统配置
func RefreshSysConfig() {
	global.SysConfig.FormatConfig()

	setLogLevel(global.SysConfig.Total.LogLevel)
	log.StdOut = global.SysConfig.Total.StdOut
}

//设置日志级别
func setLogLevel(logLevel string) {
	switch logLevel {
	case "debug":
		log.Level = log.LevelDebug
		return
	case "info":
		log.Level = log.LevelInfo
		return
	case "warn":
		log.Level = log.LevelWarn
		return
	case "error":
		log.Level = log.LevelError
		return
	default:
		log.Level = log.LevelWarn
	}
}
