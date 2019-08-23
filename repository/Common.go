package repository

import (
	"errors"
	"fmt"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goToolSecret"
	"github.com/Deansquirrel/goZ9MdDataTransV2/global"
)

import log "github.com/Deansquirrel/goToolLog"

type common struct {
}

func NewCommon() *common {
	return &common{}
}

//获取在线支撑库连接配置
func (c *common) GetOnLineDbConfig() (*goToolMSSql.MSSqlConfig, error) {
	configStr, err := goToolSecret.DecryptFromBase64Format(global.SysConfig.OnlineConfig.Address, global.SecretKey)
	if err != nil {
		errMsg := fmt.Sprintf("get online address str err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return goToolMSSqlHelper.GetDBConfigByStr(configStr)
}

//获取本地库连接配置
func (c *common) GetLocalDbConfig() *goToolMSSql.MSSqlConfig {
	return &goToolMSSql.MSSqlConfig{
		Server: global.SysConfig.LocalDb.Server,
		Port:   global.SysConfig.LocalDb.Port,
		DbName: global.SysConfig.LocalDb.DbName,
		User:   global.SysConfig.LocalDb.User,
		Pwd:    global.SysConfig.LocalDb.Pwd,
	}
}
