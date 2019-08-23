package worker

import (
	"errors"
	"fmt"
	"github.com/Deansquirrel/goServiceSupportHelper"
	"github.com/Deansquirrel/goToolCron"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goToolSVRV3"
	"github.com/Deansquirrel/goToolSVRZ5"
	"github.com/Deansquirrel/goZ9MdDataTransV2/global"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	"github.com/Deansquirrel/goZ9MdDataTransV2/repository"
	"time"
)

import log "github.com/Deansquirrel/goToolLog"

type common struct {
}

func NewCommon() *common {
	return &common{}
}

//系统配置检查
func (c *common) checkSysConfig() bool {
	if global.SysConfig.OnlineConfig.Address == "" {
		log.Error("线上配置地址不能为空")
		global.Cancel()
		return false
	}
	err := c.refreshLocalDbConfig()
	if err != nil {
		return false
	}
	return true
}

func (c *common) refreshLocalDbConfig() error {
	var dbConfig *goToolMSSql.MSSqlConfig
	var err error
	switch global.SysConfig.RunMode.Mode {
	case string(object.RunModeMdCollect):
		dbConfig, err = goToolSVRZ5.GetSQLConfig(global.SysConfig.SvrConfig.Address, goToolSVRZ5.MD)
		if err != nil {
			errMsg := fmt.Sprintf("get dbConfig from svr z5 md err: %s", err.Error())
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if dbConfig == nil {
			errMsg := fmt.Sprintf("get dbConfig from svr v5 return nil")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		accList, err := goToolSVRZ5.GetAccountList(goToolMSSqlHelper.ConvertDbConfigTo2000(dbConfig), goToolSVRZ5.MD)
		if err != nil {
			errMsg := fmt.Sprintf("get acc list from svr z5 md err: %s", err.Error())
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if accList == nil {
			errMsg := fmt.Sprintf("get acc list from svr z5 md err: return nil")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if len(accList) < 1 {
			errMsg := fmt.Sprintf("get acc list from svr z5 md err: list is empty")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if global.SysConfig.LocalDb.DbName != "" {
			flag := false
			for _, acc := range accList {
				if acc == global.SysConfig.LocalDb.DbName {
					flag = true
					break
				}
			}
			if !flag {
				log.Warn(fmt.Sprintf("db [%s] is not a effective acc", global.SysConfig.LocalDb.DbName))
				global.SysConfig.LocalDb.DbName = ""
			}
		}
		if global.SysConfig.LocalDb.DbName == "" {
			global.SysConfig.LocalDb.DbName = accList[0]
		}
	case string(object.RunModeBbRestore):
		dbConfig, err = goToolSVRV3.GetSQLConfig(global.SysConfig.SvrConfig.Address, 7091, "91", "9101")
		if err != nil {
			errMsg := fmt.Sprintf("get dbConfig from svr v3 err: %s", err.Error())
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if dbConfig == nil {
			errMsg := fmt.Sprintf("get dbConfig from svr v3 return nil")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		accList, err := goToolSVRV3.GetAccountList(goToolMSSqlHelper.ConvertDbConfigTo2000(dbConfig), "91")
		if err != nil {
			errMsg := fmt.Sprintf("get acc list err: %s", err.Error())
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if accList == nil {
			errMsg := fmt.Sprintf("get acc list from svr v3 err: return nil")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if len(accList) < 1 {
			errMsg := fmt.Sprintf("get acc list from svr v3 err: list is empty")
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		if global.SysConfig.LocalDb.DbName != "" {
			flag := false
			for _, acc := range accList {
				if acc == global.SysConfig.LocalDb.DbName {
					flag = true
					break
				}
			}
			if !flag {
				log.Warn(fmt.Sprintf("db [%s] is not a effective acc", global.SysConfig.LocalDb.DbName))
				global.SysConfig.LocalDb.DbName = ""
			}
		}
		if global.SysConfig.LocalDb.DbName == "" {
			global.SysConfig.LocalDb.DbName = accList[0]
		}
	default:
		errMsg := fmt.Sprintf("unexpected runmode %s", global.SysConfig.RunMode.Mode)
		log.Error(errMsg)
		global.Cancel()
		return errors.New(errMsg)
	}

	global.SysConfig.LocalDb.Server = dbConfig.Server
	global.SysConfig.LocalDb.Port = dbConfig.Port
	global.SysConfig.LocalDb.User = dbConfig.User
	global.SysConfig.LocalDb.Pwd = dbConfig.Pwd

	if global.SysConfig.LocalDb.DbName == "" {
		errMsg := fmt.Sprintf("无可用账套")
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (c *common) StartService() {
	log.Debug(fmt.Sprintf("RunMode %s", global.SysConfig.RunMode.Mode))
	for {
		r := c.checkSysConfig()
		if r {
			break
		} else {
			time.Sleep(time.Minute * 30)
		}
	}

	switch global.SysConfig.RunMode.Mode {
	case string(object.RunModeMdCollect):
		go func() {
			goServiceSupportHelper.SetOtherInfo(
				repository.NewCommon().GetLocalDbConfig(),
				1,
				goServiceSupportHelper.SVRZ5)
		}()
		c.addMdWorker()
	case string(object.RunModeBbRestore):
		go func() {
			goServiceSupportHelper.SetOtherInfo(
				repository.NewCommon().GetLocalDbConfig(),
				1,
				goServiceSupportHelper.SVRV3)
		}()
		c.addBbWorker()
	default:
		log.Warn(fmt.Sprintf("unknown runmode %s", global.SysConfig.RunMode.Mode))
		global.Cancel()
	}
}

func (c *common) panicHandle(v interface{}) {
	log.Error(fmt.Sprintf("panicHandle: %s", v))
}

func (c *common) addWorker(key string, cmd func(id string), cron string) {
	rJob := c.formatJob(key, cmd)
	err := goToolCron.AddFunc(
		key,
		cron,
		goServiceSupportHelper.NewJob().FormatSSJob(key, rJob),
		c.panicHandle)
	if err != nil {
		errMsg := fmt.Sprintf("add job [%s] error: %s", key, err.Error())
		log.Error(errMsg)
	}
}

func (c *common) formatJob(key string, cmd func(id string)) func(id string) {
	return func(id string) {
		log.Debug(fmt.Sprintf("%s %s Start", key, id))
		defer log.Debug(fmt.Sprintf("%s %s Complete", key, id))
		cmd(id)
	}
}

func (c *common) addMdWorker() {
	log.Debug("add md worker")
	worker := NewMdWorker()
	c.addWorker("UpdateMdYyInfo", worker.UpdateMdYyInfo, global.SysConfig.Task.UpdateMdYyInfoCron)
	c.addWorker("UpdateZxKc", worker.UpdateZxKc, global.SysConfig.Task.UpdateZxKcCron)
	c.addWorker("UpdateMdHpXsSlHz", worker.UpdateMdHpXsSlHz, global.SysConfig.Task.UpdateMdHpXsSlHzCron)
}

func (c *common) addBbWorker() {
	log.Debug("add bb worker")
	worker := NewBbWorker()
	c.addWorker("RestoreMdYyInfo", worker.RestoreMdYyInfo, global.SysConfig.Task.BbRestoreCron)
	c.addWorker("RestoreZxKc", worker.RestoreZxKc, global.SysConfig.Task.BbRestoreCron)
	c.addWorker("RestoreMdHpXsSlHz", worker.RestoreMdHpXsSlHz, global.SysConfig.Task.BbRestoreCron)
}
