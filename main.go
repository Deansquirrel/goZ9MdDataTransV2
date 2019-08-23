package main

import (
	"context"
	"fmt"
	"github.com/Deansquirrel/goZ9MdDataTransV2/common"
	"github.com/Deansquirrel/goZ9MdDataTransV2/global"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	myService "github.com/Deansquirrel/goZ9MdDataTransV2/service"
	"github.com/kardianos/service"
	"os"
	"runtime/debug"
	"time"
)

import log "github.com/Deansquirrel/goToolLog"

//初始化
func init() {
	global.Args = &object.ProgramArgs{}
	global.SysConfig = &object.SystemConfig{}

	global.Ctx, global.Cancel = context.WithCancel(context.Background())
}

func main() {
	fmt.Println(global.Version)
	log.Info(global.Version)

	//解析命令行参数
	{
		global.Args.Definition()
		global.Args.Parse()
		err := global.Args.Check()
		if err != nil {
			fmt.Print(err.Error())
			log.Error(err.Error())
			return
		}
		common.UpdateParams()
	}
	//加载系统参数
	{
		common.LoadSysConfig()
		common.RefreshSysConfig()
	}
	//安装、卸载或运行程序
	{
		svcConfig := &service.Config{
			Name:        global.SysConfig.Service.Name,
			DisplayName: global.SysConfig.Service.DisplayName,
			Description: global.SysConfig.Service.Description,
		}
		prg := &program{}
		s, err := service.New(prg, svcConfig)
		if err != nil {
			log.Error("定义服务配置时遇到错误：" + err.Error())
			return
		}

		if global.Args.IsInstall {
			err = s.Install()
			if err != nil {
				log.Error("安装为服务时遇到错误：" + err.Error())
			} else {
				fmt.Println(fmt.Sprintf("服务 %s 安装成功", global.SysConfig.Service.Name))
			}
			return
		}
		if global.Args.IsUninstall {
			err = s.Uninstall()
			if err != nil {
				log.Error("卸载服务时遇到错误：" + err.Error())
			} else {
				fmt.Println(fmt.Sprintf("服务 %s 卸载成功", global.SysConfig.Service.Name))
			}
			return
		}

		//全局错误处理（panic后重启服务）
		defer func() {
			err := recover()
			if err != nil {
				log.Error(fmt.Sprintf("recover get err: %s", err))
				log.Error(string(debug.Stack()))
				log.Warn("service restart")
				time.Sleep(time.Second * 3)
				err := s.Restart()
				if err != nil {
					log.Error(fmt.Sprintf("service restart err: %s", err.Error()))
				} else {
					log.Warn("service restart complete")
				}
			} else {
				log.Debug("recover exist")
			}
		}()

		err = s.Run()
		if err != nil {
			errMsg := fmt.Sprintf("service run err: %s", err.Error())

			log.Error(errMsg)
			err = s.Restart()
			if err != nil {
				log.Error(fmt.Sprintf("service restart err; %s", err.Error()))
			}
		}
	}
}

type program struct{}

func (p *program) Start(s service.Service) error {
	err := p.run()
	if err != nil {
		log.Error(fmt.Sprintf("服务启动时遇到错误：%s", err.Error()))
	}
	go func() {
		select {
		case <-global.Ctx.Done():
			err := p.Stop(s)
			if err != nil {
				//fmt.Println(err.Error())
				log.Error(err.Error())
			}
			time.Sleep(time.Second * 3)
			log.Warn("exist")
			os.Exit(0)
		}
	}()
	return err
}

func (p *program) run() error {
	//服务所执行的代码
	log.Warn("Service Starting")
	defer log.Warn("Service Started")
	{
		return myService.StartService()
	}
}

func (p *program) Stop(s service.Service) error {
	log.Warn("Service Stopping")
	defer log.Warn("Service Stopped")
	{
	}
	return nil
}
