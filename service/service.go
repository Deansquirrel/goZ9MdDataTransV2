package service

import (
	"github.com/Deansquirrel/goServiceSupportHelper"
	log "github.com/Deansquirrel/goToolLog"
	"github.com/Deansquirrel/goZ9MdDataTransV2/global"
	"github.com/Deansquirrel/goZ9MdDataTransV2/worker"
)

//启动服务内容
func StartService() error {
	log.Debug("Start Service")
	defer log.Debug("Start Service Complete")

	comm := worker.NewCommon()
	comm.StartService()

	go func() {
		goServiceSupportHelper.InitParam(&goServiceSupportHelper.Params{
			HttpAddress:   global.SysConfig.SSConfig.Address,
			ClientType:    global.Type,
			ClientVersion: global.Version,
			Ctx:           global.Ctx,
			Cancel:        global.Cancel,
		})
	}()

	return nil
}
