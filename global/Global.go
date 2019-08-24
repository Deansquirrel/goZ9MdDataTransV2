package global

import (
	"context"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
)

const (
	//goServiceSupportHelper Version "1.0.7 Build20190823"
	//PreVersion = "0.0.0 Build20190101"
	//TestVersion = "0.0.0 Build20190101"
	Version   = "0.0.0 Build20190101"
	Type      = "Z9MdDataTransV2"
	SecretKey = "Z9MdDataTransV2"
)

var Ctx context.Context
var Cancel func()

//程序启动参数
var Args *object.ProgramArgs

//系统参数
var SysConfig *object.SystemConfig
