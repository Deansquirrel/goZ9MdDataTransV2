package object

type RunMode string

const (
	RunModeMdCollect RunMode = "MdCollect" //门店采集
	RunModeBbRestore RunMode = "BbRestore" //报表恢复
)
