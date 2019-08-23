package worker

import (
	"github.com/Deansquirrel/goServiceSupportHelper"
	"github.com/Deansquirrel/goZ9MdDataTransV2/repository"
)

type bbWorker struct {
}

func NewBbWorker() *bbWorker {
	return &bbWorker{}
}

func (r *bbWorker) RestoreMdYyInfo(id string) {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	for {
		list, err := repOnline.GetMdYyInfoOpr()
		if err != nil {
			_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
			return
		}
		if len(list) < 1 {
			break
		}
		repBb := repository.NewRepBb()
		for _, opr := range list {
			err = repBb.RestoreMdYyInfo(opr)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
			err = repOnline.DelMdYyInfoOpr(opr.OprSn)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
		}
	}
}

func (r *bbWorker) RestoreZxKc(id string) {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	for {
		list, err := repOnline.GetZxKcOpr()
		if err != nil {
			_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
			return
		}
		if len(list) < 1 {
			break
		}
		repBb := repository.NewRepBb()
		for _, opr := range list {
			err = repBb.RestoreZxKc(opr)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
			err = repOnline.DelZxKcOpr(opr.FOprSn)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
		}
	}
}

func (r *bbWorker) RestoreMdHpXsSlHz(id string) {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	for {
		list, err := repOnline.GetMdHpXsSlHzOpr()
		if err != nil {
			_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
			return
		}
		if len(list) < 1 {
			break
		}
		repBb := repository.NewRepBb()
		for _, opr := range list {
			err = repBb.RestoreMdHpXsSlHz(opr)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
			err = repOnline.DelMdHpXsSlHzOpr(opr.FOprSn)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
		}
	}
}
