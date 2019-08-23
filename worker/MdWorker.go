package worker

import (
	"github.com/Deansquirrel/goServiceSupportHelper"
	"github.com/Deansquirrel/goToolCommon"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	"github.com/Deansquirrel/goZ9MdDataTransV2/repository"
	"math"
	"time"
)

var zxKc map[int]float64
var xsSl map[int]*object.MdHpXsSlHz

const (
	minKcDifference = 0.000001
)

func init() {
	zxKc = make(map[int]float64)
	xsSl = make(map[int]*object.MdHpXsSlHz)
}

type mdWorker struct {
}

func NewMdWorker() *mdWorker {
	return &mdWorker{}
}

func (r *mdWorker) UpdateMdYyInfo(id string) {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	lastUpdate, err := repOnline.GetMdYyInfoLastUpdate()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	repMd := repository.NewRepMd()
	tClose, err := repMd.GetLastMdYyDate()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	if goToolCommon.GetDateStr(lastUpdate) == goToolCommon.GetDateStr(goToolMSSqlHelper.GetDefaultOprTime()) {
		lastUpdate = tClose
	}
	endDate := tClose.Add(time.Hour * 24)
	list, err := repMd.GetMdYyInfo(goToolCommon.GetDateStr(lastUpdate), goToolCommon.GetDateStr(endDate))
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	lastYyr := goToolMSSqlHelper.GetDefaultOprTime()
	for _, d := range list {
		err = repOnline.UpdateMdYyInfo(d)
		if err != nil {
			_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
			return
		}
		if goToolCommon.GetDateStr(d.FYyr) > goToolCommon.GetDateStr(lastYyr) {
			lastYyr = d.FYyr
		}
	}
	err = repOnline.UpdateMdYyInfoLastUpdate(lastYyr)
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
}

func (r *mdWorker) UpdateZxKc(id string) {
	repMd := repository.NewRepMd()
	kcList, err := repMd.GetZxKc()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	for _, kc := range kcList {
		zkc, ok := zxKc[kc.FHpId]
		if ok {
			if math.Dim(math.Max(zkc, kc.FSl), math.Min(zkc, kc.FSl)) >= minKcDifference {
				err = r.updateZxKc(kc)
				if err != nil {
					_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
					return
				}
			}
		} else {
			err = r.updateZxKc(kc)
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
		}
	}
}

func (r *mdWorker) updateZxKc(d *object.ZxKc) error {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		return err
	}
	err = repOnline.UpdateZxKc(d)
	if err != nil {
		return err
	} else {
		zxKc[d.FHpId] = d.FSl
	}
	return nil
}

func (r *mdWorker) UpdateMdHpXsSlHz(id string) {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	lastUpdate, err := repOnline.GetMdHpXsSlHzLastUpdate()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	repMd := repository.NewRepMd()
	tClose, err := repMd.GetLastMdYyDate()
	if err != nil {
		_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
		return
	}
	if goToolCommon.GetDateStr(lastUpdate) == goToolCommon.GetDateStr(goToolMSSqlHelper.GetDefaultOprTime()) {
		lastUpdate = tClose
	}
	gLast := lastUpdate
	eDate := lastUpdate
	d := time.Hour * 24 * 7
	for {
		eDate = gLast.Add(d)
		if goToolCommon.GetDateStr(eDate) < goToolCommon.GetDateStr(tClose) {
			err = r.updateMdHpXsSlHz(goToolCommon.GetDateStr(gLast), goToolCommon.GetDateStr(eDate))
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
			gLast = eDate.Add(time.Hour * 24)
		} else {
			err = r.updateMdHpXsSlHz(goToolCommon.GetDateStr(gLast), goToolCommon.GetDateStr(tClose))
			if err != nil {
				_ = goServiceSupportHelper.JobErrRecord(id, err.Error())
				return
			}
			break
		}
	}
}

func (r *mdWorker) updateMdHpXsSlHz(begDate string, endDate string) error {
	repOnline, err := repository.NewRepOnline()
	if err != nil {
		return err
	}
	repMd := repository.NewRepMd()
	list, err := repMd.GetMdHpXsSlHz(begDate, endDate)
	if err != nil {
		return err
	}
	uFlag := false
	uDate := goToolMSSqlHelper.GetDefaultOprTime()

	for _, d := range list {
		currD, ok := xsSl[d.FHpId]
		if ok {
			if goToolCommon.GetDateStr(d.FYyDate) > goToolCommon.GetDateStr(currD.FYyDate) {
				//新日期数据
				xsSl[d.FHpId] = d
				err = repOnline.UpdateMdHpXsSlHz(d)
				if err != nil {
					return err
				}
			} else if goToolCommon.GetDateStr(d.FYyDate) == goToolCommon.GetDateStr(currD.FYyDate) {
				//当日数据
				if math.Dim(math.Max(currD.FXsQty, d.FXsQty), math.Min(currD.FXsQty, d.FXsQty)) > minKcDifference {
					xsSl[d.FHpId] = d
					err = repOnline.UpdateMdHpXsSlHz(d)
					if err != nil {
						return err
					}
				}
			} else {
				//历史数据
				err = repOnline.UpdateMdHpXsSlHz(d)
				if err != nil {
					return err
				}
			}
		} else {
			//新增数据
			xsSl[d.FHpId] = d
			err = repOnline.UpdateMdHpXsSlHz(d)
			if err != nil {
				return err
			}
		}
		if goToolCommon.GetDateStr(d.FYyDate) > goToolCommon.GetDateStr(uDate) {
			uDate = d.FYyDate
		}
		uFlag = true
	}
	if uFlag {
		err = repOnline.UpdateMdHpXsSlHzLastUpdate(uDate)
		if err != nil {
			return err
		}
	}
	return nil
}
