package repository

import (
	"errors"
	"fmt"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	"time"
)

import log "github.com/Deansquirrel/goToolLog"

const (
	sqlUpdateMdYyInfo = "" +
		"INSERT INTO [mdyyinfo]([mdid],[yyr],[tc],[sr],[oprtime]) " +
		"VALUES (?,?,?,?,?)"
	sqlGetMdYyInfoLastUpdate = "" +
		"select yyr " +
		"from mdyyinfolastupdate " +
		"where mdid = ?"
	sqlUpdateMdYyInfoLastUpdate = "" +
		"IF EXISTS (SELECT * FROM mdyyinfolastupdate WHERE MDID=?) " +
		"	BEGIN " +
		"		UPDATE mdyyinfolastupdate " +
		"		SET yyr = ?,oprtime= ? " +
		"		WHERE mdid = ? " +
		"	END " +
		"ELSE " +
		"	BEGIN " +
		"		INSERT INTO mdyyinfolastupdate(mdid,yyr,oprtime) " +
		"		VALUES (?,?,?) " +
		"	END"
	sqlUpdateZxKc = "" +
		"INSERT INTO [zxkc]([mdid],[hpid],[sl],[oprtime]) " +
		"VALUES (?,?,?,?)"
	sqlUpdateMdHpXsSlHz = "" +
		"INSERT INTO [mdhpxsslhz]([yydate],[mdid],[hpid],[xsqty],[jlsj]) " +
		"VALUES (?,?,?,?,?)"
	sqlGetMdHpXsSlHzLastUpdate = "" +
		"select yyr " +
		"from mdhpxsslhzlastupdate " +
		"where mdid = ?"
	sqlUpdateMdHpXsSlHzLastUpdate = "" +
		"IF EXISTS (SELECT * FROM mdhpxsslhzlastupdate WHERE MDID=?) " +
		"	BEGIN " +
		"		UPDATE mdhpxsslhzlastupdate " +
		"		SET yyr = ?,oprtime= ? " +
		"		WHERE mdid = ? " +
		"	END " +
		"ELSE " +
		"	BEGIN " +
		"		INSERT INTO mdhpxsslhzlastupdate(mdid,yyr,oprtime) " +
		"		VALUES (?,?,?) " +
		"	END"

	sqlGetMdYyInfoOpr = "" +
		"SELECT top 1 [oprsn],[mdid],[yyr],[tc],[sr],[oprtime] " +
		"FROM [mdyyinfo] " +
		"ORDER BY [oprsn] ASC"

	sqlDelMdYyInfoOpr = "" +
		"DELETE FROM [mdyyinfo] " +
		"WHERE [oprsn]=?"

	sqlGetZxKcOpr = "" +
		"SELECT TOP 1 [oprsn],[mdid],[hpid],[sl],[oprtime] " +
		"FROM [zxkc] " +
		"ORDER BY [oprsn] ASC"

	sqlDelZxKcOpr = "" +
		"DELETE FROM [zxkc] " +
		"WHERE [oprsn]=?"

	sqlGetMdHpXsSlHzOpr = "" +
		"SELECT TOP 1 [oprsn],[yydate],[mdid],[hpid],[xsqty],[jlsj] " +
		"FROM [mdhpxsslhz] " +
		"ORDER BY [oprsn] ASC"
	sqlDelMdHpXsSlHzOpr = "" +
		"DELETE FROM [mdhpxsslhz] " +
		"WHERE [oprsn]=?"
)

type repOnline struct {
	dbConfig *goToolMSSql.MSSqlConfig
}

func NewRepOnline() (*repOnline, error) {
	dbConfig, err := NewCommon().GetOnLineDbConfig()
	if err != nil {
		return nil, err
	}
	return &repOnline{
		dbConfig: dbConfig,
	}, nil
}

func (r *repOnline) GetMdYyInfoLastUpdate() (time.Time, error) {
	repMd := NewRepMd()
	mdId, err := repMd.GetMdId()
	if err != nil {
		return goToolMSSqlHelper.GetDefaultOprTime(), err
	}
	rows, err := goToolMSSqlHelper.GetRowsBySQL(r.dbConfig, sqlGetMdYyInfoLastUpdate, mdId)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdYyInfoLastUpdate err: %s", err.Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rTime := time.Now()
	flag := false
	for rows.Next() {
		err = rows.Scan(&rTime)
		if err != nil {
			errMsg := fmt.Sprintf("GetMdYyInfoLastUpdate read data err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
		flag = true
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetMdYyInfoLastUpdate read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	if flag {
		return rTime, nil
	} else {
		return goToolMSSqlHelper.GetDefaultOprTime(), nil
	}
}

func (r *repOnline) UpdateMdYyInfoLastUpdate(t time.Time) error {
	repMd := NewRepMd()
	mdId, err := repMd.GetMdId()
	if err != nil {
		return err
	}
	err = goToolMSSqlHelper.SetRowsBySQL(r.dbConfig, sqlUpdateMdYyInfoLastUpdate,
		mdId,
		t, time.Now(), mdId,
		mdId, t, time.Now())
	if err != nil {
		errMsg := fmt.Sprintf("UpdateMdYyInfoLastUpdate err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) UpdateMdYyInfo(d *object.MdYyInfo) error {
	err := goToolMSSqlHelper.SetRowsBySQL(r.dbConfig, sqlUpdateMdYyInfo,
		d.FMdId, d.FYyr, d.FTc, d.FSr, d.FOprTime)
	if err != nil {
		errMsg := fmt.Sprintf("UpdateMdYyInfo err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) UpdateZxKc(d *object.ZxKc) error {
	err := goToolMSSqlHelper.SetRowsBySQL(r.dbConfig, sqlUpdateZxKc,
		d.FMdId, d.FHpId, d.FSl, d.FOprTime)
	if err != nil {
		errMsg := fmt.Sprintf("UpdateZxKc err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) GetMdHpXsSlHzLastUpdate() (time.Time, error) {
	repMd := NewRepMd()
	mdId, err := repMd.GetMdId()
	if err != nil {
		return goToolMSSqlHelper.GetDefaultOprTime(), err
	}
	rows, err := goToolMSSqlHelper.GetRowsBySQL(r.dbConfig, sqlGetMdHpXsSlHzLastUpdate, mdId)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdHpXsSlHzLastUpdate err: %s", err.Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rTime := time.Now()
	flag := false
	for rows.Next() {
		err = rows.Scan(&rTime)
		if err != nil {
			errMsg := fmt.Sprintf("GetMdHpXsSlHzLastUpdate read data err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
		flag = true
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetMdHpXsSlHzLastUpdate read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	if flag {
		return rTime, nil
	} else {
		return goToolMSSqlHelper.GetDefaultOprTime(), nil
	}
}

func (r *repOnline) UpdateMdHpXsSlHzLastUpdate(t time.Time) error {
	repMd := NewRepMd()
	mdId, err := repMd.GetMdId()
	if err != nil {
		return err
	}
	err = goToolMSSqlHelper.SetRowsBySQL(r.dbConfig, sqlUpdateMdHpXsSlHzLastUpdate,
		mdId,
		t, time.Now(), mdId,
		mdId, t, time.Now())
	if err != nil {
		errMsg := fmt.Sprintf("UpdateMdHpXsSlHzLastUpdate err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) UpdateMdHpXsSlHz(d *object.MdHpXsSlHz) error {
	err := goToolMSSqlHelper.SetRowsBySQL(r.dbConfig, sqlUpdateMdHpXsSlHz,
		d.FYyDate, d.FMdId, d.FHpId, d.FXsQty, d.FOprTime)
	if err != nil {
		errMsg := fmt.Sprintf("UpdateMdHpXsSlHz err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) GetMdYyInfoOpr() ([]*object.MdYyInfoOpr, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL(r.dbConfig, sqlGetMdYyInfoOpr)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdYyInfoOpr err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rList := make([]*object.MdYyInfoOpr, 0)
	var oprSn int64
	var mdId, tc int
	var yyr, oprTime time.Time
	var sr float64
	for rows.Next() {
		err = rows.Scan(&oprSn, &mdId, &yyr, &tc, &sr, &oprTime)
		if err != nil {
			errMsg := fmt.Sprintf("GetMdYyInfoOpr read data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.MdYyInfoOpr{
			OprSn:    oprSn,
			FMdId:    mdId,
			FYyr:     yyr,
			FTc:      tc,
			FSr:      sr,
			FOprTime: oprTime,
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetMdYyInfoOpr read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repOnline) DelMdYyInfoOpr(sn int64) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(goToolMSSqlHelper.ConvertDbConfigTo2000(r.dbConfig), sqlDelMdYyInfoOpr, sn)
	if err != nil {
		errMsg := fmt.Sprintf("DelMdYyInfoOpr err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) GetZxKcOpr() ([]*object.ZxKcOpr, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL(r.dbConfig, sqlGetZxKcOpr)
	if err != nil {
		errMsg := fmt.Sprintf("GetZxKcOpr err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rList := make([]*object.ZxKcOpr, 0)
	var oprSn int64
	var mdId, hpId int
	var oprTime time.Time
	var sl float64
	for rows.Next() {
		err = rows.Scan(&oprSn, &mdId, &hpId, &sl, &oprTime)
		if err != nil {
			errMsg := fmt.Sprintf("GetZxKcOpr read data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.ZxKcOpr{
			FOprSn:   oprSn,
			FMdId:    mdId,
			FHpId:    hpId,
			FSl:      sl,
			FOprTime: oprTime,
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetZxKcOpr read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repOnline) DelZxKcOpr(sn int64) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(goToolMSSqlHelper.ConvertDbConfigTo2000(r.dbConfig), sqlDelZxKcOpr, sn)
	if err != nil {
		errMsg := fmt.Sprintf("DelZxKcOpr err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repOnline) GetMdHpXsSlHzOpr() ([]*object.MdHpXsSlHzOpr, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL(r.dbConfig, sqlGetMdHpXsSlHzOpr)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdHpXsSlHzOpr err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rList := make([]*object.MdHpXsSlHzOpr, 0)
	var fOprSn int64
	var fYyDate time.Time
	var fMdId, fHpId int
	var fXsQty float64
	var fOprTime time.Time
	for rows.Next() {
		err = rows.Scan(&fOprSn, &fYyDate, &fMdId, &fHpId, &fXsQty, &fOprTime)
		if err != nil {
			errMsg := fmt.Sprintf("GetMdHpXsSlHzOpr read data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.MdHpXsSlHzOpr{
			FOprSn:   fOprSn,
			FYyDate:  fYyDate,
			FMdId:    fMdId,
			FHpId:    fHpId,
			FXsQty:   fXsQty,
			FOprTime: fOprTime,
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetMdHpXsSlHzOpr read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repOnline) DelMdHpXsSlHzOpr(sn int64) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(goToolMSSqlHelper.ConvertDbConfigTo2000(r.dbConfig), sqlDelMdHpXsSlHzOpr, sn)
	if err != nil {
		errMsg := fmt.Sprintf("DelMdHpXsSlHzOpr err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}
