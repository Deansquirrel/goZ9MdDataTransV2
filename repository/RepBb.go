package repository

import (
	"errors"
	"fmt"
	"github.com/Deansquirrel/goToolCommon"
	"github.com/Deansquirrel/goToolMSSql2000"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
)

import log "github.com/Deansquirrel/goToolLog"

const (
	sqlRestoreMdYyInfo = "" +
		"IF EXISTS (SELECT * FROM [mdyyinfo] WHERE [mdid] = ? AND [yyr] = ?) " +
		"	BEGIN " +
		"		UPDATE [mdyyinfo] " +
		"		SET [tc]=?,[sr]=?,[recorddate]=? " +
		"		WHERE [mdid] = ? AND [yyr] = ? " +
		"	END " +
		"ELSE " +
		"	BEGIN " +
		"		INSERT INTO [mdyyinfo]([mdid],[yyr],[tc],[sr],[recorddate]) " +
		"		VALUES(?,?,?,?,?) " +
		"	END"
	sqlRestoreZxKc = "" +
		"IF EXISTS(SELECT * FROM [zxkc] WHERE [mdid]=? AND [hpid]=?) " +
		"	BEGIN " +
		"		UPDATE [zxkc] " +
		"		SET [sl]=?,[lastupdate]=? " +
		"		WHERE [mdid]=? AND [hpid]=? " +
		"	END " +
		"ELSE " +
		"	BEGIN " +
		"		INSERT INTO [zxkc]([mdid],[hpid],[sl],[lastupdate]) " +
		"		VALUES (?,?,?,?) " +
		"	END"
	sqlRestoreMdHpXsSlHz = "" +
		"IF EXISTS(SELECT * FROM [mdhpxsslhz] WHERE [yydate]=? AND [mdid]=? AND [hpid]=?) " +
		"	BEGIN " +
		"		UPDATE [mdhpxsslhz] " +
		"		SET [xsqty]=?,[jlsj]=? " +
		"		WHERE [yydate]=? AND [mdid]=? AND [hpid]=? " +
		"	END " +
		"ELSE " +
		"	BEGIN " +
		"		INSERT INTO [mdhpxsslhz]([yydate],[mdid],[hpid],[xsqty],[jlsj]) " +
		"		VALUES (?,?,?,?,?) " +
		"	END"
)

type repBb struct {
	dbConfig *goToolMSSql2000.MSSqlConfig
}

func NewRepBb() *repBb {
	comm := NewCommon()
	return &repBb{
		dbConfig: goToolMSSqlHelper.ConvertDbConfigTo2000(comm.GetLocalDbConfig()),
	}
}

func (r *repBb) RestoreMdYyInfo(opr *object.MdYyInfoOpr) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(r.dbConfig, sqlRestoreMdYyInfo,
		opr.FMdId,
		goToolCommon.GetDateStr(opr.FYyr),
		opr.FTc,
		opr.FSr,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FOprTime),
		opr.FMdId,
		goToolCommon.GetDateStr(opr.FYyr),
		opr.FMdId,
		goToolCommon.GetDateStr(opr.FYyr),
		opr.FTc,
		opr.FSr,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FOprTime),
	)
	if err != nil {
		errMsg := fmt.Sprintf("RestoreMdYyInfo err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repBb) RestoreZxKc(opr *object.ZxKcOpr) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(r.dbConfig, sqlRestoreZxKc,
		opr.FMdId,
		opr.FHpId,
		opr.FSl,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FOprTime),
		opr.FMdId,
		opr.FHpId,
		opr.FMdId,
		opr.FHpId,
		opr.FSl,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FOprTime))
	if err != nil {
		errMsg := fmt.Sprintf("RestoreZxKc err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (r *repBb) RestoreMdHpXsSlHz(opr *object.MdHpXsSlHzOpr) error {
	err := goToolMSSqlHelper.SetRowsBySQL2000(r.dbConfig, sqlRestoreMdHpXsSlHz,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FYyDate), opr.FMdId, opr.FHpId,
		opr.FXsQty, opr.FOprTime,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FYyDate), opr.FMdId, opr.FHpId,
		goToolCommon.GetDateTimeStrWithMillisecond(opr.FYyDate), opr.FMdId, opr.FHpId, opr.FXsQty, opr.FOprTime)
	if err != nil {
		errMsg := fmt.Sprintf("RestoreMdHpXsSlHz err: %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}
