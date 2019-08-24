package repository

import (
	"errors"
	"fmt"
	"github.com/Deansquirrel/goToolCommon"
	"github.com/Deansquirrel/goToolMSSql2000"
	"github.com/Deansquirrel/goToolMSSqlHelper"
	"github.com/Deansquirrel/goZ9MdDataTransV2/object"
	"time"
)

import log "github.com/Deansquirrel/goToolLog"

const (
	sqlGetLastMdYyDate = "" +
		"select top 1 mdyydate " +
		"from xtmdyystatus " +
		"order by mdyydate desc"
	sqlGetLastMdYyClosedDate = "" +
		"select top 1 mdyydate " +
		"from xtmdyystatus " +
		"where mdyystatus = 1 or mdyystatus = 2 " +
		"order by mdyydate desc"
	sqlGetMdYyInfo = "" +
		"declare @begdate smalldatetime " +
		"declare @enddate smalldatetime  " +
		"set @begdate = ? " +
		"set @enddate = ? " +
		"Select billdate as 营业日,brid as 门店id,Sum(tc) As 次数,Sum(realmy) As 金额 From ( " +
		"	Select Top 0 Null As billdate,Null As brid,Null As tc,Null As realmy " +
		"	Union All " +
		"	SELECT [ckyyr],[ckmdid],case when [ckcxbj]=1 then -1 else 1 end as [num],[ckcjje] as [srmy] " +
		"	FROM [z3xsckt] WITH(NOLOCK) " +
		"	WHERE [ckyyr]>=@begdate And [ckyyr]<dateadd(d,1,@enddate) " +
		"	UNION ALL " +
		"	SELECT [ckyyr],[ckmdid],case when [ckcxbj]=1 then -1 else 1 end as [num],[ckcjje] as [srmy] " +
		"	FROM [z3xscklst] " +
		"	WHERE [ckyyr]>=@begdate And [ckyyr]<dateadd(d,1,@enddate) " +
		")  b " +
		"group by billdate,brid " +
		"order by billdate asc"
	sqlGetMdId = "" +
		"select coid " +
		"from zlcompany"
	sqlGetZxKc = "" +
		"SELECT B.[coid],A.[tzhpid],A.[tzsl],A.[tzbdsj] " +
		"FROM Z3XTTZ A " +
		"INNER JOIN ZLCOMPANY B ON 1=1 " +
		"WHERE A.[tzckid] = 0 AND A.[tzbdsj] > ?"
	sqlGetMdHpXsSlHz = "" +
		"declare @begdate smalldatetime " +
		"declare @enddate smalldatetime " +
		"set @begdate = ? " +
		"set @enddate = ? " +
		"select rq as 营业日,brid as 门店id,gsid as 货品id,sum(qty) as [销售数量（min）] from ( " +
		"   select top 0 null as rq,null as brid ,null as gsid ,null as qty " +
		"   Union All " +
		"	SELECT [ckdyyr],[ckdmdid],[ckdhpid],[ckdzxsl] " +
		"	FROM [z3xsckdt] WITH(NOLOCK) " +
		"	WHERE [ckdyyr] >= @begdate AND [ckdyyr] < dateadd(d,1,@enddate) " +
		"	UNION ALL " +
		"	SELECT [ckdyyr],[ckdmdid],[ckdhpid],[ckdzxsl] " +
		"	FROM [z3xsckdlst] " +
		"	WHERE [ckdyyr] >= @begdate AND [ckdyyr] < dateadd(d,1,@enddate) " +
		")a " +
		"group by rq,brid,gsid " +
		"order by rq"
)

type repMd struct {
	dbConfig *goToolMSSql2000.MSSqlConfig
}

func NewRepMd() *repMd {
	comm := NewCommon()
	return &repMd{
		dbConfig: goToolMSSqlHelper.ConvertDbConfigTo2000(comm.GetLocalDbConfig()),
	}
}

func (r *repMd) GetLastMdYyDate() (time.Time, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetLastMdYyDate)
	if err != nil {
		errMsg := fmt.Sprintf("GetLastMdYyDate err: %s", err.Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	sResult := ""
	for rows.Next() {
		err = rows.Scan(&sResult)
		if err != nil {
			errMsg := fmt.Sprintf("GetLastMdYyDate read data err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetLastMdYyDate read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	if sResult != "" {
		tResult, err := time.Parse("20060102", sResult)
		if err != nil {
			errMsg := fmt.Sprintf("GetLastMdYyDate err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
		return tResult, nil
	} else {
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New("GetLastMdYyDate return empty")
	}
}

func (r *repMd) GetLastMdYyClosedDate() (time.Time, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetLastMdYyClosedDate)
	if err != nil {
		errMsg := fmt.Sprintf("GetLastMdYyDate err: %s", err.Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	sResult := ""
	for rows.Next() {
		err = rows.Scan(&sResult)
		if err != nil {
			errMsg := fmt.Sprintf("GetLastMdYyDate read data err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetLastMdYyDate read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
	}
	if sResult != "" {
		tResult, err := time.Parse("20060102", sResult)
		if err != nil {
			errMsg := fmt.Sprintf("GetLastMdYyDate err: %s", err.Error())
			log.Error(errMsg)
			return goToolMSSqlHelper.GetDefaultOprTime(), errors.New(errMsg)
		}
		return tResult, nil
	} else {
		return goToolMSSqlHelper.GetDefaultOprTime(), errors.New("GetLastMdYyDate return empty")
	}
}

func (r *repMd) GetMdId() (int, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetMdId)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdId err: %s", err.Error())
		log.Error(errMsg)
		return 0, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	mdId := 0
	flag := false
	for rows.Next() {
		err = rows.Scan(&mdId)
		if err != nil {
			errMsg := fmt.Sprintf("GetMdId read data err: %s", err.Error())
			log.Error(errMsg)
			return 0, errors.New(errMsg)
		}
		flag = true
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetMdId read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return 0, errors.New(errMsg)
	}
	if flag {
		return mdId, nil
	} else {
		return 0, errors.New("GetMdId return empty")
	}
}

func (r *repMd) GetMdYyInfo(begDate string, endDate string) ([]*object.MdYyInfo, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetMdYyInfo, begDate, endDate)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdYyInfo err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	var fYyr time.Time
	var fTc, fMdId int
	var fSr float64
	rList := make([]*object.MdYyInfo, 0)
	for rows.Next() {
		err = rows.Scan(&fYyr, &fMdId, &fTc, &fSr)
		if err != nil {
			errMsg := fmt.Sprintf("read GetMdYyInfo data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.MdYyInfo{
			FMdId:    fMdId,
			FYyr:     fYyr,
			FTc:      fTc,
			FSr:      fSr,
			FOprTime: time.Now(),
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("read GetMdYyInfo data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repMd) GetZxKc(lastTime time.Time) ([]*object.ZxKc, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetZxKc, goToolCommon.GetDateTimeStrWithMillisecond(lastTime))
	if err != nil {
		errMsg := fmt.Sprintf("GetZxKc err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	var fMdId, fHpId int
	var fSl float64
	var fOprTime time.Time
	rList := make([]*object.ZxKc, 0)
	for rows.Next() {
		err = rows.Scan(&fMdId, &fHpId, &fSl, &fOprTime)
		if err != nil {
			errMsg := fmt.Sprintf("read GetZxKc data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.ZxKc{
			FMdId:    fMdId,
			FHpId:    fHpId,
			FSl:      fSl,
			FOprTime: fOprTime,
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("read GetZxKc data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repMd) GetMdHpXsSlHz(begDate string, endDate string) ([]*object.MdHpXsSlHz, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetMdHpXsSlHz, begDate, endDate)
	if err != nil {
		errMsg := fmt.Sprintf("GetMdHpXsSlHz err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	var fYyDate time.Time
	var fMdId, fHpId int
	var fXsQty float64
	rList := make([]*object.MdHpXsSlHz, 0)
	for rows.Next() {
		err = rows.Scan(&fYyDate, &fMdId, &fHpId, &fXsQty)
		if err != nil {
			errMsg := fmt.Sprintf("read GetMdHpXsSlHz data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.MdHpXsSlHz{
			FYyDate:  fYyDate,
			FMdId:    fMdId,
			FHpId:    fHpId,
			FXsQty:   fXsQty,
			FOprTime: time.Now(),
		})
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("read GetMdHpXsSlHz data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}
