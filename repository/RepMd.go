package repository

import (
	"bytes"
	"errors"
	"fmt"
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
		"Select billdate as 营业日,brid as 门店id,Sum(tc) As 次数,Sum(realmy) As 金额 " +
		"From ( " +
		"	Select Top 0 Null As billdate,Null As brid,Null As tc,Null As realmy " +
		"	Union All " +
		"	Select xsckbilldate,xsckoprbrid,Cast(xsckpeople As int),xsckrealmy From [ywxsckt1] " +
		"	Where xsckbilldate>=@begdate And xsckbilldate<@enddate " +
		"	%s " +
		"	Union All " +
		"	Select qdxsbilldate,qdxsdoprbrid,Cast(1 As int),qdxsrealmy From [ywddqdxst] " +
		"	Where qdxsbilldate>=@begdate And qdxsbilldate<@enddate " +
		"	Union All " +
		"	Select jrdhbilldate,jrdhoprbrid,Cast(1 As int),jrdhtktsum From [ywjrdht] a " +
		"	Where jrdhbilldate>=@begdate And jrdhbilldate<@enddate " +
		")  b group by billdate,brid " +
		"order by billdate asc"
	sqlGetJtMdYyInfoTemplete = "" +
		"	Union All " +
		"	Select xsckbilldate,xsckoprbrid,Cast(xsckpeople As int),xsckrealmy From [%s] " +
		"	Where xsckbilldate>=@begdate And xsckbilldate<@enddate "
	sqlGetMdId = "" +
		"select coid " +
		"from zlcompany"
	sqlGetZxKc = "" +
		"select b.coid,a.gsid,a.gsqty " +
		"from xttz a " +
		"inner join zlcompany b on 1=1 " +
		"where a.dptid = -11"
	sqlGetMdHpXsSlHz = "" +
		"declare @begdate smalldatetime " +
		"declare @enddate smalldatetime " +
		"set @begdate = ? " +
		"set @enddate = ? " +
		"select rq as 营业日,brid as 门店id,gsid as 货品id,sum(qty) as [销售数量（min）] " +
		"from ( " +
		"    select top 0 null as rq,null as gsid ,null as qty,null as brid " +
		"    Union All " +
		"    select xsckhdbilldate,xsckhdgsid,xsckhdqty,xsckhdoprbrid " +
		"    from ywxsckhdt " +
		"    where xsckhdbilldate>=convert(varchar(10),@begdate,121) and xsckhdbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) " +
		"	 %s " +
		"    Union All " +
		"    select xsthhdbilldate,xsthhdgsid,-xsthhdqty,xsthhdoprbrid " +
		"    from ywxsthhdt " +
		"    where xsthhdbilldate>=convert(varchar(10),@begdate,121) and xsthhdbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) " +
		"    Union All " +
		"    select qdxshdbilldate,qdxshdgsid,qdxshdqty,qdxshdoprbrid " +
		"    from ywddqdxshdt " +
		"    where qdxshdbilldate>=convert(varchar(10),@begdate,121) and qdxshdbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) " +
		"    Union All " +
		"    select ddjfhdbilldate,ddjfhdgsid,ddjfhdqty,ddjfhdoprbrid " +
		"    from ywkhddjfhdt " +
		"    where ddjfhdbilldate>=convert(varchar(10),@begdate,121) and ddjfhdbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) and substring(ddjfhdsno,3,3)=(select coid from zlcompany) " +
		"    union all " +
		"    select jrdhbilldate,a.jrdhhdgsid,a.jrdhhdqty,jrdhhdoprbrid " +
		"    from ywjrdhhdt a " +
		"    inner join ywjrdht b on left(a.jrdhhdrwsno,12)=b.jrdhsno " +
		"    where b.jrdhbilldate>=convert(varchar(10),@begdate,121) and b.jrdhbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) " +
		")a " +
		"group by rq,brid,gsid  "
	sqlGetJtMdHpXsSlHzTemplete = "" +
		"    Union All " +
		"    select xsckhdbilldate,xsckhdgsid,xsckhdqty,xsckhdoprbrid " +
		"    from [%s] " +
		"    where xsckhdbilldate>=convert(varchar(10),@begdate,121) and xsckhdbilldate<convert(varchar(10),dateadd(d,1,@enddate),121) "
	sqlGetJtMdYyInfoTableList = "" +
		"select name " +
		"from sysobjects " +
		"where xtype='U' and name like 'ywxsckt1[_]__[_]jt'"
	sqlGetJtMdHpXsSlHzTableList = "" +
		"select name " +
		"from sysobjects " +
		"where xtype='U' and name like 'ywxsckhdt[_]__[_]jt'"
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
	sqlStr, err := r.getMdYyInfoSql()
	if err != nil {
		return nil, err
	}
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlStr, begDate, endDate)
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

func (r *repMd) getMdYyInfoSql() (string, error) {
	jtTable, err := r.GetJtMdYyInfoTableList()
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	for _, t := range jtTable {
		buffer.WriteString(fmt.Sprintf(sqlGetJtMdYyInfoTemplete, t))
	}
	return fmt.Sprintf(sqlGetMdYyInfo, buffer.String()), nil
}

func (r *repMd) GetZxKc() ([]*object.ZxKc, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetZxKc)
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
	rList := make([]*object.ZxKc, 0)
	for rows.Next() {
		err = rows.Scan(&fMdId, &fHpId, &fSl)
		if err != nil {
			errMsg := fmt.Sprintf("read GetZxKc data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, &object.ZxKc{
			FMdId:    fMdId,
			FHpId:    fHpId,
			FSl:      fSl,
			FOprTime: time.Now(),
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
	sqlStr, err := r.getMdHpXsSlHzSql()
	if err != nil {
		return nil, err
	}
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlStr, begDate, endDate)
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

func (r *repMd) getMdHpXsSlHzSql() (string, error) {
	jtTable, err := r.GetJtMdHpXsSlHzTableList()
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	for _, t := range jtTable {
		buffer.WriteString(fmt.Sprintf(sqlGetJtMdHpXsSlHzTemplete, t))
	}
	return fmt.Sprintf(sqlGetMdHpXsSlHz, buffer.String()), nil
}

func (r *repMd) GetJtMdYyInfoTableList() ([]string, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetJtMdYyInfoTableList)
	if err != nil {
		errMsg := fmt.Sprintf("GetJtTableList err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rList := make([]string, 0)
	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			errMsg := fmt.Sprintf("GetJtTableList read data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, t)
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetJtTableList read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}

func (r *repMd) GetJtMdHpXsSlHzTableList() ([]string, error) {
	rows, err := goToolMSSqlHelper.GetRowsBySQL2000(r.dbConfig, sqlGetJtMdHpXsSlHzTableList)
	if err != nil {
		errMsg := fmt.Sprintf("GetJtMdHpXsSlHzTableList err: %s", err.Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	defer func() {
		_ = rows.Close()
	}()
	rList := make([]string, 0)
	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			errMsg := fmt.Sprintf("GetJtMdHpXsSlHzTableList read data err: %s", err.Error())
			log.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		rList = append(rList, t)
	}
	if rows.Err() != nil {
		errMsg := fmt.Sprintf("GetJtMdHpXsSlHzTableList read data err: %s", rows.Err().Error())
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}
	return rList, nil
}
