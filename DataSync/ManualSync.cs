using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataSync.DBUtility;
using System.Threading.Tasks;
using Amib.Threading;
using System.Data;
using System.Data.SqlClient;

namespace DataSync
{
    class ManualSync
    {
        private string TableName = string.Empty;
        private int Row = 2000;
        private delegate object Delegate(object row); // 定义一个委托
        private Delegate @delegate;

        /// <summary>
        /// 构造函数一（默认行数2000） 朱旺 2018.4.24 add
        /// </summary>
        /// <param name="_Table"></param>
        public ManualSync(string _Table)
        {
            TableName = _Table;
            if (TableName == "Item")
            {
                @delegate = AddItem;
            }
            if (TableName == "StoreDefine")
            {
                @delegate = AddStoreDefine;
            }
        }
        /// <summary>
        /// 构造函数二（自定义行数）朱旺 2018.4.24 add
        /// </summary>
        /// <param name="_Table"></param>
        /// <param name="_Row"></param>
        public ManualSync(string _Table, int _Row)
        {
            TableName = _Table;
            Row = _Row;
            if (TableName == "Item")
            {
                @delegate = AddItem;
            }
            if (TableName == "StoreDefine")
            {
                @delegate = AddStoreDefine;
            }
        }

        #region 公共逻辑语句块  朱旺 2018.4.23 add  
        /// <summary>
        /// 程序入口  朱旺  2018.4.23 add
        /// </summary>
        public void SyncData()
        {
            int Count = GetCount();
            if (Count != 0)
            {
                int Page = Count % Row == 0 ? Count / Row : Count / Row + 1;//第一步计算该开多少个线程
                DeleteTable();//第二步清空表数据
                //第三步多线程执行方法
                using (SmartThreadPool smartThreadPool = new SmartThreadPool())
                {
                    List<IWorkItemResult> wirs = new List<IWorkItemResult>();
                    for (int i = 1; i <= Page; i++)
                    {
                        IWorkItemResult wir = smartThreadPool.QueueWorkItem(new WorkItemCallback(@delegate), i);
                        wirs.Add(wir);
                    }
                    SmartThreadPool.WaitAll(wirs.ToArray());
                }
            }
        }
        /// <summary>
        /// 获取某个表有多少条数据  朱旺  2018.4.23 add
        /// </summary>
        /// <returns></returns>
        private int GetCount()
        {
            StringBuilder Sqlstr = new StringBuilder();
            Sqlstr.AppendFormat("select count(*) from {0}", TableName);
            object CountObj = DbHelperSQL.GetSingle(DbHelperSQL.u8connectionString, Sqlstr.ToString());
            return int.Parse(CountObj.ToString());
        }
        /// <summary>
        /// 清空表所有数据  朱旺 2018.4.23 add
        /// </summary>
        private void DeleteTable()
        {
            StringBuilder Sqlstr = new StringBuilder();
            Sqlstr.AppendFormat(" Delete {0}", TableName);
            DbHelperSQL.ExecuteSql(Sqlstr.ToString());
        }
        #endregion

        #region 商品数据同步语句块  朱旺 2018.4.23 add
        /// <summary>
        /// 往商品表插入数据(BLL)  朱旺  2018.4.23 add
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private object AddItem(object obj)
        {
            DataTable dataTable = GetItem(int.Parse(obj.ToString())).Tables[0];
            foreach (DataRow item in dataTable.Rows)
            {
                AddItem(item);
            }
            return 0;
        }
        /// <summary>
        /// 获取商品表dataset 朱旺 2018.4.23 add
        /// </summary>
        /// <param name="Page"></param>
        /// <returns></returns>
        private DataSet GetItem(int Page)
        {
            StringBuilder Sql = new StringBuilder();
            Sql.AppendFormat("SELECT * from Item  ORDER BY fchrItemID OFFSET　{0} ROW  FETCH NEXT {1} ROW ONLY", (Page - 1) * Row, Row);
            DataSet ds = DbHelperSQL.Query(DbHelperSQL.u8connectionString, Sql.ToString());
            return ds;
        }
        /// <summary>
        /// 往商品表插入数据(DAL)  朱旺  2018.4.23 add
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private bool AddItem(DataRow row)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append("insert into Item(");
            strSql.Append("fchrItemID,fchrItemName,fchrAddCode,fbitNoUsed,fchrItemTypeID,fchrAccountID,fchrBarCode,fbitDiscount,fbitSpecial,fbitValidDay,fintValidDays,fbitBatch,fbitSerialNo,fintSerialNoLen,fchrProducingArea,fbitOTC,fchrLevel,fbitFree1,fbitFree2,fbitFree3,fbitFree4,fbitFree5,fbitFree6,fbitFree7,fbitFree8,fdtmLastModifyDate,fchrItemCode,fchrUnitID,fbitExport,fbitFree9,fbitFree10,fchrUnit,flotPrice,fintUnitDecimalDigits,fchrTimeStamp,fchrPKID,varInvDefine1,flotRefCostPrice,fbitPriceCanBeModify,fchrItemTypeCode,fbitVIP,fbitWholeFavorable,fchrSpec,fchrYear,fchrQuarter,fbitCanSale,fchrItemCDef1,fchrItemCDef2,fchrItemCDef3,fchrItemCDef4,fchrItemCDef5,fchrItemCDef6,fchrItemCDef7,fchrItemCDef8,fchrItemCDef9,fchrItemCDef10,fchrItemCDef11,fchrItemCDef12,fchrItemCDef13,fchrItemCDef14,fchrItemCDef15,fchrItemCDef16,fbitIsGiftToken,fbitInTrunk,fintBackRule,fintProcess,fbitIsStoredCard,fdtmGiftInvalidate,ftinItemModel,fbitInstantSale,fbitStopNeedItem,fchrAnaSql,fchrSql,fchrSqlText,fdtmStopDate,fchrDepositUnitID,fchrGtCode,fbitElecGift,fbitTransfer,fchrPrintTypeID,fbitIngredient,fbitUE,fintSaleType)");
            strSql.Append(" values (");
            strSql.Append("@fchrItemID,@fchrItemName,@fchrAddCode,@fbitNoUsed,@fchrItemTypeID,@fchrAccountID,@fchrBarCode,@fbitDiscount,@fbitSpecial,@fbitValidDay,@fintValidDays,@fbitBatch,@fbitSerialNo,@fintSerialNoLen,@fchrProducingArea,@fbitOTC,@fchrLevel,@fbitFree1,@fbitFree2,@fbitFree3,@fbitFree4,@fbitFree5,@fbitFree6,@fbitFree7,@fbitFree8,@fdtmLastModifyDate,@fchrItemCode,@fchrUnitID,@fbitExport,@fbitFree9,@fbitFree10,@fchrUnit,@flotPrice,@fintUnitDecimalDigits,@fchrTimeStamp,@fchrPKID,@varInvDefine1,@flotRefCostPrice,@fbitPriceCanBeModify,@fchrItemTypeCode,@fbitVIP,@fbitWholeFavorable,@fchrSpec,@fchrYear,@fchrQuarter,@fbitCanSale,@fchrItemCDef1,@fchrItemCDef2,@fchrItemCDef3,@fchrItemCDef4,@fchrItemCDef5,@fchrItemCDef6,@fchrItemCDef7,@fchrItemCDef8,@fchrItemCDef9,@fchrItemCDef10,@fchrItemCDef11,@fchrItemCDef12,@fchrItemCDef13,@fchrItemCDef14,@fchrItemCDef15,@fchrItemCDef16,@fbitIsGiftToken,@fbitInTrunk,@fintBackRule,@fintProcess,@fbitIsStoredCard,@fdtmGiftInvalidate,@ftinItemModel,@fbitInstantSale,@fbitStopNeedItem,@fchrAnaSql,@fchrSql,@fchrSqlText,@fdtmStopDate,@fchrDepositUnitID,@fchrGtCode,@fbitElecGift,@fbitTransfer,@fchrPrintTypeID,@fbitIngredient,@fbitUE,@fintSaleType)");
            SqlParameter[] parameters = {
                    new SqlParameter("@fchrItemID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrItemName", SqlDbType.NVarChar,255),
                    new SqlParameter("@fchrAddCode", SqlDbType.NVarChar,255),
                    new SqlParameter("@fbitNoUsed", SqlDbType.Int,4),
                    new SqlParameter("@fchrItemTypeID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrAccountID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrBarCode", SqlDbType.NVarChar,60),
                    new SqlParameter("@fbitDiscount", SqlDbType.Bit,1),
                    new SqlParameter("@fbitSpecial", SqlDbType.Bit,1),
                    new SqlParameter("@fbitValidDay", SqlDbType.Bit,1),
                    new SqlParameter("@fintValidDays", SqlDbType.Int,4),
                    new SqlParameter("@fbitBatch", SqlDbType.Bit,1),
                    new SqlParameter("@fbitSerialNo", SqlDbType.Bit,1),
                    new SqlParameter("@fintSerialNoLen", SqlDbType.Int,4),
                    new SqlParameter("@fchrProducingArea", SqlDbType.NVarChar,100),
                    new SqlParameter("@fbitOTC", SqlDbType.Bit,1),
                    new SqlParameter("@fchrLevel", SqlDbType.NVarChar,50),
                    new SqlParameter("@fbitFree1", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree2", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree3", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree4", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree5", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree6", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree7", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree8", SqlDbType.Bit,1),
                    new SqlParameter("@fdtmLastModifyDate", SqlDbType.DateTime),
                    new SqlParameter("@fchrItemCode", SqlDbType.NVarChar,255),
                    new SqlParameter("@fchrUnitID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fbitExport", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree9", SqlDbType.Bit,1),
                    new SqlParameter("@fbitFree10", SqlDbType.Bit,1),
                    new SqlParameter("@fchrUnit", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@fintUnitDecimalDigits", SqlDbType.Int,4),
                    new SqlParameter("@fchrTimeStamp", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrPKID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@varInvDefine1", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotRefCostPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@fbitPriceCanBeModify", SqlDbType.Bit,1),
                    new SqlParameter("@fchrItemTypeCode", SqlDbType.NVarChar,30),
                    new SqlParameter("@fbitVIP", SqlDbType.Bit,1),
                    new SqlParameter("@fbitWholeFavorable", SqlDbType.Bit,1),
                    new SqlParameter("@fchrSpec", SqlDbType.NVarChar,255),
                    new SqlParameter("@fchrYear", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrQuarter", SqlDbType.NVarChar,50),
                    new SqlParameter("@fbitCanSale", SqlDbType.Bit,1),
                    new SqlParameter("@fchrItemCDef1", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef2", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef3", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef4", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef5", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef6", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef7", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef8", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef9", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef10", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef11", SqlDbType.Int,4),
                    new SqlParameter("@fchrItemCDef12", SqlDbType.Int,4),
                    new SqlParameter("@fchrItemCDef13", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrItemCDef14", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrItemCDef15", SqlDbType.DateTime),
                    new SqlParameter("@fchrItemCDef16", SqlDbType.DateTime),
                    new SqlParameter("@fbitIsGiftToken", SqlDbType.Bit,1),
                    new SqlParameter("@fbitInTrunk", SqlDbType.Bit,1),
                    new SqlParameter("@fintBackRule", SqlDbType.Int,4),
                    new SqlParameter("@fintProcess", SqlDbType.Int,4),
                    new SqlParameter("@fbitIsStoredCard", SqlDbType.Bit,1),
                    new SqlParameter("@fdtmGiftInvalidate", SqlDbType.NVarChar,10),
                    new SqlParameter("@ftinItemModel", SqlDbType.Int,4),
                    new SqlParameter("@fbitInstantSale", SqlDbType.Bit,1),
                    new SqlParameter("@fbitStopNeedItem", SqlDbType.Bit,1),
                    new SqlParameter("@fchrAnaSql", SqlDbType.NText),
                    new SqlParameter("@fchrSql", SqlDbType.NText),
                    new SqlParameter("@fchrSqlText", SqlDbType.NText),
                    new SqlParameter("@fdtmStopDate", SqlDbType.DateTime),
                    new SqlParameter("@fchrDepositUnitID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrGtCode", SqlDbType.NVarChar,50),
                    new SqlParameter("@fbitElecGift", SqlDbType.Bit,1),
                    new SqlParameter("@fbitTransfer", SqlDbType.Bit,1),
                    new SqlParameter("@fchrPrintTypeID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fbitIngredient", SqlDbType.Bit,1),
                    new SqlParameter("@fbitUE", SqlDbType.Bit,1),
                    new SqlParameter("@fintSaleType", SqlDbType.Int,4)};
            parameters[0].Value = row["fchrItemID"];
            parameters[1].Value = row["fchrItemName"];
            parameters[2].Value = row["fchrAddCode"];
            parameters[3].Value = row["fbitNoUsed"];
            parameters[4].Value = row["fchrItemTypeID"];
            parameters[5].Value = row["fchrAccountID"];
            parameters[6].Value = row["fchrBarCode"];
            parameters[7].Value = row["fbitDiscount"];
            parameters[8].Value = row["fbitSpecial"];
            parameters[9].Value = row["fbitValidDay"];
            parameters[10].Value = row["fintValidDays"];
            parameters[11].Value = row["fbitBatch"];
            parameters[12].Value = row["fbitSerialNo"];
            parameters[13].Value = row["fintSerialNoLen"];
            parameters[14].Value = row["fchrProducingArea"];
            parameters[15].Value = row["fbitOTC"];
            parameters[16].Value = row["fchrLevel"];
            parameters[17].Value = row["fbitFree1"];
            parameters[18].Value = row["fbitFree2"];
            parameters[19].Value = row["fbitFree3"];
            parameters[20].Value = row["fbitFree4"];
            parameters[21].Value = row["fbitFree5"];
            parameters[22].Value = row["fbitFree6"];
            parameters[23].Value = row["fbitFree7"];
            parameters[24].Value = row["fbitFree8"];
            parameters[25].Value = row["fdtmLastModifyDate"];
            parameters[26].Value = row["fchrItemCode"];
            parameters[27].Value = row["fchrUnitID"];
            parameters[28].Value = row["fbitExport"];
            parameters[29].Value = row["fbitFree9"];
            parameters[30].Value = row["fbitFree10"];
            parameters[31].Value = row["fchrUnit"];
            parameters[32].Value = row["flotPrice"];
            parameters[33].Value = row["fintUnitDecimalDigits"];
            parameters[34].Value = row["fchrTimeStamp"];
            parameters[35].Value = row["fchrPKID"];
            parameters[36].Value = row["varInvDefine1"];
            parameters[37].Value = row["flotRefCostPrice"];
            parameters[38].Value = row["fbitPriceCanBeModify"];
            parameters[39].Value = row["fchrItemTypeCode"];
            parameters[40].Value = row["fbitVIP"];
            parameters[41].Value = row["fbitWholeFavorable"];
            parameters[42].Value = row["fchrSpec"];
            parameters[43].Value = row["fchrYear"];
            parameters[44].Value = row["fchrQuarter"];
            parameters[45].Value = row["fbitCanSale"];
            parameters[46].Value = row["fchrItemCDef1"];
            parameters[47].Value = row["fchrItemCDef2"];
            parameters[48].Value = row["fchrItemCDef3"];
            parameters[49].Value = row["fchrItemCDef4"];
            parameters[50].Value = row["fchrItemCDef5"];
            parameters[51].Value = row["fchrItemCDef6"];
            parameters[52].Value = row["fchrItemCDef7"];
            parameters[53].Value = row["fchrItemCDef8"];
            parameters[54].Value = row["fchrItemCDef9"];
            parameters[55].Value = row["fchrItemCDef10"];
            parameters[56].Value = row["fchrItemCDef11"];
            parameters[57].Value = row["fchrItemCDef12"];
            parameters[58].Value = row["fchrItemCDef13"];
            parameters[59].Value = row["fchrItemCDef14"];
            parameters[60].Value = row["fchrItemCDef15"];
            parameters[61].Value = row["fchrItemCDef16"];
            parameters[62].Value = row["fbitIsGiftToken"];
            parameters[63].Value = row["fbitInTrunk"];
            parameters[64].Value = row["fintBackRule"];
            parameters[65].Value = row["fintProcess"];
            parameters[66].Value = row["fbitIsStoredCard"];
            parameters[67].Value = row["fdtmGiftInvalidate"];
            parameters[68].Value = row["ftinItemModel"];
            parameters[69].Value = row["fbitInstantSale"];
            parameters[70].Value = row["fbitStopNeedItem"];
            parameters[71].Value = row["fchrAnaSql"];
            parameters[72].Value = row["fchrSql"];
            parameters[73].Value = row["fchrSqlText"];
            parameters[74].Value = row["fdtmStopDate"];
            parameters[75].Value = row["fchrDepositUnitID"];
            parameters[76].Value = row["fchrGtCode"];
            parameters[77].Value = row["fbitElecGift"];
            parameters[78].Value = row["fbitTransfer"];
            parameters[79].Value = row["fchrPrintTypeID"];
            parameters[80].Value = row["fbitIngredient"];
            parameters[81].Value = row["fbitUE"];
            parameters[82].Value = row["fintSaleType"];
            return DbHelperSQL.ExecuteSql(strSql.ToString(), parameters) > 0;
        }
        #endregion

        #region 门店数据同步语句块  朱旺 2018.4.23 add
        /// <summary>
        /// 往门店表插入数据(BLL)  朱旺  2018.4.23 add
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private object AddStoreDefine(object obj)
        {
            DataTable dataTable = GetStoreDefine(int.Parse(obj.ToString())).Tables[0];
            foreach (DataRow item in dataTable.Rows)
            {
                AddStoreDefine(item);
            }
            return 0;
        }
        /// <summary>
        /// 获取门店表dataset 朱旺 2018.4.23 add
        /// </summary>
        /// <param name="Page"></param>
        /// <returns></returns>
        private DataSet GetStoreDefine(int Page)
        {
            StringBuilder Sql = new StringBuilder();
            Sql.AppendFormat("SELECT * from StoreDefine  ORDER BY fchrStoreCode OFFSET　{0} ROW  FETCH NEXT {1} ROW ONLY", (Page - 1) * Row, Row);
            DataSet ds = DbHelperSQL.Query(DbHelperSQL.u8connectionString, Sql.ToString());
            return ds;
        }
        /// <summary>
        /// 往门店表插入数据(DAL)  朱旺  2018.4.23 add
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private bool AddStoreDefine(DataRow row)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.Append("insert into StoreDefine(");
            strSql.Append("fchrStoreCode,fchrStoreName,fchrStoreType,fchrCusomerID,fchrDepartmetID,fchrWarehouseID,fchrAccountID,flotDiscountRate,flotDiscount,fbitAccountStock,fbitManagePerson,fdtmStartDate,fchrStoreUserDefine,fbitExport,fchrStoreDefineID,fchrDirectory,fchrExchangeID,fchrCusReceiveAddressID,fchrCustomerReceiveAddress,fchrPostCode,fchrLinkman,fchrLinkphone,fchrFax,fchrPhone,fchrAddress,fchrUseDiscount,fbitUsed,fbitUseRetailSystem,fchrRegisterKey,fchrTimeStamp,fchrStorePriceManage,fdtmLastModifyTime,fbitStoreDataAutoDelete,fintStoreDataDayNum,fbitSupportOnline,fintIntervalTime,fchrIpAddress,fchrDBName,fintNetStyle,fdtmLastModified,flotDiscountRateHighLimit,fbitEmployeeTransferred,fchrStoreArea,fchrStorePeopleCount,fchrItemCDef1,fchrItemCDef2,fchrItemCDef3,fchrItemCDef4,fchrItemCDef5,fchrItemCDef6,fchrItemCDef7,fchrItemCDef8,fchrItemCDef9,fchrItemCDef10,fchrAreaID,fintOperatorManage,fchrBrandID,fbitOnLine,fbitWhPos,fchrDefaultInPosID,fchrDefaultOutPosID,fchrSendWarehouseID,fchrSendOrgID,fchrOnlyCode,fbitLinkCode,fchrMachineCode,fintAllOrg,fbitManageBatchAndValid,fintWhPosBySelf,fbitPickUpSelf,fchrEarthPosition)");
            strSql.Append(" values (");
            strSql.Append("@fchrStoreCode,@fchrStoreName,@fchrStoreType,@fchrCusomerID,@fchrDepartmetID,@fchrWarehouseID,@fchrAccountID,@flotDiscountRate,@flotDiscount,@fbitAccountStock,@fbitManagePerson,@fdtmStartDate,@fchrStoreUserDefine,@fbitExport,@fchrStoreDefineID,@fchrDirectory,@fchrExchangeID,@fchrCusReceiveAddressID,@fchrCustomerReceiveAddress,@fchrPostCode,@fchrLinkman,@fchrLinkphone,@fchrFax,@fchrPhone,@fchrAddress,@fchrUseDiscount,@fbitUsed,@fbitUseRetailSystem,@fchrRegisterKey,@fchrTimeStamp,@fchrStorePriceManage,@fdtmLastModifyTime,@fbitStoreDataAutoDelete,@fintStoreDataDayNum,@fbitSupportOnline,@fintIntervalTime,@fchrIpAddress,@fchrDBName,@fintNetStyle,@fdtmLastModified,@flotDiscountRateHighLimit,@fbitEmployeeTransferred,@fchrStoreArea,@fchrStorePeopleCount,@fchrItemCDef1,@fchrItemCDef2,@fchrItemCDef3,@fchrItemCDef4,@fchrItemCDef5,@fchrItemCDef6,@fchrItemCDef7,@fchrItemCDef8,@fchrItemCDef9,@fchrItemCDef10,@fchrAreaID,@fintOperatorManage,@fchrBrandID,@fbitOnLine,@fbitWhPos,@fchrDefaultInPosID,@fchrDefaultOutPosID,@fchrSendWarehouseID,@fchrSendOrgID,@fchrOnlyCode,@fbitLinkCode,@fchrMachineCode,@fintAllOrg,@fbitManageBatchAndValid,@fintWhPosBySelf,@fbitPickUpSelf,@fchrEarthPosition)");
            SqlParameter[] parameters = {
                    new SqlParameter("@fchrStoreCode", SqlDbType.NVarChar,10),
                    new SqlParameter("@fchrStoreName", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrStoreType", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrCusomerID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrDepartmetID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrWarehouseID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrAccountID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@flotDiscountRate", SqlDbType.Decimal,13),
                    new SqlParameter("@flotDiscount", SqlDbType.Decimal,13),
                    new SqlParameter("@fbitAccountStock", SqlDbType.Bit,1),
                    new SqlParameter("@fbitManagePerson", SqlDbType.Bit,1),
                    new SqlParameter("@fdtmStartDate", SqlDbType.SmallDateTime),
                    new SqlParameter("@fchrStoreUserDefine", SqlDbType.NVarChar,60),
                    new SqlParameter("@fbitExport", SqlDbType.Bit,1),
                    new SqlParameter("@fchrStoreDefineID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrDirectory", SqlDbType.NVarChar,40),
                    new SqlParameter("@fchrExchangeID", SqlDbType.NVarChar,40),
                    new SqlParameter("@fchrCusReceiveAddressID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrCustomerReceiveAddress", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrPostCode", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrLinkman", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrLinkphone", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrFax", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrPhone", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrAddress", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrUseDiscount", SqlDbType.Bit,1),
                    new SqlParameter("@fbitUsed", SqlDbType.Bit,1),
                    new SqlParameter("@fbitUseRetailSystem", SqlDbType.Bit,1),
                    new SqlParameter("@fchrRegisterKey", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrTimeStamp", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrStorePriceManage", SqlDbType.Char,1),
                    new SqlParameter("@fdtmLastModifyTime", SqlDbType.SmallDateTime),
                    new SqlParameter("@fbitStoreDataAutoDelete", SqlDbType.Bit,1),
                    new SqlParameter("@fintStoreDataDayNum", SqlDbType.Int,4),
                    new SqlParameter("@fbitSupportOnline", SqlDbType.Bit,1),
                    new SqlParameter("@fintIntervalTime", SqlDbType.TinyInt,1),
                    new SqlParameter("@fchrIpAddress", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrDBName", SqlDbType.NVarChar,50),
                    new SqlParameter("@fintNetStyle", SqlDbType.TinyInt,1),
                    new SqlParameter("@fdtmLastModified", SqlDbType.SmallDateTime),
                    new SqlParameter("@flotDiscountRateHighLimit", SqlDbType.Decimal,13),
                    new SqlParameter("@fbitEmployeeTransferred", SqlDbType.Bit,1),
                    new SqlParameter("@fchrStoreArea", SqlDbType.Float,8),
                    new SqlParameter("@fchrStorePeopleCount", SqlDbType.Float,8),
                    new SqlParameter("@fchrItemCDef1", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef2", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef3", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef4", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef5", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef6", SqlDbType.NVarChar,200),
                    new SqlParameter("@fchrItemCDef7", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrItemCDef8", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrItemCDef9", SqlDbType.SmallDateTime),
                    new SqlParameter("@fchrItemCDef10", SqlDbType.SmallDateTime),
                    new SqlParameter("@fchrAreaID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fintOperatorManage", SqlDbType.TinyInt,1),
                    new SqlParameter("@fchrBrandID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fbitOnLine", SqlDbType.Bit,1),
                    new SqlParameter("@fbitWhPos", SqlDbType.Bit,1),
                    new SqlParameter("@fchrDefaultInPosID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrDefaultOutPosID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrSendWarehouseID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrSendOrgID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrOnlyCode", SqlDbType.NVarChar,200),
                    new SqlParameter("@fbitLinkCode", SqlDbType.Bit,1),
                    new SqlParameter("@fchrMachineCode", SqlDbType.NVarChar,100),
                    new SqlParameter("@fintAllOrg", SqlDbType.Int,4),
                    new SqlParameter("@fbitManageBatchAndValid", SqlDbType.Bit,1),
                    new SqlParameter("@fintWhPosBySelf", SqlDbType.TinyInt,1),
                    new SqlParameter("@fbitPickUpSelf", SqlDbType.Bit,1),
                    new SqlParameter("@fchrEarthPosition", SqlDbType.NVarChar,50)};
            parameters[0].Value = row["fchrStoreCode"];
            parameters[1].Value = row["fchrStoreName"];
            parameters[2].Value = row["fchrStoreType"];
            parameters[3].Value = row["fchrCusomerID"];
            parameters[4].Value = row["fchrDepartmetID"];
            parameters[5].Value = row["fchrWarehouseID"];
            parameters[6].Value = row["fchrAccountID"];
            parameters[7].Value = row["flotDiscountRate"];
            parameters[8].Value = row["flotDiscount"];
            parameters[9].Value = row["fbitAccountStock"];
            parameters[10].Value = row["fbitManagePerson"];
            parameters[11].Value = row["fdtmStartDate"];
            parameters[12].Value = row["fchrStoreUserDefine"];
            parameters[13].Value = row["fbitExport"];
            parameters[14].Value = row["fchrStoreDefineID"];
            parameters[15].Value = row["fchrDirectory"];
            parameters[16].Value = row["fchrExchangeID"];
            parameters[17].Value = row["fchrCusReceiveAddressID"];
            parameters[18].Value = row["fchrCustomerReceiveAddress"];
            parameters[19].Value = row["fchrPostCode"];
            parameters[20].Value = row["fchrLinkman"];
            parameters[21].Value = row["fchrLinkphone"];
            parameters[22].Value = row["fchrFax"];
            parameters[23].Value = row["fchrPhone"];
            parameters[24].Value = row["fchrAddress"];
            parameters[25].Value = row["fchrUseDiscount"];
            parameters[26].Value = row["fbitUsed"];
            parameters[27].Value = row["fbitUseRetailSystem"];
            parameters[28].Value = row["fchrRegisterKey"];
            parameters[29].Value = row["fchrTimeStamp"];
            parameters[30].Value = row["fchrStorePriceManage"];
            parameters[31].Value = row["fdtmLastModifyTime"];
            parameters[32].Value = row["fbitStoreDataAutoDelete"];
            parameters[33].Value = row["fintStoreDataDayNum"];
            parameters[34].Value = row["fbitSupportOnline"];
            parameters[35].Value = row["fintIntervalTime"];
            parameters[36].Value = row["fchrIpAddress"];
            parameters[37].Value = row["fchrDBName"];
            parameters[38].Value = row["fintNetStyle"];
            parameters[39].Value = row["fdtmLastModified"];
            parameters[40].Value = row["flotDiscountRateHighLimit"];
            parameters[41].Value = row["fbitEmployeeTransferred"];
            parameters[42].Value = row["fchrStoreArea"];
            parameters[43].Value = row["fchrStorePeopleCount"];
            parameters[44].Value = row["fchrItemCDef1"];
            parameters[45].Value = row["fchrItemCDef2"];
            parameters[46].Value = row["fchrItemCDef3"];
            parameters[47].Value = row["fchrItemCDef4"];
            parameters[48].Value = row["fchrItemCDef5"];
            parameters[49].Value = row["fchrItemCDef6"];
            parameters[50].Value = row["fchrItemCDef7"];
            parameters[51].Value = row["fchrItemCDef8"];
            parameters[52].Value = row["fchrItemCDef9"];
            parameters[53].Value = row["fchrItemCDef10"];
            parameters[54].Value = row["fchrAreaID"];
            parameters[55].Value = row["fintOperatorManage"];
            parameters[56].Value = row["fchrBrandID"];
            parameters[57].Value = row["fbitOnLine"];
            parameters[58].Value = row["fbitWhPos"];
            parameters[59].Value = row["fchrDefaultInPosID"];
            parameters[60].Value = row["fchrDefaultOutPosID"];
            parameters[61].Value = row["fchrSendWarehouseID"];
            parameters[62].Value = row["fchrSendOrgID"];
            parameters[63].Value = row["fchrOnlyCode"];
            parameters[64].Value = row["fbitLinkCode"];
            parameters[65].Value = row["fchrMachineCode"];
            parameters[66].Value = row["fintAllOrg"];
            parameters[67].Value = row["fbitManageBatchAndValid"];
            parameters[68].Value = row["fintWhPosBySelf"];
            parameters[69].Value = row["fbitPickUpSelf"];
            parameters[70].Value = row["fchrEarthPosition"];
            return DbHelperSQL.ExecuteSql(strSql.ToString(), parameters) > 0;
        }
    }
    #endregion
}


