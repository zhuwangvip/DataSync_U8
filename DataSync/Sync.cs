using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amib.Threading;
using DataSync.DBUtility;

namespace DataSync
{
    class Sync
    {
        private string TableName = string.Empty;
        private string TimeName = string.Empty;
        private delegate object Delegate(object row); // 定义一个委托
        private Delegate @delegate;

        public Sync() { }
        public Sync(string _Table)
        {
            TableName = _Table;
            if (TableName == "RetailVouch")
            {
                TimeName = "fdtmAddTime";
                @delegate = AddRetailVouchData;
            }
            if (TableName == "RetailVouchDetail")
            {
                TimeName = "fdtmOldSellDate";
                @delegate = AddRetailVouchDetailData;
            }
        }

        /// <summary>
        /// 主订单和子订单程序入口 朱旺  2018.4.19 add
        /// </summary>
        public void SyncData()
        {
            if (!string.IsNullOrEmpty(TableName))
            {
                string EndTime = GetEndTime();
                DataTable dataTable = GetDataSet(EndTime).Tables[0];
                using (SmartThreadPool smartThreadPool = new SmartThreadPool())
                {
                    List<IWorkItemResult> wirs = new List<IWorkItemResult>();
                    int count = 0;
                    foreach (DataRow item in dataTable.Rows)
                    {
                        if (count == 500)
                        {
                            SmartThreadPool.WaitAll(wirs.ToArray());
                            wirs.RemoveAll(it => true);
                            count = 0;
                        }
                        IWorkItemResult wir = smartThreadPool.QueueWorkItem(new WorkItemCallback(@delegate), item);
                        wirs.Add(wir);
                        count++;
                    }
                }
            }
        }

        /// <summary>
        /// 获取本地服务器上某表的最后一条数据 朱旺 2018.4.19 add
        /// </summary>
        /// <returns></returns>
        private string GetEndTime()
        {
            StringBuilder Sql = new StringBuilder();
            Sql.AppendFormat("select  max({0}) as MaxTime from {1}", TimeName, TableName);
            object TimeObj = DbHelperSQL.GetSingle(Sql.ToString());
            if (TimeObj == null)
            {
                return "";
            }
            else
            {
                return DateTime.Parse(TimeObj.ToString()).ToString("yyyy-MM-dd HH:mm:ss.fff");
            }
        }

        /// <summary>
        /// 获取U8数据（排除本地服务器有的数据）  朱旺  2018.4.19 
        /// </summary>
        /// <param name="EndTime"></param>
        /// <returns></returns>
        private DataSet GetDataSet(string EndTime)
        {
            StringBuilder Sql = new StringBuilder();
            if (TableName == "RetailVouchDetail")
            {
                Sql.Append(" select * from (select(select fdtmAddTime from RetailVouch where fchrRetailVouchID = a.fchrRetailVouchID) as fdtmAddTime2, *from RetailVouchDetail as a");
                Sql.AppendFormat(" ) as b where b.fdtmAddTime2 > '{0}'  order by b.fdtmAddTime2", EndTime);
            }
            else
            {
                Sql.AppendFormat("select * from {0} where {1}>'{2}'  order by {1}", TableName, TimeName, EndTime);
            }
            DataSet ds = DbHelperSQL.Query(DbHelperSQL.u8connectionString, Sql.ToString());
            return ds;
        }

        /// <summary>
        /// 对订单主表插入数据  朱旺 2018.4.19 add
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private object AddRetailVouchData(object obj)
        {
            DataRow row = obj as DataRow;
            StringBuilder strSql = new StringBuilder();
            strSql.Append("insert into [RetailVouch](");
            strSql.Append("[fchrAccountID],[fchrStoreDefineID],[fchrRetailVouchID],[fchrDepartmentID],[fdtmDate],[fchrSquadID],[fchrSquadName],[fchrBackID],[fchrRetailReportID],[fchrVerifier],[fbitExport],[fchrCode],[fchrDiscountCardCode],[fchrMaker],[fchrGatheringVouchID],[flotGatheringMoney],[fchrCoRetailVouchID],[flotDiscountCardRate],[fchrWholeDiscountCode],[flotWholeDiscountRate],[flotWholeDiscount],[fnarvipcardcode],[fdecvipdiscountrate],[flotEffaceMoney],[fchrSaleTypeCode],[fchrRetailVouchRelatingID],[fchrPromotionPloyID],[fbitSettle],[flotCurrentPoint],[flotPointBalance],[fchrAddPointRuleID],[fchrVIPCardClassID],[fchrVipConsumerID],[fchrPromotionPloyIDList],[fdtmPlanShipmentDate],[fdtmShipmentDate],[fchrRemark],[flotVipDiscountMoneySum],[flotDiscountCardMoneySum],[flotPromotionPloyMoneySum],[flotLocaleDiscountSum],[fbitCalcVIP],[flotSaleMoney],[flotMoneySum],[flotQuantitySum],[flotDiscountSum],[ftinPreSellType],[fchrShipmentStoreID],[ftinPreDownload],[ftinShipDownload],[fchrPreSellStoreID],[fbitRestore],[fbitAllowGiftToken],[fbitAllowStoreCard],[fchrOweCustID],[fdtmPointPay],[flotPointPay],[flotPointPayMoney],[fchrSaleTypeID],[fchrUH1],[fchrUH2],[fchrUH3],[fdtmUH4],[fintUH5],[fdtmUH6],[flotUH7],[fchrUH8],[fchrUH9],[fchrUH10],[fchrUH11],[fchrUH12],[fchrUH13],[fchrUH14],[fintUH15],[flotUH16],[fchrdeliveradd],[fchrcusperson],[fchrCenterDeliveryID],[fchrCenterDOrgID],[fbitSN],[fchrPostalCode],[fchrMobileNo],[fchrECTradeCode],[fchrECOrderCode],[fdtmECTrade],[fintOID],[fchrExpressCode],[fdtmAddTime])");
            strSql.Append(" values (");
            strSql.Append("@fchrAccountID,@fchrStoreDefineID,@fchrRetailVouchID,@fchrDepartmentID,@fdtmDate,@fchrSquadID,@fchrSquadName,@fchrBackID,@fchrRetailReportID,@fchrVerifier,@fbitExport,@fchrCode,@fchrDiscountCardCode,@fchrMaker,@fchrGatheringVouchID,@flotGatheringMoney,@fchrCoRetailVouchID,@flotDiscountCardRate,@fchrWholeDiscountCode,@flotWholeDiscountRate,@flotWholeDiscount,@fnarvipcardcode,@fdecvipdiscountrate,@flotEffaceMoney,@fchrSaleTypeCode,@fchrRetailVouchRelatingID,@fchrPromotionPloyID,@fbitSettle,@flotCurrentPoint,@flotPointBalance,@fchrAddPointRuleID,@fchrVIPCardClassID,@fchrVipConsumerID,@fchrPromotionPloyIDList,@fdtmPlanShipmentDate,@fdtmShipmentDate,@fchrRemark,@flotVipDiscountMoneySum,@flotDiscountCardMoneySum,@flotPromotionPloyMoneySum,@flotLocaleDiscountSum,@fbitCalcVIP,@flotSaleMoney,@flotMoneySum,@flotQuantitySum,@flotDiscountSum,@ftinPreSellType,@fchrShipmentStoreID,@ftinPreDownload,@ftinShipDownload,@fchrPreSellStoreID,@fbitRestore,@fbitAllowGiftToken,@fbitAllowStoreCard,@fchrOweCustID,@fdtmPointPay,@flotPointPay,@flotPointPayMoney,@fchrSaleTypeID,@fchrUH1,@fchrUH2,@fchrUH3,@fdtmUH4,@fintUH5,@fdtmUH6,@flotUH7,@fchrUH8,@fchrUH9,@fchrUH10,@fchrUH11,@fchrUH12,@fchrUH13,@fchrUH14,@fintUH15,@flotUH16,@fchrdeliveradd,@fchrcusperson,@fchrCenterDeliveryID,@fchrCenterDOrgID,@fbitSN,@fchrPostalCode,@fchrMobileNo,@fchrECTradeCode,@fchrECOrderCode,@fdtmECTrade,@fintOID,@fchrExpressCode,@fdtmAddTime)");
            SqlParameter[] parameters = {
                    new SqlParameter("@fchrAccountID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrStoreDefineID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrRetailVouchID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrDepartmentID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fdtmDate", SqlDbType.DateTime),
                    new SqlParameter("@fchrSquadID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrSquadName", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrBackID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrRetailReportID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrVerifier", SqlDbType.NVarChar,50),
                    new SqlParameter("@fbitExport", SqlDbType.Bit),
                    new SqlParameter("@fchrCode", SqlDbType.NVarChar,30),
                    new SqlParameter("@fchrDiscountCardCode", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrMaker", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrGatheringVouchID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@flotGatheringMoney", SqlDbType.Decimal),
                    new SqlParameter("@fchrCoRetailVouchID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@flotDiscountCardRate", SqlDbType.Decimal),
                    new SqlParameter("@fchrWholeDiscountCode", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotWholeDiscountRate", SqlDbType.Decimal),
                    new SqlParameter("@flotWholeDiscount", SqlDbType.Decimal),
                    new SqlParameter("@fnarvipcardcode", SqlDbType.NVarChar,30),
                    new SqlParameter("@fdecvipdiscountrate", SqlDbType.Decimal),
                    new SqlParameter("@flotEffaceMoney", SqlDbType.Decimal),
                    new SqlParameter("@fchrSaleTypeCode", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrRetailVouchRelatingID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrPromotionPloyID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fbitSettle", SqlDbType.Bit),
                    new SqlParameter("@flotCurrentPoint", SqlDbType.Decimal),
                    new SqlParameter("@flotPointBalance", SqlDbType.Decimal),
                    new SqlParameter("@fchrAddPointRuleID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrVIPCardClassID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrVipConsumerID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrPromotionPloyIDList", SqlDbType.NVarChar,800),
                    new SqlParameter("@fdtmPlanShipmentDate", SqlDbType.DateTime),
                    new SqlParameter("@fdtmShipmentDate", SqlDbType.DateTime),
                    new SqlParameter("@fchrRemark", SqlDbType.NVarChar,200),
                    new SqlParameter("@flotVipDiscountMoneySum", SqlDbType.Decimal),
                    new SqlParameter("@flotDiscountCardMoneySum", SqlDbType.Decimal),
                    new SqlParameter("@flotPromotionPloyMoneySum", SqlDbType.Decimal),
                    new SqlParameter("@flotLocaleDiscountSum", SqlDbType.Decimal),
                    new SqlParameter("@fbitCalcVIP", SqlDbType.Int),
                    new SqlParameter("@flotSaleMoney", SqlDbType.Decimal),
                    new SqlParameter("@flotMoneySum", SqlDbType.Float),
                    new SqlParameter("@flotQuantitySum", SqlDbType.Float),
                    new SqlParameter("@flotDiscountSum", SqlDbType.Decimal),
                    new SqlParameter("@ftinPreSellType", SqlDbType.Int),
                    new SqlParameter("@fchrShipmentStoreID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@ftinPreDownload", SqlDbType.Int),
                    new SqlParameter("@ftinShipDownload", SqlDbType.Int),
                    new SqlParameter("@fchrPreSellStoreID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fbitRestore", SqlDbType.Bit),
                    new SqlParameter("@fbitAllowGiftToken", SqlDbType.Bit),
                    new SqlParameter("@fbitAllowStoreCard", SqlDbType.Bit),
                    new SqlParameter("@fchrOweCustID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fdtmPointPay", SqlDbType.DateTime),
                    new SqlParameter("@flotPointPay", SqlDbType.Decimal),
                    new SqlParameter("@flotPointPayMoney", SqlDbType.Decimal),
                    new SqlParameter("@fchrSaleTypeID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrUH1", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrUH2", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrUH3", SqlDbType.NVarChar,20),
                    new SqlParameter("@fdtmUH4", SqlDbType.DateTime),
                    new SqlParameter("@fintUH5", SqlDbType.Int),
                    new SqlParameter("@fdtmUH6", SqlDbType.DateTime),
                    new SqlParameter("@flotUH7", SqlDbType.Decimal),
                    new SqlParameter("@fchrUH8", SqlDbType.NVarChar,4),
                    new SqlParameter("@fchrUH9", SqlDbType.NVarChar,8),
                    new SqlParameter("@fchrUH10", SqlDbType.NVarChar,60),
                    new SqlParameter("@fchrUH11", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUH12", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUH13", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUH14", SqlDbType.NVarChar,120),
                    new SqlParameter("@fintUH15", SqlDbType.Int),
                    new SqlParameter("@flotUH16", SqlDbType.Decimal),
                    new SqlParameter("@fchrdeliveradd", SqlDbType.NVarChar,255),
                    new SqlParameter("@fchrcusperson", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrCenterDeliveryID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fchrCenterDOrgID", SqlDbType.UniqueIdentifier),
                    new SqlParameter("@fbitSN", SqlDbType.Bit),
                    new SqlParameter("@fchrPostalCode", SqlDbType.NVarChar,20),
                    new SqlParameter("@fchrMobileNo", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrECTradeCode", SqlDbType.NVarChar,100),
                    new SqlParameter("@fchrECOrderCode", SqlDbType.VarChar,50),
                    new SqlParameter("@fdtmECTrade", SqlDbType.DateTime),
                    new SqlParameter("@fintOID", SqlDbType.BigInt),
                    new SqlParameter("@fchrExpressCode", SqlDbType.VarChar,50),
                     new SqlParameter("@fdtmAddTime", SqlDbType.DateTime)
            };
            parameters[0].Value = row["fchrAccountID"];
            parameters[1].Value = row["fchrStoreDefineID"];
            parameters[2].Value = row["fchrRetailVouchID"];

            if (row["fchrDepartmentID"] != null && !string.IsNullOrEmpty(row["fchrDepartmentID"].ToString()) && Guid.Parse(row["fchrDepartmentID"].ToString()) != Guid.Empty)
                parameters[3].Value = row["fchrDepartmentID"];
            else
                parameters[3].Value = DBNull.Value;


            if (row["fdtmDate"] != null && !string.IsNullOrEmpty(row["fdtmDate"].ToString()) && DateTime.Parse(row["fdtmDate"].ToString()) != DateTime.MinValue)
                parameters[4].Value = row["fdtmDate"];
            else
                parameters[4].Value = DBNull.Value;


            if (row["fchrSquadID"] != null && !string.IsNullOrEmpty(row["fchrSquadID"].ToString()) && Guid.Parse(row["fchrSquadID"].ToString()) != Guid.Empty)
                parameters[5].Value = row["fchrSquadID"];
            else
                parameters[5].Value = DBNull.Value;


            if (row["fchrSquadName"] != null)
                parameters[6].Value = row["fchrSquadName"];
            else
                parameters[6].Value = DBNull.Value;


            if (row["fchrBackID"] != null && !string.IsNullOrEmpty(row["fchrBackID"].ToString()) && Guid.Parse(row["fchrBackID"].ToString()) != Guid.Empty)
                parameters[7].Value = row["fchrBackID"];
            else
                parameters[7].Value = DBNull.Value;


            if (row["fchrRetailReportID"] != null && !string.IsNullOrEmpty(row["fchrRetailReportID"].ToString()) && Guid.Parse(row["fchrRetailReportID"].ToString()) != Guid.Empty)
                parameters[8].Value = row["fchrRetailReportID"];
            else
                parameters[8].Value = DBNull.Value;


            if (row["fchrVerifier"] != null)
                parameters[9].Value = row["fchrVerifier"];
            else
                parameters[9].Value = DBNull.Value;

            parameters[10].Value = row["fbitExport"];

            if (row["fchrCode"] != null)
                parameters[11].Value = row["fchrCode"];
            else
                parameters[11].Value = DBNull.Value;


            if (row["fchrDiscountCardCode"] != null)
                parameters[12].Value = row["fchrDiscountCardCode"];
            else
                parameters[12].Value = DBNull.Value;


            if (row["fchrMaker"] != null)
                parameters[13].Value = row["fchrMaker"];
            else
                parameters[13].Value = DBNull.Value;


            if (row["fchrGatheringVouchID"] != null && !string.IsNullOrEmpty(row["fchrGatheringVouchID"].ToString()) && Guid.Parse(row["fchrGatheringVouchID"].ToString()) != Guid.Empty)
                parameters[14].Value = row["fchrGatheringVouchID"];
            else
                parameters[14].Value = DBNull.Value;

            parameters[15].Value = row["flotGatheringMoney"];

            if (row["fchrCoRetailVouchID"] != null && !string.IsNullOrEmpty(row["fchrCoRetailVouchID"].ToString()) && Guid.Parse(row["fchrCoRetailVouchID"].ToString()) != Guid.Empty)
                parameters[16].Value = row["fchrCoRetailVouchID"];
            else
                parameters[16].Value = DBNull.Value;

            parameters[17].Value = row["flotDiscountCardRate"];

            if (row["fchrWholeDiscountCode"] != null)
                parameters[18].Value = row["fchrWholeDiscountCode"];
            else
                parameters[18].Value = DBNull.Value;

            parameters[19].Value = row["flotWholeDiscountRate"];
            parameters[20].Value = row["flotWholeDiscount"];

            if (row["fnarvipcardcode"] != null)
                parameters[21].Value = row["fnarvipcardcode"];
            else
                parameters[21].Value = DBNull.Value;

            parameters[22].Value = row["fdecvipdiscountrate"];
            parameters[23].Value = row["flotEffaceMoney"];

            if (row["fchrSaleTypeCode"] != null)
                parameters[24].Value = row["fchrSaleTypeCode"];
            else
                parameters[24].Value = DBNull.Value;


            if (row["fchrRetailVouchRelatingID"] != null && !string.IsNullOrEmpty(row["fchrRetailVouchRelatingID"].ToString()) && Guid.Parse(row["fchrRetailVouchRelatingID"].ToString()) != Guid.Empty)
                parameters[25].Value = row["fchrRetailVouchRelatingID"];
            else
                parameters[25].Value = DBNull.Value;


            if (row["fchrPromotionPloyID"] != null && !string.IsNullOrEmpty(row["fchrPromotionPloyID"].ToString()) && Guid.Parse(row["fchrPromotionPloyID"].ToString()) != Guid.Empty)
                parameters[26].Value = row["fchrPromotionPloyID"];
            else
                parameters[26].Value = DBNull.Value;

            parameters[27].Value = row["fbitSettle"];
            parameters[28].Value = row["flotCurrentPoint"];
            parameters[29].Value = row["flotPointBalance"];

            if (row["fchrAddPointRuleID"] != null && !string.IsNullOrEmpty(row["fchrAddPointRuleID"].ToString()) && Guid.Parse(row["fchrAddPointRuleID"].ToString()) != Guid.Empty)
                parameters[30].Value = row["fchrAddPointRuleID"];
            else
                parameters[30].Value = DBNull.Value;


            if (row["fchrVIPCardClassID"] != null && !string.IsNullOrEmpty(row["fchrVIPCardClassID"].ToString()) && Guid.Parse(row["fchrVIPCardClassID"].ToString()) != Guid.Empty)
                parameters[31].Value = row["fchrVIPCardClassID"];
            else
                parameters[31].Value = DBNull.Value;


            if (row["fchrVipConsumerID"] != null && !string.IsNullOrEmpty(row["fchrVipConsumerID"].ToString()) && Guid.Parse(row["fchrVipConsumerID"].ToString()) != Guid.Empty)
                parameters[32].Value = row["fchrVipConsumerID"];
            else
                parameters[32].Value = DBNull.Value;


            if (row["fchrPromotionPloyIDList"] != null)
                parameters[33].Value = row["fchrPromotionPloyIDList"];
            else
                parameters[33].Value = DBNull.Value;


            if (row["fdtmPlanShipmentDate"] != null && !string.IsNullOrEmpty(row["fdtmPlanShipmentDate"].ToString()) && DateTime.Parse(row["fdtmPlanShipmentDate"].ToString()) != DateTime.MinValue)
                parameters[34].Value = row["fdtmPlanShipmentDate"];
            else
                parameters[34].Value = DBNull.Value;


            if (row["fdtmShipmentDate"] != null && !string.IsNullOrEmpty(row["fdtmShipmentDate"].ToString()) && DateTime.Parse(row["fdtmShipmentDate"].ToString()) != DateTime.MinValue)
                parameters[35].Value = row["fdtmShipmentDate"];
            else
                parameters[35].Value = DBNull.Value;


            if (row["fchrRemark"] != null)
                parameters[36].Value = row["fchrRemark"];
            else
                parameters[36].Value = DBNull.Value;

            parameters[37].Value = row["flotVipDiscountMoneySum"];
            parameters[38].Value = row["flotDiscountCardMoneySum"];
            parameters[39].Value = row["flotPromotionPloyMoneySum"];
            parameters[40].Value = row["flotLocaleDiscountSum"];
            parameters[41].Value = row["fbitCalcVIP"];
            parameters[42].Value = row["flotSaleMoney"];
            parameters[43].Value = row["flotMoneySum"];
            parameters[44].Value = row["flotQuantitySum"];
            parameters[45].Value = row["flotDiscountSum"];
            parameters[46].Value = row["ftinPreSellType"];

            if (row["fchrShipmentStoreID"] != null && !string.IsNullOrEmpty(row["fchrShipmentStoreID"].ToString()) && Guid.Parse(row["fchrShipmentStoreID"].ToString()) != Guid.Empty)
                parameters[47].Value = row["fchrShipmentStoreID"];
            else
                parameters[47].Value = DBNull.Value;

            parameters[48].Value = row["ftinPreDownload"];
            parameters[49].Value = row["ftinShipDownload"];

            if (row["fchrPreSellStoreID"] != null && !string.IsNullOrEmpty(row["fchrPreSellStoreID"].ToString()) && Guid.Parse(row["fchrPreSellStoreID"].ToString()) != Guid.Empty)
                parameters[50].Value = row["fchrPreSellStoreID"];
            else
                parameters[50].Value = DBNull.Value;

            parameters[51].Value = row["fbitRestore"];
            parameters[52].Value = row["fbitAllowGiftToken"];
            parameters[53].Value = row["fbitAllowStoreCard"];

            if (row["fchrOweCustID"] != null && !string.IsNullOrEmpty(row["fchrOweCustID"].ToString()) && Guid.Parse(row["fchrOweCustID"].ToString()) != Guid.Empty)
                parameters[54].Value = row["fchrOweCustID"];
            else
                parameters[54].Value = DBNull.Value;


            if (row["fdtmPointPay"] != null && !string.IsNullOrEmpty(row["fdtmPointPay"].ToString()) && DateTime.Parse(row["fdtmPointPay"].ToString()) != DateTime.MinValue)
                parameters[55].Value = row["fdtmPointPay"];
            else
                parameters[55].Value = DBNull.Value;

            parameters[56].Value = row["flotPointPay"];
            parameters[57].Value = row["flotPointPayMoney"];

            if (row["fchrSaleTypeID"] != null && !string.IsNullOrEmpty(row["fchrSaleTypeID"].ToString()) && Guid.Parse(row["fchrSaleTypeID"].ToString()) != Guid.Empty)
                parameters[58].Value = row["fchrSaleTypeID"];
            else
                parameters[58].Value = DBNull.Value;


            if (row["fchrUH1"] != null)
                parameters[59].Value = row["fchrUH1"];
            else
                parameters[59].Value = DBNull.Value;


            if (row["fchrUH2"] != null)
                parameters[60].Value = row["fchrUH2"];
            else
                parameters[60].Value = DBNull.Value;


            if (row["fchrUH3"] != null)
                parameters[61].Value = row["fchrUH3"];
            else
                parameters[61].Value = DBNull.Value;


            if (row["fdtmUH4"] != null && !string.IsNullOrEmpty(row["fdtmUH4"].ToString()) && DateTime.Parse(row["fdtmUH4"].ToString()) != DateTime.MinValue)
                parameters[62].Value = row["fdtmUH4"];
            else
                parameters[62].Value = DBNull.Value;

            parameters[63].Value = row["fintUH5"];

            if (row["fdtmUH6"] != null && !string.IsNullOrEmpty(row["fdtmUH6"].ToString()) && DateTime.Parse(row["fdtmUH6"].ToString()) != DateTime.MinValue)
                parameters[64].Value = row["fdtmUH6"];
            else
                parameters[64].Value = DBNull.Value;

            parameters[65].Value = row["flotUH7"];

            if (row["fchrUH8"] != null)
                parameters[66].Value = row["fchrUH8"];
            else
                parameters[66].Value = DBNull.Value;


            if (row["fchrUH9"] != null)
                parameters[67].Value = row["fchrUH9"];
            else
                parameters[67].Value = DBNull.Value;


            if (row["fchrUH10"] != null)
                parameters[68].Value = row["fchrUH10"];
            else
                parameters[68].Value = DBNull.Value;


            if (row["fchrUH11"] != null)
                parameters[69].Value = row["fchrUH11"];
            else
                parameters[69].Value = DBNull.Value;


            if (row["fchrUH12"] != null)
                parameters[70].Value = row["fchrUH12"];
            else
                parameters[70].Value = DBNull.Value;


            if (row["fchrUH13"] != null)
                parameters[71].Value = row["fchrUH13"];
            else
                parameters[71].Value = DBNull.Value;


            if (row["fchrUH14"] != null)
                parameters[72].Value = row["fchrUH14"];
            else
                parameters[72].Value = DBNull.Value;

            parameters[73].Value = row["fintUH15"];
            parameters[74].Value = row["flotUH16"];

            if (row["fchrdeliveradd"] != null)
                parameters[75].Value = row["fchrdeliveradd"];
            else
                parameters[75].Value = DBNull.Value;


            if (row["fchrcusperson"] != null)
                parameters[76].Value = row["fchrcusperson"];
            else
                parameters[76].Value = DBNull.Value;


            if (row["fchrCenterDeliveryID"] != null && !string.IsNullOrEmpty(row["fchrCenterDeliveryID"].ToString()) && Guid.Parse(row["fchrCenterDeliveryID"].ToString()) != Guid.Empty)
                parameters[77].Value = row["fchrCenterDeliveryID"];
            else
                parameters[77].Value = DBNull.Value;


            if (row["fchrCenterDOrgID"] != null && !string.IsNullOrEmpty(row["fchrCenterDOrgID"].ToString()) && Guid.Parse(row["fchrCenterDOrgID"].ToString()) != Guid.Empty)
                parameters[78].Value = row["fchrCenterDOrgID"];
            else
                parameters[78].Value = DBNull.Value;

            parameters[79].Value = row["fbitSN"];

            if (row["fchrPostalCode"] != null)
                parameters[80].Value = row["fchrPostalCode"];
            else
                parameters[80].Value = DBNull.Value;


            if (row["fchrMobileNo"] != null)
                parameters[81].Value = row["fchrMobileNo"];
            else
                parameters[81].Value = DBNull.Value;


            if (row["fchrECTradeCode"] != null)
                parameters[82].Value = row["fchrECTradeCode"];
            else
                parameters[82].Value = DBNull.Value;


            if (row["fchrECOrderCode"] != null)
                parameters[83].Value = row["fchrECOrderCode"];
            else
                parameters[83].Value = DBNull.Value;


            if (row["fdtmECTrade"] != null && !string.IsNullOrEmpty(row["fdtmECTrade"].ToString()) && DateTime.Parse(row["fdtmECTrade"].ToString()) != DateTime.MinValue)
                parameters[84].Value = row["fdtmECTrade"];
            else
                parameters[84].Value = DBNull.Value;

            parameters[85].Value = row["fintOID"];

            if (row["fchrExpressCode"] != null)
                parameters[86].Value = row["fchrExpressCode"];
            else
                parameters[86].Value = DBNull.Value;
            if (row["fdtmAddTime"] != null && !string.IsNullOrEmpty(row["fdtmAddTime"].ToString()) && DateTime.Parse(row["fdtmAddTime"].ToString()) != DateTime.MinValue)
                parameters[87].Value = row["fdtmAddTime"];
            else
                parameters[87].Value = DBNull.Value;


            return DbHelperSQL.ExecuteSql(strSql.ToString(), parameters);
        }

        /// <summary>
        /// 对订单子表插入数据  朱旺 2018.4.19 add
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private object AddRetailVouchDetailData(object obj)
        {
            DataRow row = obj as DataRow;
            StringBuilder strSql = new StringBuilder();
            strSql.Append("insert into RetailVouchDetail(");
            strSql.Append("fchrAccountID,fchrRetailVouchDetailId,fchrRetailVouchID,fchrItemID,fchrUnitID,fchrBatchCode,fdtmProduceDate,flotQuantity,flotMoney,fchrMemo,flotPrice,flotDiscountRate,flotDiscount,fchrFree1,fchrFree2,fchrFree3,fchrFree4,fchrFree5,fchrFree6,fchrFree7,fchrFree8,fdtmInvalidDate,fchrFree9,fchrFree10,flotQuotePrice,ftinOrder,fchrBarCode,flotRetQuantity,flotRetMoney,flotRetDiscount,fchrEmployeeID,fchrEmployeeCode,fchrEmployeeName,flotDenoQuantity,fchrCoRetailDetailID,flotlocalediscountrate,flotmoneydiscount,fbitspecial,fchrPromotionID,flotRealPrice,fintTeamNum,fbitEndPromotion,ftinPromotionType,fchrDiscountCardCode,fintVipLevel,fintVipCardCode,ftinItemModel,fbitUse,flotLocaleDiscount,fintLocaleDiscountType,fintLocaleDiscountPrice,flotDiscountMoney,flotSaleNumber,flotEffaceMoney,fchrBackID,fchrOtherOperator,flotPromotionPloyPrice,flotPromotionPloyMoney,ftinPresentType,flotVipDiscountMoney,fchrPromotionPloyID,fchrPromotionUnitGroupID,flotDiscountCardMoney,fchrPromotionPriceSpecial,fdtmOldSellDate,flotCostPrice,fchrPosID,fchrPreItemCode,fchrUB1,fchrUB2,fchrUB3,fchrUB4,flotUB5,flotUB6,fchrUB7,fchrUB8,fchrUB9,fchrUB10,fchrUB11,fchrUB12,fintUB13,fintUB14,fdtmUB15,fdtmUB16,fchrProMode,flotDeliveryQuantity,fchrPPDetailIDs,fchrSavingCardNO,fchrGiftTokenNO,fchrRetailReportID,ftinItemType,flotCardDisApportion,flotGiftDisApportion,flotCardApportion,flotGiftApportion)");
            strSql.Append(" values (");
            strSql.Append("@fchrAccountID,@fchrRetailVouchDetailId,@fchrRetailVouchID,@fchrItemID,@fchrUnitID,@fchrBatchCode,@fdtmProduceDate,@flotQuantity,@flotMoney,@fchrMemo,@flotPrice,@flotDiscountRate,@flotDiscount,@fchrFree1,@fchrFree2,@fchrFree3,@fchrFree4,@fchrFree5,@fchrFree6,@fchrFree7,@fchrFree8,@fdtmInvalidDate,@fchrFree9,@fchrFree10,@flotQuotePrice,@ftinOrder,@fchrBarCode,@flotRetQuantity,@flotRetMoney,@flotRetDiscount,@fchrEmployeeID,@fchrEmployeeCode,@fchrEmployeeName,@flotDenoQuantity,@fchrCoRetailDetailID,@flotlocalediscountrate,@flotmoneydiscount,@fbitspecial,@fchrPromotionID,@flotRealPrice,@fintTeamNum,@fbitEndPromotion,@ftinPromotionType,@fchrDiscountCardCode,@fintVipLevel,@fintVipCardCode,@ftinItemModel,@fbitUse,@flotLocaleDiscount,@fintLocaleDiscountType,@fintLocaleDiscountPrice,@flotDiscountMoney,@flotSaleNumber,@flotEffaceMoney,@fchrBackID,@fchrOtherOperator,@flotPromotionPloyPrice,@flotPromotionPloyMoney,@ftinPresentType,@flotVipDiscountMoney,@fchrPromotionPloyID,@fchrPromotionUnitGroupID,@flotDiscountCardMoney,@fchrPromotionPriceSpecial,@fdtmOldSellDate,@flotCostPrice,@fchrPosID,@fchrPreItemCode,@fchrUB1,@fchrUB2,@fchrUB3,@fchrUB4,@flotUB5,@flotUB6,@fchrUB7,@fchrUB8,@fchrUB9,@fchrUB10,@fchrUB11,@fchrUB12,@fintUB13,@fintUB14,@fdtmUB15,@fdtmUB16,@fchrProMode,@flotDeliveryQuantity,@fchrPPDetailIDs,@fchrSavingCardNO,@fchrGiftTokenNO,@fchrRetailReportID,@ftinItemType,@flotCardDisApportion,@flotGiftDisApportion,@flotCardApportion,@flotGiftApportion)");
            SqlParameter[] parameters = {
                    new SqlParameter("@fchrAccountID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrRetailVouchDetailId", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrRetailVouchID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrItemID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrUnitID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrBatchCode", SqlDbType.NVarChar,40),
                    new SqlParameter("@fdtmProduceDate", SqlDbType.DateTime),
                    new SqlParameter("@flotQuantity", SqlDbType.Decimal,13),
                    new SqlParameter("@flotMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrMemo", SqlDbType.NVarChar,200),
                    new SqlParameter("@flotPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@flotDiscountRate", SqlDbType.Decimal,13),
                    new SqlParameter("@flotDiscount", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrFree1", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree2", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree3", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree4", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree5", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree6", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree7", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree8", SqlDbType.NVarChar,50),
                    new SqlParameter("@fdtmInvalidDate", SqlDbType.DateTime),
                    new SqlParameter("@fchrFree9", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrFree10", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotQuotePrice", SqlDbType.Decimal,13),
                    new SqlParameter("@ftinOrder", SqlDbType.Int,4),
                    new SqlParameter("@fchrBarCode", SqlDbType.NVarChar,40),
                    new SqlParameter("@flotRetQuantity", SqlDbType.Decimal,13),
                    new SqlParameter("@flotRetMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@flotRetDiscount", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrEmployeeID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrEmployeeCode", SqlDbType.NVarChar,50),
                    new SqlParameter("@fchrEmployeeName", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotDenoQuantity", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrCoRetailDetailID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@flotlocalediscountrate", SqlDbType.Decimal,13),
                    new SqlParameter("@flotmoneydiscount", SqlDbType.Decimal,13),
                    new SqlParameter("@fbitspecial", SqlDbType.NVarChar,10),
                    new SqlParameter("@fchrPromotionID", SqlDbType.NVarChar,2000),
                    new SqlParameter("@flotRealPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@fintTeamNum", SqlDbType.Int,4),
                    new SqlParameter("@fbitEndPromotion", SqlDbType.Bit,1),
                    new SqlParameter("@ftinPromotionType", SqlDbType.TinyInt,1),
                    new SqlParameter("@fchrDiscountCardCode", SqlDbType.NVarChar,20),
                    new SqlParameter("@fintVipLevel", SqlDbType.Int,4),
                    new SqlParameter("@fintVipCardCode", SqlDbType.NVarChar,30),
                    new SqlParameter("@ftinItemModel", SqlDbType.TinyInt,1),
                    new SqlParameter("@fbitUse", SqlDbType.Bit,1),
                    new SqlParameter("@flotLocaleDiscount", SqlDbType.Decimal,13),
                    new SqlParameter("@fintLocaleDiscountType", SqlDbType.Decimal,13),
                    new SqlParameter("@fintLocaleDiscountPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@flotDiscountMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@flotSaleNumber", SqlDbType.Decimal,13),
                    new SqlParameter("@flotEffaceMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrBackID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrOtherOperator", SqlDbType.NVarChar,50),
                    new SqlParameter("@flotPromotionPloyPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@flotPromotionPloyMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@ftinPresentType", SqlDbType.TinyInt,1),
                    new SqlParameter("@flotVipDiscountMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrPromotionPloyID", SqlDbType.NVarChar,800),
                    new SqlParameter("@fchrPromotionUnitGroupID", SqlDbType.NVarChar,800),
                    new SqlParameter("@flotDiscountCardMoney", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrPromotionPriceSpecial", SqlDbType.NVarChar,10),
                    new SqlParameter("@fdtmOldSellDate", SqlDbType.DateTime),
                    new SqlParameter("@flotCostPrice", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrPosID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@fchrPreItemCode", SqlDbType.NVarChar,255),
                    new SqlParameter("@fchrUB1", SqlDbType.NVarChar,60),
                    new SqlParameter("@fchrUB2", SqlDbType.NVarChar,60),
                    new SqlParameter("@fchrUB3", SqlDbType.NVarChar,60),
                    new SqlParameter("@fchrUB4", SqlDbType.NVarChar,60),
                    new SqlParameter("@flotUB5", SqlDbType.Decimal,13),
                    new SqlParameter("@flotUB6", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrUB7", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUB8", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUB9", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUB10", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUB11", SqlDbType.NVarChar,120),
                    new SqlParameter("@fchrUB12", SqlDbType.NVarChar,120),
                    new SqlParameter("@fintUB13", SqlDbType.Int,4),
                    new SqlParameter("@fintUB14", SqlDbType.Int,4),
                    new SqlParameter("@fdtmUB15", SqlDbType.DateTime),
                    new SqlParameter("@fdtmUB16", SqlDbType.DateTime),
                    new SqlParameter("@fchrProMode", SqlDbType.NVarChar,60),
                    new SqlParameter("@flotDeliveryQuantity", SqlDbType.Decimal,13),
                    new SqlParameter("@fchrPPDetailIDs", SqlDbType.NText),
                    new SqlParameter("@fchrSavingCardNO", SqlDbType.NText),
                    new SqlParameter("@fchrGiftTokenNO", SqlDbType.NText),
                    new SqlParameter("@fchrRetailReportID", SqlDbType.UniqueIdentifier,16),
                    new SqlParameter("@ftinItemType", SqlDbType.TinyInt,1),
                    new SqlParameter("@flotCardDisApportion", SqlDbType.Decimal,13),
                    new SqlParameter("@flotGiftDisApportion", SqlDbType.Decimal,13),
                    new SqlParameter("@flotCardApportion", SqlDbType.Decimal,13),
                    new SqlParameter("@flotGiftApportion", SqlDbType.Decimal,13)};

            if (row["fchrAccountID"] != null && !string.IsNullOrEmpty(row["fchrAccountID"].ToString()) && Guid.Parse(row["fchrAccountID"].ToString()) != Guid.Empty)
                parameters[0].Value = row["fchrAccountID"];
            else
                parameters[0].Value = DBNull.Value;

            if (row["fchrRetailVouchDetailId"] != null && !string.IsNullOrEmpty(row["fchrRetailVouchDetailId"].ToString()) && Guid.Parse(row["fchrRetailVouchDetailId"].ToString()) != Guid.Empty)
                parameters[1].Value = row["fchrRetailVouchDetailId"];
            else
                parameters[1].Value = DBNull.Value;

            if (row["fchrRetailVouchID"] != null && !string.IsNullOrEmpty(row["fchrRetailVouchID"].ToString()) && Guid.Parse(row["fchrRetailVouchID"].ToString()) != Guid.Empty)
                parameters[2].Value = row["fchrRetailVouchID"];
            else
                parameters[2].Value = DBNull.Value;

            if (row["fchrItemID"] != null && !string.IsNullOrEmpty(row["fchrItemID"].ToString()) && Guid.Parse(row["fchrItemID"].ToString()) != Guid.Empty)
                parameters[3].Value = row["fchrItemID"];
            else
                parameters[3].Value = DBNull.Value;

            if (row["fchrUnitID"] != null && !string.IsNullOrEmpty(row["fchrUnitID"].ToString()) && Guid.Parse(row["fchrUnitID"].ToString()) != Guid.Empty)
                parameters[4].Value = row["fchrUnitID"];
            else
                parameters[4].Value = DBNull.Value;
            parameters[5].Value = row["fchrBatchCode"];
            parameters[6].Value = row["fdtmProduceDate"];
            parameters[7].Value = row["flotQuantity"];
            parameters[8].Value = row["flotMoney"];
            parameters[9].Value = row["fchrMemo"];
            parameters[10].Value = row["flotPrice"];
            parameters[11].Value = row["flotDiscountRate"];
            parameters[12].Value = row["flotDiscount"];
            parameters[13].Value = row["fchrFree1"];
            parameters[14].Value = row["fchrFree2"];
            parameters[15].Value = row["fchrFree3"];
            parameters[16].Value = row["fchrFree4"];
            parameters[17].Value = row["fchrFree5"];
            parameters[18].Value = row["fchrFree6"];
            parameters[19].Value = row["fchrFree7"];
            parameters[20].Value = row["fchrFree8"];
            parameters[21].Value = row["fdtmInvalidDate"];
            parameters[22].Value = row["fchrFree9"];
            parameters[23].Value = row["fchrFree10"];
            parameters[24].Value = row["flotQuotePrice"];
            parameters[25].Value = row["ftinOrder"];
            parameters[26].Value = row["fchrBarCode"];
            parameters[27].Value = row["flotRetQuantity"];
            parameters[28].Value = row["flotRetMoney"];
            parameters[29].Value = row["flotRetDiscount"];
            parameters[30].Value = row["fchrEmployeeID"];
            parameters[31].Value = row["fchrEmployeeCode"];
            parameters[32].Value = row["fchrEmployeeName"];
            parameters[33].Value = row["flotDenoQuantity"];
            parameters[34].Value = row["fchrCoRetailDetailID"];
            parameters[35].Value = row["flotlocalediscountrate"];
            parameters[36].Value = row["flotmoneydiscount"];
            parameters[37].Value = row["fbitspecial"];
            parameters[38].Value = row["fchrPromotionID"];
            parameters[39].Value = row["flotRealPrice"];
            parameters[40].Value = row["fintTeamNum"];
            parameters[41].Value = row["fbitEndPromotion"];
            parameters[42].Value = row["ftinPromotionType"];
            parameters[43].Value = row["fchrDiscountCardCode"];
            parameters[44].Value = row["fintVipLevel"];
            parameters[45].Value = row["fintVipCardCode"];
            parameters[46].Value = row["ftinItemModel"];
            parameters[47].Value = row["fbitUse"];
            parameters[48].Value = row["flotLocaleDiscount"];
            parameters[49].Value = row["fintLocaleDiscountType"];
            parameters[50].Value = row["fintLocaleDiscountPrice"];
            parameters[51].Value = row["flotDiscountMoney"];
            parameters[52].Value = row["flotSaleNumber"];
            parameters[53].Value = row["flotEffaceMoney"];
            parameters[54].Value = row["fchrBackID"];
            parameters[55].Value = row["fchrOtherOperator"];
            parameters[56].Value = row["flotPromotionPloyPrice"];
            parameters[57].Value = row["flotPromotionPloyMoney"];
            parameters[58].Value = row["ftinPresentType"];
            parameters[59].Value = row["flotVipDiscountMoney"];
            parameters[60].Value = row["fchrPromotionPloyID"];
            parameters[61].Value = row["fchrPromotionUnitGroupID"];
            parameters[62].Value = row["flotDiscountCardMoney"];
            parameters[63].Value = row["fchrPromotionPriceSpecial"];
            parameters[64].Value = row["fdtmAddTime2"];
            parameters[65].Value = row["flotCostPrice"];
            parameters[66].Value = row["fchrPosID"];
            parameters[67].Value = row["fchrPreItemCode"];
            parameters[68].Value = row["fchrUB1"];
            parameters[69].Value = row["fchrUB2"];
            parameters[70].Value = row["fchrUB3"];
            parameters[71].Value = row["fchrUB4"];
            parameters[72].Value = row["flotUB5"];
            parameters[73].Value = row["flotUB6"];
            parameters[74].Value = row["fchrUB7"];
            parameters[75].Value = row["fchrUB8"];
            parameters[76].Value = row["fchrUB9"];
            parameters[77].Value = row["fchrUB10"];
            parameters[78].Value = row["fchrUB11"];
            parameters[79].Value = row["fchrUB12"];
            parameters[80].Value = row["fintUB13"];
            parameters[81].Value = row["fintUB14"];
            parameters[82].Value = row["fdtmUB15"];
            parameters[83].Value = row["fdtmUB16"];
            parameters[84].Value = row["fchrProMode"];
            parameters[85].Value = row["flotDeliveryQuantity"];
            parameters[86].Value = row["fchrPPDetailIDs"];
            parameters[87].Value = row["fchrSavingCardNO"];
            parameters[88].Value = row["fchrGiftTokenNO"];
            parameters[89].Value = row["fchrRetailReportID"];
            parameters[90].Value = row["ftinItemType"];
            parameters[91].Value = row["flotCardDisApportion"];
            parameters[92].Value = row["flotGiftDisApportion"];
            parameters[93].Value = row["flotCardApportion"];
            parameters[94].Value = row["flotGiftApportion"];
            return DbHelperSQL.ExecuteSql(strSql.ToString(), parameters);
        }
    }
}
