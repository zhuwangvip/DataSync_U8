using Amib.Threading;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSync
{
    class Program
    {
        static void Main(string[] args)
        {
            RetailVouchDetail();
            //ManualSync sync = new ManualSync("StoreDefine");
            //sync.SyncData();
            //RetailVouchDetail();
            //using (SmartThreadPool smartThreadPool = new SmartThreadPool())
            //{
            //    List<IWorkItemResult> wirs = new List<IWorkItemResult>();
            //    IWorkItemResult wir = smartThreadPool.QueueWorkItem(new WorkItemCallback(RetailVouch));
            //    wirs.Add(wir);
            //    wir = smartThreadPool.QueueWorkItem(new WorkItemCallback(RetailVouchDetail));
            //    wirs.Add(wir);
            //    SmartThreadPool.WaitAll(wirs.ToArray());
            //}
        }

        private static object RetailVouch(object obj)
        {
            Sync sync = new Sync("RetailVouch");
            sync.SyncData();
            return 0;
        }
        private static object RetailVouchDetail()
        {
            Sync sync = new Sync("RetailVouchDetail");
            sync.SyncData();
            return 0;
        }
    }

}
