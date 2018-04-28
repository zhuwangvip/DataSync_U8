using Amib.Threading;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataSync
{
    class Program
    {
        static void Main(string[] args)
        {
            new Thread(new ParameterizedThreadStart(RetailVouch)).Start(0);
            new Thread(new ParameterizedThreadStart(RetailVouchDetail)).Start(0);
            //string type = Console.ReadLine();

            //RetailVouchDetail();
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

        private static void RetailVouch(object o)
        {
            Sync sync = new Sync("RetailVouch");
            sync.SyncData();
        }
        private static void RetailVouchDetail(object o)
        {
            Sync sync = new Sync("RetailVouchDetail");
            sync.SyncData();
        }
    }
}
