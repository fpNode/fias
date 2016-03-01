using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace FIASSplit
{
    class CursorHelper
    {
        private int top;
        private int left;

        public CursorHelper()
        {
            top = Console.CursorTop;
            left = Console.CursorLeft;
        }

        public void Write(int n)
        {
            Console.CursorTop = top;
            Console.CursorLeft = left;

            Console.Write(n);
        }
        public void Write(string s)
        {
            Console.CursorTop = top;
            Console.CursorLeft = left;

            Console.Write(s);
        }

        public void WriteLine(string s)
        {
            Console.CursorTop = top;
            Console.CursorLeft = left;

            Console.WriteLine("{0} {1} {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString(), s);
        }
    }

    public class ConsoleHelper
    {
        public static void WriteLine(string s)
        {
            Console.WriteLine("{0} {1} {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString(), s);
        }
    }


    class Program
    {
        public static DirectoryInfo dataDir = null;

        static void Main(string[] args)
        {
            var dataDirName = ConfigurationManager.AppSettings["DataFolder"];

            if (string.IsNullOrEmpty(dataDirName))
                dataDirName = "Updates";

            dataDir = new DirectoryInfo(dataDirName);
            if (!dataDir.Exists)
                dataDir.Create();

            
            var info = DownloadLastDelta();

            CleanDB();

            AddrXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));
            HouseXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));

            AddIndex();

            BackupDB();

            //DownloadDeltas();

            //AddrXMLNode.FindNodeInFile(new FileInfo(@"Z:\Program Files\FIASFiles\_234\AS_ADDROBJ_20160203_81d86e60-67bf-456c-8193-8711d4fccc54.XML"));
            //HouseXMLNode.FindNodeInFile(new FileInfo(@"Z:\Program Files\FIASFiles\234\Split\AS_HOUSE_28.xml"));

            //AddrXMLNode.Update(new DirectoryInfo(Path.Combine(dataDir.FullName, "234")));
            //HouseXMLNode.Update(new DirectoryInfo(Path.Combine(dataDir.FullName, "234")));

            Console.WriteLine("finish");
            Console.ReadKey();
        }

        static void BackupDB()
        {
            //
            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandTimeout = 3000;
            cmd.CommandText = string.Format("BACKUP DATABASE {0} TO DISK = '{1}' WITH FORMAT, COMPRESSION", conn.Database, Path.Combine(dataDir.FullName, "FIAS.bak"));
            cmd.ExecuteNonQuery();
            ConsoleHelper.WriteLine("BackupDB");
            conn.Close();
        }

        static void CleanDB()
        {
            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "truncate table [dbo].[ADDROBJ]";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "truncate table[dbo].[HOUSE]";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "DROP INDEX[FK_AOID] ON[dbo].[HOUSE]";
            cmd.ExecuteNonQuery();
            ConsoleHelper.WriteLine("CleanDB");
            conn.Close();
        }

        static void AddIndex()
        {
            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE NONCLUSTERED INDEX [FK_AOID] ON [dbo].[HOUSE]([AOGUID] ASC)";
            cmd.CommandTimeout = 3000;
            cmd.ExecuteNonQuery();
            ConsoleHelper.WriteLine("Create Index");
            conn.Close();
        }

        static int FindLastDelta()
        {
            int maxDelta = int.MinValue;
            foreach(var d in dataDir.GetDirectories())
            {
                int n = 0;
                if (int.TryParse(d.Name, out n))
                {
                    maxDelta = Math.Max(maxDelta, n);
                }
            }
            return maxDelta;
        }

        static FIASService.DownloadFileInfo DownloadLastDelta()
        {
            Console.WriteLine("check update ...");

            var binding = new BasicHttpBinding();
            binding.MaxReceivedMessageSize = Int32.MaxValue;

            var addr = new EndpointAddress("http://fias.nalog.ru/WebServices/Public/DownloadService.asmx");

            var c = new FIASService.DownloadServiceSoapClient(binding, addr);
            var info = c.GetLastDownloadFileInfo();

            var rDir = new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString()));
            rDir.Create();

            var updateFile = new FileInfo(Path.Combine(rDir.FullName, string.Format("fias_delta_xml_{0}.rar", info.VersionId)));
            if (!updateFile.Exists)
            {
                using (var client = new WebClient())
                {
                    ConsoleHelper.WriteLine(string.Format("download {0}...", updateFile));
                    client.DownloadFile(info.FiasCompleteXmlUrl, updateFile.FullName);
                    ConsoleHelper.WriteLine("download ok");
                }
            }

            return info;
        }

        static void DownloadDeltas()
        {
            Console.WriteLine("check update ...");

            var binding = new BasicHttpBinding();
            binding.MaxReceivedMessageSize = Int32.MaxValue;

            var addr = new EndpointAddress("http://fias.nalog.ru/WebServices/Public/DownloadService.asmx");

            var c = new FIASService.DownloadServiceSoapClient(binding, addr);
            IEnumerable<FIASService.DownloadFileInfo> infos = (IEnumerable<FIASService.DownloadFileInfo>)c.GetAllDownloadFileInfo();

            int lastDelta = FindLastDelta();

            var infoLst = infos.Where(v => v.VersionId > lastDelta).OrderBy(ord => ord.VersionId).ToList();
            if (infoLst.Count() > 0)
            {
                foreach (var info in infoLst)
                {
                    var rDir = new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString()));
                    rDir.Create();

                    var updateFile = new FileInfo(Path.Combine(rDir.FullName, string.Format("fias_delta_xml_{0}.rar", info.VersionId)));
                    if (!updateFile.Exists)
                    {
                        using (var client = new WebClient())
                        {
                            Console.WriteLine("download {0}...", updateFile);
                            client.DownloadFile(info.FiasDeltaXmlUrl, updateFile.FullName);
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("No updates available");
            }
        }
    }
}
