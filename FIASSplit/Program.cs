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

            //AddrTable.CreateIndex();
            //HouseTable.CreateIndex();
            //RoomTable.CreateIndex(); 

            Update(new FileInfo(@"Z:\Program Files\FIASFiles\fias_delta_xml_20190311.rar"));

            //Update();

            //var info = DownloadLastDelta();


            //AddrXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));
            //HouseXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));
            //RoomXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));


            //FIASFile.SplitFile(new FileInfo(@"Z:\Program Files\FIASFiles\402\Src\AS_ROOM_20171126_de683306-b1d5-4518-8776-44f9866e7c26.XML"),
            //                   new DirectoryInfo(@"Z:\Program Files\FIASFiles\402\Split")
            //                    );
            //AddrXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, "402")));
            //HouseXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, "402")));
            //RoomXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, "402")));

            //DownloadDeltas();

            //Console.WriteLine("test");
            //RoomXMLNode.FindNodeInFile(new FileInfo(@"Z:\Program Files\FIASFiles\402\Split\AS_ROOM_00.xml"), "00444d64-c7b0-4dd1-81ca-ca374d28e863");
            //Console.WriteLine("test finish");
            //Console.WriteLine();
            //Console.WriteLine();
            //Console.WriteLine();
            //Console.WriteLine("---");
            //Console.WriteLine("find 6abee843-9661-495e-af7b-c742b067b56e");
            //HouseXMLNode.FindNodeInFile(new FileInfo(@"Z:\Program Files\FIASFiles\402\Split\AS_HOUSE_6a.xml"), "6abee843-9661-495e-af7b-c742b067b56e");

            //AddrXMLNode.Update(new DirectoryInfo(Path.Combine(dataDir.FullName, "234")));
            //HouseXMLNode.Update(new DirectoryInfo(Path.Combine(dataDir.FullName, "234")));
            //Console.WriteLine("find finish");

            //CursorHelper ch = new CursorHelper();
            //int i = 0;
            //foreach (var n in HouseXMLNode.GetActual(new DirectoryInfo(Path.Combine(dataDir.FullName, "402"))))
            //{
            //    if (i % 10000 == 1)
            //    {
            //        ch.WriteLine(string.Format("test {0} HOUSEID", i.ToString("### ### ###")));
            //        Console.WriteLine("c0 {0}", HouseXMLNode.c0.ToString("### ### ###"));
            //        Console.WriteLine("c1 {0}", HouseXMLNode.c1.ToString("### ### ###"));
            //        Console.WriteLine("c2 {0}", HouseXMLNode.c2.ToString("### ### ###"));
            //        Console.WriteLine("c3 {0}", HouseXMLNode.c3.ToString("### ### ###"));
            //    }
            //    ++i;
            //}

            Console.WriteLine("finish");
            Console.ReadKey();
        }

        static void UpdateOld()
        {
            var info = DownloadLastDelta();

            CleanDB();

            AddrXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));
            HouseXMLNode.Upload(new DirectoryInfo(Path.Combine(dataDir.FullName, info.VersionId.ToString())));

            AddIndex();
            BackupDB();
        }


        static void Update()
        {
            Update(DownloadLastDelta2());
            BackupDB();
        }
        
        static void Update(FileInfo file)
        {
            AddrTable.Upload(file);
            HouseTable.Upload(file);
            RoomTable.Upload(file);
        }

        static void BackupDB()
        {
            var DBBackupFolder = new DirectoryInfo(ConfigurationManager.AppSettings["DBBackupFolder"]);
            //
            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandTimeout = 3000;
            cmd.CommandText = string.Format("BACKUP DATABASE {0} TO DISK = '{1}' WITH FORMAT, COMPRESSION", conn.Database, Path.Combine(DBBackupFolder.FullName, "FIAS.bak"));
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
            cmd.CommandText = "truncate table[dbo].[ROOM]";
            cmd.ExecuteNonQuery();

            // ADDROBJ
            cmd.CommandText = "ALTER TABLE [dbo].[ADDROBJ] DROP CONSTRAINT [PK_dbo.ADDROBJ]";
            cmd.ExecuteNonQuery();

            // HOUSE
            cmd.CommandText = "DROP INDEX [FK_AOID] ON [dbo].[HOUSE]";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "ALTER TABLE [dbo].[HOUSE] DROP CONSTRAINT [PK_dbo.HOUSE]";
            cmd.ExecuteNonQuery();
            
            // ROOM
            cmd.CommandText = "DROP INDEX [FK_HOUSEGUID] ON [dbo].[ROOM]";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "ALTER TABLE [dbo].[ROOM] DROP CONSTRAINT [PK_dbo.ROOM]";
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

            // AO
            ConsoleHelper.WriteLine("Start create PK_dbo.ADDROBJ");
            cmd.CommandText = "ALTER TABLE[dbo].[ADDROBJ] ADD CONSTRAINT[PK_dbo.ADDROBJ] PRIMARY KEY NONCLUSTERED([AOGUID] ASC)";
            cmd.CommandTimeout = 3000;
            cmd.ExecuteNonQuery();

            // House
            ConsoleHelper.WriteLine("Start create PK_dbo.HOUSE");
            cmd.CommandText = "ALTER TABLE[dbo].[HOUSE] ADD CONSTRAINT[PK_dbo.HOUSE] PRIMARY KEY NONCLUSTERED([HOUSEGUID] ASC)";
            cmd.CommandTimeout = 3000;
            cmd.ExecuteNonQuery();

            ConsoleHelper.WriteLine("Start create FK_AOID");
            cmd.CommandText = "CREATE NONCLUSTERED INDEX [FK_AOID] ON [dbo].[HOUSE]([AOGUID] ASC)";
            cmd.CommandTimeout = 3000;
            cmd.ExecuteNonQuery();
            
            // Room
            ConsoleHelper.WriteLine("Start create PK_dbo.ROOM");
            cmd.CommandText = "ALTER TABLE[dbo].[ROOM] ADD CONSTRAINT[PK_dbo.ROOM] PRIMARY KEY NONCLUSTERED([ROOMGUID] ASC)";
            cmd.CommandTimeout = 3000;
            cmd.ExecuteNonQuery();

            ConsoleHelper.WriteLine("Start create FK_HOUSEGUID");
            cmd.CommandText = "CREATE NONCLUSTERED INDEX [FK_HOUSEGUID] ON [dbo].[ROOM]([HOUSEGUID] ASC)";
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

        static FileInfo DownloadLastDelta2()
        {
            Console.WriteLine("check update ...");

            var binding = new BasicHttpBinding();
            binding.MaxReceivedMessageSize = Int32.MaxValue;

            var addr = new EndpointAddress("http://fias.nalog.ru/WebServices/Public/DownloadService.asmx");

            var c = new FIASService.DownloadServiceSoapClient(binding, addr);
            var info = c.GetLastDownloadFileInfo();

            var updateFile = new FileInfo(Path.Combine(dataDir.FullName, string.Format("fias_delta_xml_{0}.rar", info.VersionId)));
            if (!updateFile.Exists)
            {
                using (var client = new WebClient())
                {
                    ConsoleHelper.WriteLine(string.Format("download {0}...", updateFile));
                    client.DownloadFile(info.FiasCompleteXmlUrl, updateFile.FullName);
                    ConsoleHelper.WriteLine("download ok");
                }
            }

            return updateFile;
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
