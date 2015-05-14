using fpNode.FIAS.Models;
using NUnrar.Archive;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Serialization;
using System.Linq;

namespace fpNode.FIAS.Tools
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

            Console.WriteLine(s);
        }

    }
    
    class Program
    {
        static void UpdateObject<T>(T self, T other)
        {
            Type t = self.GetType();
            foreach (PropertyInfo info in t.GetProperties())
            {
                if (info.CanWrite)
                {
                    info.SetValue(self, info.GetValue(other), null);
                }
            }
        }

        static DirectoryInfo dataDir = null;
        static FileInfo      updateInfoFile = null;

        static void Main(string[] args)
        {
            var dataDirName = ConfigurationManager.AppSettings["DataFolder"];

            if (string.IsNullOrEmpty(dataDirName))
                dataDirName = "Updates";

            dataDir = new DirectoryInfo(dataDirName);
            if (!dataDir.Exists)
                dataDir.Create();

            updateInfoFile = new FileInfo(dataDir + "/update.info");

            if (!updateInfoFile.Exists)
            {
                XMLBulkCopy();
            }
            UpdateData();

            Console.WriteLine("finish");
            Console.ReadKey();
        }

        static void UpdateData()
        {
            Console.WriteLine("check update ...");

            var c = new FIASService.DownloadServiceSoapClient();
            IEnumerable<FIASService.DownloadFileInfo> infos = (IEnumerable<FIASService.DownloadFileInfo>)c.GetAllDownloadFileInfo();

            int lastDelta = FindLastDelta();

            var infoLst = infos.Where(v => v.VersionId > lastDelta).OrderBy(ord => ord.VersionId).ToList();
            if (infoLst.Count() > 0)
            {
                var updateInfoFile = new FileInfo(dataDir + "/update.info");

                foreach (var info in infoLst)
                {
                    var updateFile = new FileInfo(string.Format("{0}/fias_delta_xml_{1}.rar", dataDir.ToString(), info.VersionId));
                    if (!updateFile.Exists)
                    {
                        using (var client = new WebClient())
                        {
                            Console.WriteLine("download {0}...", updateFile);
                            client.DownloadFile(info.FiasDeltaXmlUrl, updateFile.FullName);
                        }
                    }
                    ExtractRar(updateFile);
                    XMLDelta();

                    using (var writer = updateInfoFile.AppendText())
                    {
                        writer.WriteLine(info.VersionId);
                    }
                }
            }
            else
            {
                Console.WriteLine("No updates available");
            }
        }

        static void DelObjects()
        {
            foreach (var f in dataDir.GetFiles())
            {
                if (f.Name.StartsWith("AS_DEL_ADDROBJ_"))
                {
                    DELADDROBJ(f);
                }
                else if (f.Name.StartsWith("AS_DEL_HOUSE_"))
                {
                    DELHOUSE(f);
                }
                else if (f.Name.StartsWith("AS_DEL_HOUSEINT_"))
                {
                    DELHOUSEINT(f);
                }
                else if (f.Name.StartsWith("AS_DEL_LANDMARK_"))
                {
                    DELLANDMARK(f);
                }
            }
        }

        static void XMLDelta()
        {
            Console.WriteLine("applying changes...");

            foreach (var f in dataDir.GetFiles("*.xml"))
            {
                if (f.Name.StartsWith("AS_ADDROBJ_"))
                {
                    ADDADDROBJ(f);
                }
                //else if (f.Name.StartsWith("AS_HOUSE_"))
                //{
                //    ADDHOUSE(f);
                //}
                //else if (f.Name.StartsWith("AS_HOUSEINT_"))
                //{
                //    ADDHOUSEINT(f);
                //}
                //else if (f.Name.StartsWith("AS_LANDMARK_"))
                //{
                //    ADDLANDMARK(f);
                //}
            }
            DelObjects();
        }

        static int FindLastDelta()
        {
            int maxDelta = int.MinValue;
            using (var sr = updateInfoFile.OpenText())
            {
                string s = "";
                while ((s = sr.ReadLine()) != null)
                {
                    maxDelta = Math.Max(maxDelta, int.Parse(s));
                }
            }
            return maxDelta;
        }

        static int FindFirstDelta()
        {
            int minDelta = int.MaxValue;
            var re = new Regex(@".*?(\d+)\.rar");
            foreach (var f in dataDir.GetFiles("*.rar"))
            {
                var m = re.Match(f.Name);
                if (m.Success)
                {
                    minDelta = Math.Min(minDelta, int.Parse(m.Groups[1].Value));
                }
            }
            return minDelta;
        }

        static void ExtractRar(FileInfo f)
        {
            foreach (var xmlFile in dataDir.GetFiles("*.xml"))
            {
                xmlFile.Delete();
            }

            Console.WriteLine("extract {0}...", f);
            RarArchive archive = RarArchive.Open(f.FullName);
            foreach (RarArchiveEntry entry in archive.Entries)
            {
                string path = Path.Combine(dataDir.ToString(), Path.GetFileName(entry.FilePath));
                entry.WriteToFile(path);
            }
        }

        static void XMLBulkCopy()
        {
            //Create tables
            var context = new FIASContext("name=DefaultConnection");
            context.Database.Initialize(false);

            using(SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                var tables = new string[] {
                    "ADDROBJ",
                    "HOUSE",
                    "HOUSEINT",
                    "LANDMARK",
                    "SOCRBASE"
                };

                conn.Open();
                foreach(var t in tables)
                { 
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = string.Format("ALTER TABLE [dbo].[{0}] DROP CONSTRAINT [PK_dbo.{0}]", t);
                    cmd.ExecuteNonQuery();
                }
                conn.Close();
            }

            int minDelta = FindFirstDelta();
            var updateFile = new FileInfo(string.Format("{0}/fias_delta_xml_{1}.rar", dataDir.ToString(), minDelta));

            ExtractRar(updateFile);
            
            foreach (var f in dataDir.GetFiles("*.xml"))
            {
                if (f.Name.StartsWith("AS_ADDROBJ_"))
                {
                    XMLBulkCopy(f, "ADDROBJ", typeof(AddressObjectsObject));
                }
                //else if (f.Name.StartsWith("AS_HOUSE_"))
                //{
                //    XMLBulkCopy(f, "HOUSE", typeof(HousesHouse));
                //}
                //else if (f.Name.StartsWith("AS_HOUSEINT_"))
                //{
                //    XMLBulkCopy(f, "HOUSEINT", typeof(HouseIntervalsHouseInterval));
                //}
                //else if (f.Name.StartsWith("AS_LANDMARK_"))
                //{
                //    XMLBulkCopy(f, "LANDMARK", typeof(LandmarksLandmark));
                //}
                //else if (f.Name.StartsWith("AS_SOCRBASE_"))
                //{
                //    XMLBulkCopy(f, "SOCRBASE", typeof(AddressObjectTypesAddressObjectType));
                //}
            }

            using(SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                

                Console.WriteLine("ADDROBJ create constraint ...");
                var cmd = conn.CreateCommand();
                cmd.CommandTimeout = 0;
                cmd.CommandText = "ALTER TABLE [dbo].[ADDROBJ] ADD  CONSTRAINT [PK_dbo.ADDROBJ] PRIMARY KEY NONCLUSTERED([AOID] ASC)";
                cmd.ExecuteNonQuery();
                Console.WriteLine("complete");

                Console.WriteLine("HOUSE create constraint ...");
                cmd.CommandText = "ALTER TABLE [dbo].[HOUSE] ADD  CONSTRAINT [PK_dbo.HOUSE] PRIMARY KEY NONCLUSTERED([HOUSEID] ASC)";
                cmd.ExecuteNonQuery();
                Console.WriteLine("complete");

                Console.WriteLine("HOUSEINT create constraint ...");
                cmd.CommandText = "ALTER TABLE [dbo].[HOUSEINT] ADD  CONSTRAINT [PK_dbo.HOUSEINT] PRIMARY KEY NONCLUSTERED([HOUSEINTID] ASC)";
                cmd.ExecuteNonQuery();
                Console.WriteLine("complete");

                Console.WriteLine("LANDMARK create constraint ...");
                cmd.CommandText = "ALTER TABLE [dbo].[LANDMARK] ADD  CONSTRAINT [PK_dbo.LANDMARK] PRIMARY KEY NONCLUSTERED([LANDID] ASC)";
                cmd.ExecuteNonQuery();
                Console.WriteLine("complete");

                Console.WriteLine("SOCRBASE create constraint ...");
                cmd.CommandText = "ALTER TABLE [dbo].[SOCRBASE] ADD  CONSTRAINT [PK_dbo.SOCRBASE] PRIMARY KEY NONCLUSTERED([KOD_T_ST] ASC)";
                cmd.ExecuteNonQuery();
                Console.WriteLine("complete");

                conn.Close();
            }
            DelObjects();

            using (StreamWriter writer = updateInfoFile.CreateText())
            {
                writer.WriteLine(minDelta);
            }
        }

        static void XMLBulkCopy(FileInfo fi, string DestinationTableName, Type type)
        {
            Console.Write(DestinationTableName + " ");
            var ch = new CursorHelper();
            
            XmlReader reader = null;
            SqlConnection conn = null;

            try
            {
                conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
                conn.Open();

                SqlBulkCopyOptions options = (SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock);
                var copy = new SqlBulkCopy(conn, options, null);
                copy.BatchSize = 5000;

                DataTable dt = new DataTable();
                foreach (var prop in type.GetProperties())
                {
                    dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
                    copy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(prop.Name, prop.Name));
                }
                copy.DestinationTableName = DestinationTableName;
                copy.BulkCopyTimeout = 0;
                int count = 0;

                reader = XmlReader.Create(fi.FullName);
                reader.MoveToContent();

                // loop through Object elements
                while (reader.Read())
                {
                    if (reader.NodeType == XmlNodeType.Element)
                    {
                        DataRow row = dt.NewRow();
                        while (reader.MoveToNextAttribute())
                        {
                            row[reader.LocalName] = reader.Value;
                        }
                        dt.Rows.Add(row);

                        if (++count % 10000 == 0)
                        {
                            ch.WriteLine(string.Format("count: {0}", count));
                            copy.WriteToServer(dt);
                            dt.Rows.Clear();
                        }
                    }
                }
                copy.WriteToServer(dt);
                dt.Rows.Clear();
                ch.WriteLine(string.Format("count: {0} complete", count));
                copy.Close();
            }
            finally
            {
                if (reader != null)
                    reader.Dispose();
                if (conn != null)
                    conn.Dispose();
            }
        }

        static void ADDADDROBJ(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(AddressObjectsObject), new XmlRootAttribute("Object"));

            Console.Write("AddressObjects ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int addCount = 0;
                int updateCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();
                    
                    reader.Read();
                    
                    // loop through Object elements
                    while (reader.Name == "Object")
                    {
                        AddressObjectsObject obj = (AddressObjectsObject)serializer.Deserialize(new StringReader(reader.ReadOuterXml()));

                        var ao = context.AddressObjects.Find(obj.AOID);
                        if (null == ao)
                        {
                            context.AddressObjects.Add(obj);
                            ++addCount;
                        }
                        else
                        {
                            UpdateObject(ao, obj);
                            ++updateCount;
                        }

                        ch.WriteLine(string.Format("add: {0} update: {1} total {2} ", addCount, updateCount, ++count));
                    }
                }
                Console.WriteLine("Save changes...");
                context.SaveChanges();
                Console.WriteLine("complete");
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void ADDHOUSE(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(HousesHouse), new XmlRootAttribute("House"));

            Console.Write("House ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int addCount = 0;
                int updateCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    reader.Read();

                    // loop through Object elements
                    while (reader.Name == "House")
                    {
                        HousesHouse obj = (HousesHouse)serializer.Deserialize(new StringReader(reader.ReadOuterXml()));

                        var ao = context.Houses.Find(obj.HOUSEID);
                        if (null == ao)
                        {
                            context.Houses.Add(obj);
                            ++addCount;
                        }
                        else
                        {
                            UpdateObject(ao, obj);
                            ++updateCount;
                        }

                        ch.WriteLine(string.Format("add: {0} update: {1} total {2} ", addCount, updateCount, ++count));
                    }
                }
                Console.WriteLine("Save changes...");
                context.SaveChanges();
                Console.WriteLine("complete");
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void ADDHOUSEINT(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(HouseIntervalsHouseInterval), new XmlRootAttribute("HouseInterval"));

            Console.Write("HouseInterval ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int addCount = 0;
                int updateCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    reader.Read();

                    // loop through Object elements
                    while (reader.Name == "HouseInterval")
                    {
                        HouseIntervalsHouseInterval obj = (HouseIntervalsHouseInterval)serializer.Deserialize(new StringReader(reader.ReadOuterXml()));

                        var ao = context.HouseIntervals.Find(obj.HOUSEINTID);
                        if (null == ao)
                        {
                            context.HouseIntervals.Add(obj);
                            ++addCount;
                        }
                        else
                        {
                            UpdateObject(ao, obj);
                            ++updateCount;
                        }

                        ch.WriteLine(string.Format("add: {0} update: {1} total {2} ", addCount, updateCount, ++count));
                    }
                }
                Console.WriteLine("Save changes...");
                context.SaveChanges();
                Console.WriteLine("complete");
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void ADDLANDMARK(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(LandmarksLandmark), new XmlRootAttribute("Landmark"));

            Console.Write("Landmark ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int addCount = 0;
                int updateCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    reader.Read();

                    // loop through Object elements
                    while (reader.Name == "Landmark")
                    {
                        LandmarksLandmark obj = (LandmarksLandmark)serializer.Deserialize(new StringReader(reader.ReadOuterXml()));

                        var dbSet = context.Set<LandmarksLandmark>();
                        var ao = dbSet.Find(EntityKeyHelper.Instance.GetKeys(obj, context));

                        if (null == ao)
                        {
                            dbSet.Add(obj);
                            ++addCount;
                        }
                        else
                        {
                            UpdateObject(ao, obj);
                            ++updateCount;
                        }

                        ch.WriteLine(string.Format("add: {0} update: {1} total {2} ", addCount, updateCount, ++count));
                    }
                }
                Console.WriteLine("Save changes...");
                context.SaveChanges();
                Console.WriteLine("complete");
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void DELADDROBJ(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(AddressObjectsObject), new XmlRootAttribute("Object"));

            Console.Write("AddressObjects ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int delCount = 0;
                int skipCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    // loop through Object elements
                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.Name == "Object")
                        {
                            var aoId = reader.GetAttribute("AOID");

                            var ao = context.AddressObjects.Find(aoId);
                            if (null != ao)
                            {
                                context.AddressObjects.Remove(ao);
                                context.SaveChanges();
                                ++delCount;
                            }
                            else
                            {
                                ++skipCount;
                            }

                            ch.WriteLine(string.Format("del: {0} skip: {1} total {2} ", delCount, skipCount, ++count));
                        }
                    }
                }
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void DELHOUSE(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(HousesHouse), new XmlRootAttribute("House"));

            Console.Write("House ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int delCount = 0;
                int skipCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    // loop through Object elements
                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.Name == "House")
                        {
                            var aoId = reader.GetAttribute("HOUSEID");

                            var ao = context.Houses.Find(aoId);
                            if (null != ao)
                            {
                                context.Houses.Remove(ao);
                                context.SaveChanges();
                                ++delCount;
                            }
                            else
                            {
                                ++skipCount;
                            }

                            ch.WriteLine(string.Format("del: {0} skip: {1} total {2} ", delCount, skipCount, ++count));
                        }
                    }
                }
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void DELHOUSEINT(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(HouseIntervalsHouseInterval), new XmlRootAttribute("HouseInterval"));

            Console.Write("HouseInterval ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int delCount = 0;
                int skipCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    // loop through Object elements
                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.Name == "HouseInterval")
                        {
                            var aoId = reader.GetAttribute("HOUSEINTID");

                            var ao = context.HouseIntervals.Find(aoId);
                            if (null != ao)
                            {
                                context.HouseIntervals.Remove(ao);
                                context.SaveChanges();
                                ++delCount;
                            }
                            else
                            {
                                ++skipCount;
                            }

                            ch.WriteLine(string.Format("del: {0} skip: {1} total {2} ", delCount, skipCount, ++count));
                        }
                    }
                }
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }

        static void DELLANDMARK(FileInfo fi)
        {
            var serializer = new XmlSerializer(typeof(LandmarksLandmark), new XmlRootAttribute("Landmark"));

            Console.Write("Landmark ");
            var ch = new CursorHelper();

            FIASContext context = null;
            try
            {
                context = new FIASContext("name=DefaultConnection");
                context.Configuration.AutoDetectChangesEnabled = false;

                int count = 0;
                int delCount = 0;
                int skipCount = 0;

                using (XmlReader reader = XmlReader.Create(fi.FullName))
                {
                    reader.MoveToContent();

                    // loop through Object elements
                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.Name == "Landmark")
                        {
                            var aoId = reader.GetAttribute("LANDID");

                            var ao = context.Landmarks.Find(aoId);
                            if (null != ao)
                            {
                                context.Landmarks.Remove(ao);
                                context.SaveChanges();
                                ++delCount;
                            }
                            else
                            {
                                ++skipCount;
                            }

                            ch.WriteLine(string.Format("del: {0} skip: {1} total {2} ", delCount, skipCount, ++count));
                        }
                    }
                }
            }
            finally
            {
                if (context != null)
                    context.Dispose();
            }
        }
    }
}
