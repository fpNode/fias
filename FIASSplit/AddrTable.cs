using FIASSplit.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace FIASSplit
{
    class AddrTable
    {
        private static Dictionary<Guid, byte> _ActualIds = new Dictionary<Guid, byte>(1500000);

        private static void TrunTable()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                ConsoleHelper.WriteLine("truncate ADDROBJ");

                conn.Open();
                var cmd = conn.CreateCommand();

                cmd.CommandText = "truncate table[dbo].[ADDROBJ]";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "ALTER TABLE [dbo].[ADDROBJ] DROP CONSTRAINT [PK_dbo.ADDROBJ]";
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        private static void CreateIndex()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();

                ConsoleHelper.WriteLine("Start create PK_dbo.ADDROBJ");
                cmd.CommandText = "ALTER TABLE[dbo].[ADDROBJ] ADD CONSTRAINT[PK_dbo.ADDROBJ] PRIMARY KEY NONCLUSTERED([AOGUID] ASC)";
                cmd.CommandTimeout = 3000;
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        public static Dictionary<Guid, byte> GetActualIds()
        {
            if (_ActualIds.Count == 0)
            {
                using (var db = new FIASContext())
                {
                    foreach (var h in db.ADDROBJS.Select(a => a.AOGUID))
                    {
                        _ActualIds[h] = 0;
                    }
                }
                ConsoleHelper.WriteLine(string.Format("Load {0} ADDR", _ActualIds.Count));
            }
            return _ActualIds;
        }

        private static IEnumerable<DataTable> GetTables(FileInfo file)
        {
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo(@"C:\Program Files\7-Zip\7z.exe", "e -trar \"" + file.FullName + "\" -so  AS_DEL_ADDROBJ_*.*")
                {
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Normal,
                    RedirectStandardOutput = true
                }
            };

            proc.Start();

            var delRec = new Dictionary<Guid, byte>(2000);

            if (!proc.StandardOutput.EndOfStream)
            {
                var reader = XmlReader.Create(proc.StandardOutput);
                reader.MoveToContent();
                reader.Read();

                // loop through Object elements
                while (reader.NodeType == XmlNodeType.Element)
                {
                    while (reader.MoveToNextAttribute())
                    {
                        switch (reader.Name)
                        {
                            case "AOID":
                                delRec[Guid.Parse(reader.Value)] = 0;
                                break;
                        }
                    }
                    reader.Read();
                }
            }

            proc.WaitForExit();
            Console.WriteLine();
            ConsoleHelper.WriteLine(string.Format("Load {0} deleted AOID", delRec.Count));

            CursorHelper ch = null;
            DataTable dt = new DataTable();

            foreach (var prop in typeof(ADDROBJBULK).GetProperties())
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
            }
            
            proc = new Process
            {
                StartInfo = new ProcessStartInfo(@"C:\Program Files\7-Zip\7z.exe", "e -trar \"" + file.FullName + "\" -so  AS_ADDROBJ_*.*")
                {
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Normal,
                    RedirectStandardOutput = true
                }
            };

            Console.WriteLine();
            ConsoleHelper.WriteLine("Start load Addr ...");
            proc.Start();
                       

            int bulkCnt = 1;
            var cur_date = DateTime.Now;

            if (!proc.StandardOutput.EndOfStream)
            {
                var reader = XmlReader.Create(proc.StandardOutput);
                reader.MoveToContent();
                reader.Read();

                // loop through Object elements

                DataRow row = dt.NewRow();
                while (reader.NodeType == XmlNodeType.Element)
                {
                    bool isActual = true;
                    row = dt.NewRow();
                    while (reader.MoveToNextAttribute())
                    {
                        switch (reader.Name)
                        {
                            case "AOGUID":
                            case "PARENTGUID":
                            case "FORMALNAME":
                            case "OFFNAME":
                            case "SHORTNAME":
                            case "AOLEVEL":
                            case "UPDATEDATE":
                                row[reader.Name] = reader.Value;
                                break;
                            case "CODE":
                                row["KLADRCODE"] = reader.Value;
                                break;
                            case "POSTALCODE":
                            case "OKATO":
                            case "OKTMO":
                                if (int.TryParse(reader.Value, out int code))
                                {
                                    row[reader.Name] = code;
                                }
                                break;
                            case "AOID":
                                if (delRec.ContainsKey(Guid.Parse(reader.Value)))
                                {
                                    isActual = false;
                                }
                                break;
                            case "LIVESTATUS":
                                if (reader.Value != "1")
                                {
                                    isActual = false;
                                }
                                break;
                            case "ENDDATE":
                                if (DateTime.Parse(reader.Value) < cur_date)
                                {
                                    isActual = false;
                                }
                                break;
                        }
                    }
                    reader.Read();

                    if (isActual && !_ActualIds.ContainsKey((Guid)row["AOGUID"]))
                    {
                        _ActualIds[(Guid)row["AOGUID"]] = 0;

                        dt.Rows.Add(row);

                        if (++bulkCnt % 5000 == 0)
                        {
                            yield return dt;
                            dt = dt.Clone();

                            if (ch == null)
                            {
                                Console.WriteLine();
                                Console.WriteLine();
                                ch = new CursorHelper();
                            }
                            ch.WriteLine(string.Format("load Addr: {0}; speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
                        }
                    }
                }
            }
            yield return dt;
            proc.WaitForExit();
            
            Console.WriteLine();
            ConsoleHelper.WriteLine(string.Format("End load Addr: {0}; avg speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
        }

        public static void Upload(FileInfo file)
        {
            TrunTable();

            //Parallel.ForEach(GetTables(file), table =>
            //{
            //    BulkLoadData(table);
            //});

            foreach (var table in GetTables(file))
            {
                BulkLoadData(table);
            }

            CreateIndex();
        }

        private static void BulkLoadData(DataTable dt)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = "ADDROBJ";

                foreach (var prop in typeof(ADDROBJBULK).GetProperties())
                {
                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(prop.Name, prop.Name));
                }

                conn.Open();
                bulkCopy.WriteToServer(dt);
                bulkCopy.Close();
            }
        }
    }
}
