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
    class HouseTable
    {
        private static Dictionary<Guid, byte>_ActualIds = new Dictionary<Guid, byte>(30000000);

        private static void TrunTable()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                ConsoleHelper.WriteLine("truncate HOUSE");

                conn.Open();
                var cmd = conn.CreateCommand();

                cmd.CommandText = "truncate table[dbo].[HOUSE]";
                cmd.ExecuteNonQuery();

                try
                {
                    cmd.CommandText = "DROP INDEX [FK_AOID] ON [dbo].[HOUSE]";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "ALTER TABLE [dbo].[HOUSE] DROP CONSTRAINT [PK_dbo.HOUSE]";
                    cmd.ExecuteNonQuery();
                }
                catch (Exception)
                {
                }


                conn.Close();
            }
        }

        public static void CreateIndex()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                                
                ConsoleHelper.WriteLine("Start create PK_dbo.HOUSE");
                cmd.CommandText = "ALTER TABLE[dbo].[HOUSE] ADD CONSTRAINT[PK_dbo.HOUSE] PRIMARY KEY NONCLUSTERED([HOUSEGUID] ASC)";
                cmd.CommandTimeout = 3000;
                cmd.ExecuteNonQuery();

                ConsoleHelper.WriteLine("Start create FK_AOID");
                cmd.CommandText = "CREATE NONCLUSTERED INDEX [FK_AOID] ON [dbo].[HOUSE]([AOGUID] ASC)";
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
                    foreach (var h in db.HOUSES.Select(a => a.HOUSEGUID))
                    {
                        _ActualIds[h] = 0;
                    }
                }
                ConsoleHelper.WriteLine(string.Format("Load {0} HOUSEGUID", _ActualIds.Count));
            }
            return _ActualIds;
        }

        private static IEnumerable<DataTable> GetTables(FileInfo file)
        {
            var actualAOIds = AddrTable.GetActualIds();

            var proc = new Process
            {
                StartInfo = new ProcessStartInfo(@"C:\Program Files\7-Zip\7z.exe", "e -trar \"" + file.FullName + "\" -so  AS_DEL_HOUSE_*.*")
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
                            case "HOUSEID":
                                delRec[Guid.Parse(reader.Value)] = 0;
                                break;
                        }
                    }
                    reader.Read();
                }
            }

            proc.WaitForExit();
            Console.WriteLine();
            ConsoleHelper.WriteLine(string.Format("Load {0} deleted HOUSEID", delRec.Count));

            CursorHelper ch = null;
            DataTable dt = new DataTable();

            foreach (var prop in typeof(HOUSEBULK).GetProperties())
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
            }
            
            proc = new Process
            {
                StartInfo = new ProcessStartInfo(@"C:\Program Files\7-Zip\7z.exe", "e -trar \"" + file.FullName + "\" -so  AS_HOUSE_*.*")
                {
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Normal,
                    RedirectStandardOutput = true
                }
            };

            Console.WriteLine();
            ConsoleHelper.WriteLine("Start load House ...");
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
                            case "HOUSEGUID":
                            case "UPDATEDATE":
                            case "HOUSENUM":
                            case "ESTSTATUS":
                            case "BUILDNUM":
                            case "STRUCNUM":
                            case "STRSTATUS":
                                row[reader.Name] = reader.Value;
                                break;
                            case "POSTALCODE":
                            case "OKATO":
                            case "OKTMO":
                                if (long.TryParse(reader.Value, out long code))
                                {
                                    row[reader.Name] = code;
                                }
                                break;
                            case "HOUSEID":
                                if (delRec.ContainsKey(Guid.Parse(reader.Value)))
                                {
                                    isActual = false;
                                }
                                break;
                            case "AOGUID":
                                if (!actualAOIds.ContainsKey(Guid.Parse(reader.Value)))
                                {
                                    isActual = false;
                                }
                                else
                                {
                                    row[reader.Name] = reader.Value;
                                }
                                break;
                            case "STARTDATE":
                                if (DateTime.Parse(reader.Value) > cur_date)
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

                    var errPath = Path.Combine(Program.dataDir.FullName, "house_err.txt");
                    if (isActual)
                    {
                        if (_ActualIds.ContainsKey((Guid)row["HOUSEGUID"]))
                        {
                            _ActualIds[(Guid)row["HOUSEGUID"]] += 1;

                            using (StreamWriter w = new StreamWriter(errPath, true, Encoding.UTF8))
                            {
                                w.WriteLine(row["HOUSEGUID"]);
                                w.Close();
                            }
                        }
                        else
                        {
                            _ActualIds[(Guid)row["HOUSEGUID"]] = 0;
                        
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
                                ch.WriteLine(string.Format("load HOUSE: {0}; speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
                            }
                        }
                    }
                }
            }

            yield return dt;
            proc.WaitForExit();

            Console.WriteLine();
            ConsoleHelper.WriteLine(string.Format("End load HOUSE: {0}; avg speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
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
                bulkCopy.DestinationTableName = "HOUSE";

                foreach (var prop in typeof(HOUSEBULK).GetProperties())
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
