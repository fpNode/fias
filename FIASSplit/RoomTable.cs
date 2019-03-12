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
    public class ROOMBULK
    {
        public Guid ROOMGUID { get; set; }
        public Guid HOUSEGUID { get; set; }
        public int POSTALCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string FLATNUMBER { get; set; }
        public int FLATTYPE { get; set; }
        public string ROOMNUMBER { get; set; }
        public int ROOMTYPE { get; set; }
    }

    class RoomTable
    {
        private static Dictionary<Guid, byte>_ActualIds = new Dictionary<Guid, byte>(40000000);

        private static void TrunTable()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                ConsoleHelper.WriteLine("truncate ROOM");

                conn.Open();
                var cmd = conn.CreateCommand();

                cmd.CommandText = "truncate table[dbo].[ROOM]";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "DROP INDEX [FK_HOUSEGUID] ON [dbo].[ROOM]";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "ALTER TABLE [dbo].[ROOM] DROP CONSTRAINT [PK_dbo.ROOM]";
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        public static void CreateIndex()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();

                ConsoleHelper.WriteLine("Start create PK_dbo.ROOM");
                cmd.CommandText = "ALTER TABLE[dbo].[ROOM] ADD CONSTRAINT[PK_dbo.ROOM] PRIMARY KEY NONCLUSTERED([ROOMGUID] ASC)";
                cmd.CommandTimeout = 3000;
                cmd.ExecuteNonQuery();

                ConsoleHelper.WriteLine("Start create FK_HOUSEGUID");
                cmd.CommandText = "CREATE NONCLUSTERED INDEX [FK_HOUSEGUID] ON [dbo].[ROOM]([HOUSEGUID] ASC)";
                cmd.CommandTimeout = 3000;
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        private static IEnumerable<DataTable> GetTables(FileInfo file)
        {
            var actualHouseIds = HouseTable.GetActualIds();

            CursorHelper ch = null;
            DataTable dt = new DataTable();

            foreach (var prop in typeof(ROOMBULK).GetProperties())
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
            }
            
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo(@"C:\Program Files\7-Zip\7z.exe", "e -trar \"" + file.FullName + "\" -so  AS_ROOM_*.*")
                {
                    UseShellExecute = false,
                    WindowStyle = ProcessWindowStyle.Normal,
                    RedirectStandardOutput = true
                }
            };

            Console.WriteLine();
            ConsoleHelper.WriteLine("Start load Room ...");
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
                            case "ROOMGUID":
                            case "UPDATEDATE":
                            case "FLATNUMBER":
                            case "FLATTYPE":
                            case "ROOMNUMBER":
                            case "ROOMTYPE":
                                row[reader.Name] = reader.Value;
                                break;
                            case "POSTALCODE":
                                if (int.TryParse(reader.Value, out int code))
                                {
                                    row[reader.Name] = code;
                                }
                                break;
                            case "HOUSEGUID":
                                if (!actualHouseIds.ContainsKey(Guid.Parse(reader.Value)))
                                {
                                    isActual = false;
                                }
                                else
                                {
                                    row[reader.Name] = reader.Value;
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

                    if (isActual && !_ActualIds.ContainsKey((Guid)row["ROOMGUID"]))
                    {
                        _ActualIds[(Guid)row["ROOMGUID"]] = 0;

                        if(row["ROOMTYPE"] is DBNull)
                        {
                            row["ROOMTYPE"] = 0;
                        }

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
                            ch.WriteLine(string.Format("load ROOM: {0}; speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
                        }
                    }
                }
            }
            
            yield return dt;
            proc.WaitForExit();

            Console.WriteLine();
            ConsoleHelper.WriteLine(string.Format("End load ROOM: {0}; avg speed {1} row/s", bulkCnt.ToString("### ### ###"), (bulkCnt / (DateTime.Now - cur_date).TotalSeconds).ToString("### ###")));
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
                bulkCopy.DestinationTableName = "ROOM";

                foreach (var prop in typeof(ROOMBULK).GetProperties())
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
