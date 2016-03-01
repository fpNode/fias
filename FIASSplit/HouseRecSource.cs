using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace FIASSplit
{
    public class HOUSEREC
    {
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid HOUSEID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid HOUSEGUID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid AOGUID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public int POSTALCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long OKATO { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long OKTMO { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime UPDATEDATE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string HOUSENUM { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public int ESTSTATUS { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string BUILDNUM { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string STRUCNUM { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public int STRSTATUS { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime STARTDATE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime ENDDATE { get; set; }

        public bool isEquivalent(HOUSEREC n)
        {
            return (
                UPDATEDATE == n.UPDATEDATE &&
                AOGUID == n.AOGUID &&
                POSTALCODE == n.POSTALCODE &&
                OKATO == n.OKATO &&
                OKTMO == n.OKTMO &&
                HOUSENUM == n.HOUSENUM &&
                ESTSTATUS == n.ESTSTATUS &&
                BUILDNUM == n.BUILDNUM &&
                STRUCNUM == n.STRUCNUM &&
                STRSTATUS == n.STRSTATUS
               );
        }
    }

    public class HOUSEBULK
    {
        public Guid HOUSEGUID { get; set; }
        public Guid AOGUID { get; set; }
        public int POSTALCODE { get; set; }
        public long OKATO { get; set; }
        public long OKTMO { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string HOUSENUM { get; set; }
        public int ESTSTATUS { get; set; }
        public string BUILDNUM { get; set; }
        public string STRUCNUM { get; set; }
        public int STRSTATUS { get; set; }
    }

    class HouseXMLNode
    {
        static Regex re = new Regex("<House HOUSEID=\"(.*?)\" HOUSEGUID=\"(.*?)\"");
        static XmlSerializer Serializer = new XmlSerializer(typeof(HOUSEREC), new XmlRootAttribute("House"));

        public static string GetRecKey(string data)
        {
            var m = re.Match(data);        
            return m.Groups[2].Value + m.Groups[1].Value;
        }

        public static void FindNodeInFile(FileInfo f)
        {
            var reader = XmlReader.Create(f.FullName);
            reader.MoveToContent();
            reader.Read();

            var id = Guid.Parse("28c8cc94-03cd-4694-babe-30bb8e717949");

            var ch = new CursorHelper();
            int cnt = 0;

            while (reader.NodeType == XmlNodeType.Element)
            {
                var data = reader.ReadOuterXml();
                var obj = (HOUSEREC)Serializer.Deserialize(new StringReader(data));

                if (obj.HOUSEGUID == id)
                {
                    Console.WriteLine(data);
                }

                if (++cnt % 10000 == 0)
                {
                    ch.WriteLine(string.Format("process {0} rows", cnt));
                }
            }
        }

        public static IEnumerable<string> GetNodes(DirectoryInfo dir)
        {
            foreach (var f in FIASFile.SplitFiles(dir).GetFiles("AS_HOUSE_*.xml").OrderBy(fn => fn.Name.Substring(9, 2)))
            {
                var ids = new Dictionary<string, string>();

                var reader = XmlReader.Create(f.FullName);
                reader.MoveToContent();
                reader.Read();

                while (reader.NodeType == XmlNodeType.Element)
                {
                    var data = reader.ReadOuterXml();

                    var key = GetRecKey(data);

                    if (ids.ContainsKey(key))
                    {
                        Console.WriteLine("dublicate record {0}", data);
                    }
                    else
                    {
                        ids[key] = data;
                    }
                }

                foreach(var r in ids.Keys.OrderBy(s => s).ToList())
                {
                    yield return ids[r];
                }
            }
            yield break;
        }

        public static IEnumerable<string> GetOrderedNodes(DirectoryInfo dir)
        {
            if (dir == null || !dir.Exists)
            {
                yield break;
            }

            var reader = XmlReader.Create(Merge(dir).FullName);
            reader.MoveToContent();
            reader.Read();

            while (reader.NodeType == XmlNodeType.Element)
            {
                yield return reader.ReadOuterXml();
            }

            yield break;
        }

        public static IEnumerable<string> GetOrderedNodesFull(DirectoryInfo dir)
        {
            if (dir == null || !dir.Exists)
            {
                yield break;
            }

            var delRec = new Dictionary<string, bool>();
            var lst = FIASFile.ExtractFiles(dir).GetFiles("AS_DEL_HOUSE_*.xml");
            if (lst.Count() > 0)
            {
                var delReader = XmlReader.Create(lst.First().FullName);
                delReader.MoveToContent();
                delReader.Read();

                while (delReader.NodeType == XmlNodeType.Element)
                {
                    delRec[GetRecKey(delReader.ReadOuterXml())] = true;
                }
            }

            var en2 = GetNodes(dir).GetEnumerator();
            var d2 = en2.MoveNext() ? en2.Current : "";

            foreach (var n in GetNodes(dir))
            {
                if (!delRec.ContainsKey(GetRecKey(n)))
                {
                    yield return n;
                }
            }
        }

        public static HOUSEREC MathRecord(List<HOUSEREC> buf)
        {
            if (buf.Count(br => br.ENDDATE < DateTime.Now) == buf.Count())
            {
                return null;
            }
            else
            {
                var temp = new List<HOUSEREC>();
                temp.AddRange(buf);

                if (buf.Count > 1)
                {
                    buf.RemoveAll(br => br.ENDDATE < DateTime.Now);
                }

                if (buf.Count > 1)
                {
                    buf.RemoveAll(br => br.STARTDATE > DateTime.Now);
                }

                if (buf.Count > 1)
                {
                    var sd = buf.Select(br => br.STARTDATE).Max();
                    buf.RemoveAll(br => br.STARTDATE < sd);
                }

                if (buf.Count > 1)
                {
                    var ud = buf.Select(br => br.UPDATEDATE).Max();
                    buf.RemoveAll(br => br.UPDATEDATE < ud);
                }

                if (buf.Count != 1)
                {
                    foreach (var tr in temp)
                    {
                        Console.WriteLine(string.Format("{0} id: {1} s: {2} e:{3} u: {4}", tr.HOUSEGUID, tr.HOUSEID, tr.STARTDATE.ToShortDateString(), tr.ENDDATE.ToShortDateString(), tr.UPDATEDATE.ToShortDateString()));
                    }

                    throw new Exception("bad AddrObj records");
                }

                return buf.First();
            }
        }

        public static IEnumerable<HOUSEREC> GetActual(DirectoryInfo dir)
        {
            var buf = new List<HOUSEREC>();
            var prevId = Guid.Empty;
            var AOIds = new Dictionary<Guid, bool>();

            CursorHelper ch = null;
            foreach (var r in AddrXMLNode.GetActual(dir))
            {
                AOIds[r.AOGUID] = true;
                if(AOIds.Count % 10000 == 0)
                {
                    if (ch == null)
                    {
                        ch = new CursorHelper();
                    }
                    ch.WriteLine(string.Format("load {0} AOGUID", AOIds.Count));
                }
            }
            if (ch == null)
            {
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("load {0} AOGUID", AOIds.Count));

            foreach (var r in GetOrderedNodesFull(dir))
            {
                var obj = (HOUSEREC)Serializer.Deserialize(new StringReader(r));

                if (obj.HOUSEGUID != prevId)
                {
                    var res = MathRecord(buf);
                    if (res != null && AOIds.ContainsKey(res.AOGUID))
                    {
                        yield return res;
                    }
                    buf.Clear();
                    prevId = obj.HOUSEGUID;
                }
                buf.Add(obj);
            }

            var fres = MathRecord(buf);
            if (fres != null && AOIds.ContainsKey(fres.AOGUID))
            {
                yield return fres;
            }
            yield break;
        }

        public static IEnumerable<HOUSEREC> GetRecords(FileInfo file)
        {
            var reader = XmlReader.Create(file.FullName);
            reader.MoveToContent();
            reader.Read();

            while (reader.NodeType == XmlNodeType.Element)
            {
                yield return (HOUSEREC)Serializer.Deserialize(new StringReader(reader.ReadOuterXml())); ;
            }

            yield break;
        }

        public static void InsertRecords(IEnumerable<HOUSEREC> src)
        {
            CursorHelper ch = null;

            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();

            SqlBulkCopyOptions options = (SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock);
            var copy = new SqlBulkCopy(conn, options, null);
            copy.BatchSize = 5000;

            DataTable dt = new DataTable();

            foreach (var prop in typeof(HOUSEBULK).GetProperties())
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
                copy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(prop.Name, prop.Name));
            }

            copy.DestinationTableName = "HOUSE";
            copy.BulkCopyTimeout = 0;

            int bulkCnt = 0;

            // loop through Object elements
            foreach (var r in src)
            {
                DataRow row = dt.NewRow();

                row["HOUSEGUID"] = r.HOUSEGUID;
                row["AOGUID"] = r.AOGUID;

                if (r.POSTALCODE != 0)
                    row["POSTALCODE"] = r.POSTALCODE;
                if (r.OKATO != 0)
                    row["OKATO"] = r.OKATO;
                if (r.OKTMO != 0)
                    row["OKTMO"] = r.OKTMO;
                row["UPDATEDATE"] = r.UPDATEDATE;

                row["HOUSENUM"] = r.HOUSENUM;

                row["ESTSTATUS"] = r.ESTSTATUS;
                row["BUILDNUM"] = r.BUILDNUM;
                row["STRUCNUM"] = r.STRUCNUM;
                row["STRSTATUS"] = r.STRSTATUS;

                dt.Rows.Add(row);

                if (++bulkCnt % 10000 == 0)
                {
                    if (ch == null)
                    {
                        ch = new CursorHelper();
                    }
                    ch.WriteLine(string.Format("store: {0}", bulkCnt));
                    copy.WriteToServer(dt);
                    dt.Rows.Clear();
                }
            }
            copy.WriteToServer(dt);
            dt.Rows.Clear();
            if (ch == null)
            {
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("store: {0}", bulkCnt));
            copy.Close();
        }

        public static void DeleteRecords(IEnumerable<HOUSEREC> src)
        {
            var ch = new CursorHelper();

            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();

            int cnt = 0;

            var cmd = conn.CreateCommand();
            foreach (var r in src)
            {
                cmd.CommandText = string.Format("DELETE FROM [dbo].[HOUSE] WHERE [HOUSEGUID] = '{0}'", r.HOUSEGUID);
                cmd.ExecuteNonQuery();

                if (++cnt % 1000 == 0)
                {
                    ch.WriteLine(string.Format("delete {0}", cnt));
                }
            }
            ch.WriteLine(string.Format("delete {0}", cnt));
            conn.Close();
        }

        public static void Upload(DirectoryInfo dir)
        {
            InsertRecords(GetActual(dir));
        }

        public static void Update(DirectoryInfo dir)
        {
            var diffFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_HOUSE_DIFF.xml")));
            if (!diffFile.Exists)
            {
                Diff(dir);
            }

            Console.WriteLine("update from {0}", dir.FullName);

            var delFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_DEL_HOUSE_DIFF.xml")));
            if (delFile.Exists)
            {
                DeleteRecords(GetRecords(delFile));
            }

            InsertRecords(GetRecords(diffFile));
        }

        public static FileInfo Merge(DirectoryInfo dir)
        {
            var outFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_HOUSE_{0}.xml", dir.Name)));
            if (outFile.Exists)
                return outFile;

            Console.WriteLine("merge {0}", outFile.FullName);
            CursorHelper ch = null;

            var delRec = new Dictionary<string, bool>();
            var lst = FIASFile.ExtractFiles(dir).GetFiles("AS_DEL_HOUSE_*.xml");
            if (lst.Count() > 0)
            {
                var delReader = XmlReader.Create(lst.First().FullName);
                delReader.MoveToContent();
                delReader.Read();

                while (delReader.NodeType == XmlNodeType.Element)
                {
                    delRec[GetRecKey(delReader.ReadOuterXml())] = true;
                }
            }

            var writer = XmlWriter.Create(outFile.FullName);

            writer.WriteStartDocument();
            writer.WriteStartElement("ROOT");

            var en1 = GetOrderedNodes(FIASFile.GetPrevDir(dir)).GetEnumerator();
            var en2 = GetNodes(dir).GetEnumerator();

            var d1 = en1.MoveNext() ? en1.Current : "";
            var d2 = en2.MoveNext() ? en2.Current : "";

            int cnt = 0;

            while (!string.IsNullOrEmpty(d1) || !string.IsNullOrEmpty(d2))
            {
                string dataForStore = "";

                if (string.IsNullOrEmpty(d1))
                {
                    dataForStore = d2;
                    d2 = en2.MoveNext() ? en2.Current : "";
                }
                else if (string.IsNullOrEmpty(d2))
                {
                    dataForStore = d1;
                    d1 = en1.MoveNext() ? en1.Current : "";
                }
                else
                {
                    var ord = string.Compare(GetRecKey(d1), GetRecKey(d2));

                    if (ord < 0)
                    {
                        dataForStore = d1;
                        d1 = en1.MoveNext() ? en1.Current : "";
                    }
                    else if (ord == 0)
                    {
                        dataForStore = d2;
                        d2 = en2.MoveNext() ? en2.Current : "";
                        d1 = en1.MoveNext() ? en1.Current : "";
                    }
                    else
                    {
                        dataForStore = d2;
                        d2 = en2.MoveNext() ? en2.Current : "";
                    }
                }

                if (!delRec.ContainsKey(GetRecKey(dataForStore)))
                {
                    writer.WriteRaw(dataForStore);
                }

                if (++cnt % 10000 == 0)
                {
                    if (ch == null)
                    {
                        ch = new CursorHelper();
                    }
                    ch.WriteLine(string.Format("merge {0}", cnt));
                }
            }
            if (ch == null)
            {
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("merge {0}", cnt));

            writer.WriteEndElement();
            writer.WriteEndDocument();
            writer.Close();

            return outFile;
        }

        public static void Diff(DirectoryInfo dir)
        {
            var prevDir = FIASFile.GetPrevDir(dir);
            var prev = GetActual(prevDir).GetEnumerator();
            var cur = GetActual(dir).GetEnumerator();

            Console.WriteLine("make diff {0}", dir.FullName);
            CursorHelper ch = null;

            var delFile = new FileInfo(Path.Combine(dir.FullName, "AS_DEL_HOUSE_DIFF.xml"));
            var delWriter = XmlWriter.Create(delFile.FullName);
            delWriter.WriteStartDocument();
            delWriter.WriteStartElement("ROOT");

            var insFile = new FileInfo(Path.Combine(dir.FullName, "AS_HOUSE_DIFF.xml"));
            var insWriter = XmlWriter.Create(insFile.FullName);
            insWriter.WriteStartDocument();
            insWriter.WriteStartElement("ROOT");

            var prevRec = prev.MoveNext() ? prev.Current : null;
            var curRec = cur.MoveNext() ? cur.Current : null;

            XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
            ns.Add("", "");

            int cnt = 0;
            while (prevRec != null || curRec != null)
            {
                if (++cnt % 10000 == 0)
                {
                    if (ch == null)
                    {
                        ch = new CursorHelper();
                    }
                    ch.WriteLine(string.Format("process {0} rows", cnt));
                }

                if (prevRec == null)
                {
                    Serializer.Serialize(insWriter, curRec, ns);
                    curRec = cur.MoveNext() ? cur.Current : null;
                    continue;
                }
                else if (curRec == null)
                {
                    Serializer.Serialize(delWriter, prevRec, ns);
                    prevRec = prev.MoveNext() ? prev.Current : null;
                    continue;
                }

                var ord = string.Compare(prevRec.HOUSEGUID.ToString(), curRec.HOUSEGUID.ToString());

                if (ord < 0)
                {
                    Serializer.Serialize(delWriter, prevRec, ns);
                    prevRec = prev.MoveNext() ? prev.Current : null;
                }
                else if (ord == 0)
                {
                    if (!prevRec.isEquivalent(curRec))
                    {
                        Serializer.Serialize(delWriter, prevRec, ns);
                        Serializer.Serialize(insWriter, curRec, ns);
                    }
                    curRec = cur.MoveNext() ? cur.Current : null;
                    prevRec = prev.MoveNext() ? prev.Current : null;
                }
                else
                {
                    Serializer.Serialize(insWriter, curRec, ns);
                    curRec = cur.MoveNext() ? cur.Current : null;
                }
            }
            if (ch == null)
            {
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("process {0} rows", cnt));

            insWriter.WriteEndElement();
            insWriter.WriteEndDocument();
            insWriter.Close();

            delWriter.WriteEndElement();
            delWriter.WriteEndDocument();
            delWriter.Close();
        }
    }
}
