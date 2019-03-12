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
    public class ADDROBJBULK
    {
        public Guid AOGUID { get; set; }
        public Guid PARENTGUID { get; set; }
        public string FORMALNAME { get; set; }
        public string OFFNAME { get; set; }
        public string SHORTNAME { get; set; }
        public int POSTALCODE { get; set; }
        public long OKATO { get; set; }
        public long OKTMO { get; set; }
        public int AOLEVEL { get; set; }
        public string KLADRCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
    }

    public class ADDROBJREC
    {
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid AOGUID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid AOID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid PREVID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid NEXTID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public byte LIVESTATUS { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public Guid PARENTGUID { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime STARTDATE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime ENDDATE { get; set; }

        //FIASCODE = [REGIONCODE] + [AUTOCODE] + [AREACODE] + [CITYCODE] + [CTARCODE] + [PLACECODE] + [STREETCODE] + [EXTRCODE] + [SEXTCODE]
        [XmlIgnore]
        public string FIASCODE { 
            get
            {
                return REGIONCODE + AUTOCODE + AREACODE + CITYCODE + CTARCODE + PLACECODE + STREETCODE + EXTRCODE + SEXTCODE;
            }
        }
        
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string REGIONCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string AUTOCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string AREACODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string CITYCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string CTARCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string PLACECODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string STREETCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string EXTRCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string SEXTCODE { get; set; }

        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string FORMALNAME { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string OFFNAME { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string SHORTNAME { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public int POSTALCODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long OKATO { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long OKTMO { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public int AOLEVEL { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string CODE { get; set; }
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public DateTime UPDATEDATE { get; set; }

        public bool isEquivalent(ADDROBJREC n)
        {
            return (
                UPDATEDATE == n.UPDATEDATE &&
                PARENTGUID == n.PARENTGUID &&
                FIASCODE == n.FIASCODE &&
                FORMALNAME == n.FORMALNAME &&
                OFFNAME == n.OFFNAME &&
                SHORTNAME == n.SHORTNAME &&
                POSTALCODE == n.POSTALCODE &&
                OKATO == n.OKATO &&
                OKTMO == n.OKTMO &&
                AOLEVEL == n.AOLEVEL &&
                CODE == n.CODE &&
                STARTDATE == n.STARTDATE &&
                ENDDATE == n.ENDDATE
               );
        }
    }

    class AddrXMLNode
    {
        static Regex re = new Regex("<Object AOID=\"(.{36})\" AOGUID=\"(.{36})\"");
        static XmlSerializer Serializer = new XmlSerializer(typeof(ADDROBJREC), new XmlRootAttribute("Object"));
        public static Dictionary<Guid, byte> _ActualIds = new Dictionary<Guid, byte>(1500000);

        public static int c0 = 0;
        public static int c1 = 0;
        public static int c2 = 0;
        public static int c3 = 0;

        public static Dictionary<Guid, byte> GetActualIds()
        {
            if(_ActualIds.Count == 0)
            {
                using (var db = new FIASContext())
                {
                    foreach (var g in db.ADDROBJS.Select(a => a.AOGUID))
                    {
                        _ActualIds[g] = 0;
                    }
                }
            }
            return _ActualIds;
        }

        public static string GetRecKey(string data)
        {
            var m = re.Match(data);        
            return m.Groups[2].Value + m.Groups[1].Value;
        }

        public static IEnumerable<string> GetNodes(DirectoryInfo dir)
        {
            foreach (var f in FIASFile.SplitFiles(dir).GetFiles("AS_ADDROBJ_*.xml").OrderBy(fn => fn.Name.Substring(11, 2)))
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

        public static void FindNodeInFile(FileInfo f)
        {
            var reader = XmlReader.Create(f.FullName);
            reader.MoveToContent();
            reader.Read();

            var id = Guid.Parse("7a9b16b8-31bf-4a6d-bd33-827400670981");

            var ch = new CursorHelper();
            int cnt = 0;

            while (reader.NodeType == XmlNodeType.Element)
            {
                var data = reader.ReadOuterXml();
                var obj = (ADDROBJREC)Serializer.Deserialize(new StringReader(data));

                if (obj.AOID == id || obj.AOGUID == id)
                {
                    Console.WriteLine(data);
                }

                if (++cnt % 10000 == 0)
                {
                    ch.WriteLine(string.Format("process {0} rows", cnt));
                }
            }
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
            var lst = FIASFile.ExtractFiles(dir).GetFiles("AS_DEL_ADDROBJ_*.xml");
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

            foreach (var n in GetNodes(dir))
            {
                if (!delRec.ContainsKey(GetRecKey(n)))
                {
                    yield return n;
                }
            }
        }

        private static void WriteBuffer(List<ADDROBJREC> buf)
        {
            var log = new FileInfo(Path.Combine(Program.dataDir.FullName, string.Format("addr_{0}.txt", buf.First().AOGUID)));
            using (StreamWriter w = log.CreateText())
            {
                foreach (var tr in buf)
                {
                    Serializer.Serialize(w, tr);
                }
                w.Close();
            }
        }

        private static ADDROBJREC MathRecord(List<ADDROBJREC> buf)
        {
            try
            {
                return buf.SingleOrDefault(r => r.LIVESTATUS == 1 && r.ENDDATE > DateTime.Now);
            }
            catch (Exception)
            {
                var rec = buf.FirstOrDefault(r => r.NEXTID != default(Guid));
                if (rec != null)
                {
                    WriteBuffer(buf);

                    // перебираем по NEXTID
                    while (rec != null && rec.NEXTID != default(Guid))
                    {
                        Debug.Assert(buf.Where(r => r.AOID == rec.NEXTID).Count() == 1); // запись с таким ID единственная
                        rec = buf.First(r => r.AOID == rec.NEXTID);
                    }
                    ++c2;
                }
                else
                {
                    WriteBuffer(buf);

                    Debug.Assert(buf.Count(r => r.PREVID == default(Guid)) == 1); // запись у которой нет PREVID единственная
                    rec = buf.First(r => r.PREVID == default(Guid));
                    ++c3;
                }
                Debug.Assert(rec.ENDDATE > DateTime.Now);
                Debug.Assert(rec.LIVESTATUS == 1);
                return rec.LIVESTATUS == 1 && rec.ENDDATE > DateTime.Now ? rec : null;
            }
        }

        public static IEnumerable<ADDROBJREC> GetActual(DirectoryInfo dir)
        {
            var buf = new List<ADDROBJREC>();
            var prevId = Guid.Empty;

            foreach (var r in GetOrderedNodesFull(dir))
            {
                var obj = (ADDROBJREC)Serializer.Deserialize(new StringReader(r));

                if (obj.AOGUID != prevId)
                {
                    var res = MathRecord(buf);
                    if (res != null)
                    {
                        _ActualIds[res.AOGUID] = 0;
                        yield return res;
                    }
                    buf.Clear();
                    prevId = obj.AOGUID;
                }
                buf.Add(obj);
            }

            var fres = MathRecord(buf);
            if (fres != null)
            {
                Debug.Assert(fres.ENDDATE > DateTime.Now);
                Debug.Assert(fres.LIVESTATUS == 1);

                _ActualIds[fres.AOGUID] = 0;
                yield return fres;
            }

            yield break;
        }

        public static IEnumerable<ADDROBJREC> GetActual2(FileInfo file)
        {

            var delRec = new HashSet<string>();
  
            foreach (var r in FIASFile.ExtractNodes(file, "AS_DEL_ADDROBJ_"))
            {
                delRec.Add(GetRecKey(r));
            }

            var cur_date = DateTime.Now;
            foreach (var n in FIASFile.ExtractNodes(file, "AS_ADDROBJ_"))
            {
                if (!delRec.Contains(GetRecKey(n)))
                {
                    var obj = (ADDROBJREC)Serializer.Deserialize(new StringReader(n));
                    
                    if (obj.LIVESTATUS == 1 && obj.ENDDATE > cur_date)
                    {
                        if(_ActualIds.ContainsKey(obj.AOGUID))
                        {
                            var log = new FileInfo(Path.Combine(Program.dataDir.FullName, "addr.txt"));
                            using (StreamWriter w = log.CreateText())
                            {
                                w.WriteLine(n);
                                w.Close();
                            }
                        }
                        else
                        {
                            _ActualIds[obj.AOGUID] = 0;
                            yield return obj;
                        }
                    }
                }
            }
            yield break;
        }

        public static IEnumerable<ADDROBJREC> GetRecords(FileInfo file)
        {
            var reader = XmlReader.Create(file.FullName);
            reader.MoveToContent();
            reader.Read();

            while (reader.NodeType == XmlNodeType.Element)
            {
                yield return (ADDROBJREC)Serializer.Deserialize(new StringReader(reader.ReadOuterXml())); ;
            }

            yield break;
        }

        public static void InsertRecords(IEnumerable<ADDROBJREC> src)
        {
            CursorHelper ch = null;

            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();

            SqlBulkCopyOptions options = (SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock);
            var copy = new SqlBulkCopy(conn, options, null);
            copy.BatchSize = 5000;

            DataTable dt = new DataTable();

            foreach (var prop in typeof(ADDROBJBULK).GetProperties())
            {
                dt.Columns.Add(new DataColumn(prop.Name, prop.PropertyType));
                copy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(prop.Name, prop.Name));
            }

            copy.DestinationTableName = "ADDROBJ";
            copy.BulkCopyTimeout = 0;

            int bulkCnt = 0;

            foreach (var r in src)
            {
                DataRow row = dt.NewRow();

                row["AOGUID"] = r.AOGUID;

                if (r.PARENTGUID != default(Guid))
                    row["PARENTGUID"] = r.PARENTGUID;
                row["FORMALNAME"] = r.FORMALNAME;
                row["OFFNAME"] = r.OFFNAME;
                row["SHORTNAME"] = r.SHORTNAME;
                if (r.POSTALCODE != 0)
                    row["POSTALCODE"] = r.POSTALCODE;
                if (r.OKATO != 0)
                    row["OKATO"] = r.OKATO;
                if (r.OKTMO != 0)
                    row["OKTMO"] = r.OKTMO;
                row["AOLEVEL"] = r.AOLEVEL;
                row["KLADRCODE"] = r.CODE;
                row["UPDATEDATE"] = r.UPDATEDATE;

                dt.Rows.Add(row);

                if (++bulkCnt % 10000 == 0)
                {
                    if (ch == null)
                    {
                        Console.WriteLine();
                        ch = new CursorHelper();
                    }

                    ch.WriteLine(string.Format("store AO: {0}", bulkCnt.ToString("### ### ###")));
                    copy.WriteToServer(dt);
                    dt.Rows.Clear();
                }
            }
            copy.WriteToServer(dt);
            dt.Rows.Clear();
            if (ch == null)
            {
                Console.WriteLine();
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("store AO: {0}", bulkCnt.ToString("### ### ###")));
            copy.Close();
        }

        public static void DeleteRecords(IEnumerable<ADDROBJREC> src)
        {
            var ch = new CursorHelper();

            SqlConnection conn = null;
            conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString);
            conn.Open();

            int cnt = 0;

            var cmd = conn.CreateCommand();
            foreach(var r in src)
            { 
                cmd.CommandText = string.Format("DELETE FROM [dbo].[ADDROBJ] WHERE [AOGUID] = '{0}'", r.AOGUID);
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

        public static void Upload2(FileInfo file)
        {
            InsertRecords(GetActual2(file));
        }

        public static void Update(DirectoryInfo dir)
        {
            var diffFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_ADDROBJ_DIFF.xml")));
            if (!diffFile.Exists)
            {
                Diff(dir);
            }

            var delFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_DEL_ADDROBJ_DIFF.xml")));
            if (delFile.Exists)
            {
                Console.WriteLine("delete from {0}", delFile.FullName);
                DeleteRecords(GetRecords(delFile));
            }

            Console.WriteLine("insert from {0}", diffFile.FullName);
            InsertRecords(GetRecords(diffFile));
        }

        public static FileInfo Merge(DirectoryInfo dir)
        {
            var outFile = new FileInfo(Path.Combine(dir.FullName, string.Format("AS_ADDROBJ_{0}.xml", dir.Name)));
            if (outFile.Exists)
                return outFile;
            
            Console.WriteLine("merge {0}", outFile.FullName);
            CursorHelper ch = null;

            var delRec = new Dictionary<string, bool>();
            var lst = FIASFile.ExtractFiles(dir).GetFiles("AS_DEL_ADDROBJ_*.xml");
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
            var prev = GetActual(FIASFile.GetPrevDir(dir)).GetEnumerator();
            var cur  = GetActual(dir).GetEnumerator();

            Console.WriteLine("make diff {0}", dir.FullName);
            CursorHelper ch = null;

            var delFile = new FileInfo(Path.Combine(dir.FullName, "AS_DEL_ADDROBJ_DIFF.xml"));
            var delWriter = XmlWriter.Create(delFile.FullName);
            delWriter.WriteStartDocument();
            delWriter.WriteStartElement("ROOT");

            var insFile = new FileInfo(Path.Combine(dir.FullName, "AS_ADDROBJ_DIFF.xml"));
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

                if (curRec == null )
                {
                    Serializer.Serialize(delWriter, prevRec, ns);
                    prevRec = prev.MoveNext() ? prev.Current : null;
                    continue;
                }

                var ord = string.Compare(prevRec.AOGUID.ToString(), curRec.AOGUID.ToString());

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
