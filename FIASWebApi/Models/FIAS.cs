using System;
using SD.Tools.Algorithmia.GeneralDataStructures;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Configuration;
using System.Linq;

namespace fpNode.FIAS
{
    public static class SqlHelpers
    {
        public static string SafeGetString(this SqlDataReader reader, string colName)
        {
           int index = reader.GetOrdinal(colName);
           if(!reader.IsDBNull(index))
               return reader.GetString(index);
           else 
               return string.Empty;
        }

        public static Guid SafeGetGuid(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return Guid.Parse(reader.GetString(index));
            else
                return Guid.Empty;
        }

        public static int SafeGetInt(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return int.Parse(reader.GetString(index));
            else
                return 0;
        }

        public static ulong SafeGetUlong(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return ulong.Parse(reader.GetString(index));
            else
                return 0;
        }
    }

    public class Node
    {
        static Dictionary<Guid, Node> Nodes = new Dictionary<Guid, Node>();
        static MultiValueDictionary<string, Node> NodeMap = new MultiValueDictionary<string, Node>();
     
        public Guid Id { get; set; }
        public Node Parent { get; set; }
        public string FIASCODE { get; set; }
        public string FORMALNAME { get; set; }
        public string OFFNAME { get; set; }
        public int POSTALCODE { get; set; }
        public ulong OKATO { get; set; }
        public ulong OKTMO { get; set; }
        public int AOLEVEL { get; set; }
        public string KLADRCODE { get; set; }

        public string FullName { 
            get 
            {
                string result = OFFNAME;

                foreach(var p in GetAncestors())
                {
                    result += ", " + p.OFFNAME;
                }
                return result;
            }
        }

        class NodeRecord
        {
            public static string CommandText
            {
                get { return string.Format("select [AOID], [AOGUID], [PARENTGUID], [FORMALNAME], [OFFNAME], [SHORTNAME], [PREVID], [NEXTID], [REGIONCODE] + [AUTOCODE] + [AREACODE] + [CITYCODE] + [CTARCODE] + [PLACECODE] + [STREETCODE] + [EXTRCODE] + [SEXTCODE] as FIASCODE, [POSTALCODE], [OKATO], [OKTMO], [SHORTNAME], [AOLEVEL], [PLAINCODE] from  [dbo].[ADDROBJ] where LIVESTATUS = '1'"); }
            }

            public static NodeRecord Read(SqlDataReader reader)
            {
                NodeRecord nr = new NodeRecord();

                nr.AOID       = reader.SafeGetGuid("AOID");
                nr.AOGUID     = reader.SafeGetGuid("AOGUID");
                nr.PARENTGUID = reader.SafeGetGuid("PARENTGUID");
                nr.PREVID     = reader.SafeGetGuid("PREVID");
                nr.NEXTID     = reader.SafeGetGuid("NEXTID");
                nr.FORMALNAME = reader.SafeGetString("FORMALNAME");
                nr.OFFNAME    = reader.SafeGetString("OFFNAME");
                nr.FIASCODE   = reader.SafeGetString("FIASCODE");
                nr.POSTALCODE = reader.SafeGetInt("POSTALCODE");
                nr.OKATO      = reader.SafeGetUlong("OKATO");
                nr.OKTMO      = reader.SafeGetUlong("OKTMO");
                nr.AOLEVEL    = reader.SafeGetInt("AOLEVEL");
                nr.KLADRCODE  = reader.SafeGetString("PLAINCODE").PadRight(20, '0');

                if (nr.FORMALNAME == nr.OFFNAME)
                {
                    nr.OFFNAME = reader.SafeGetString("SHORTNAME") + ". " + nr.OFFNAME;
                }

                return nr;
            }

            public Guid AOID { get; set; }
            public Guid AOGUID { get; set; }
            public Guid PARENTGUID { get; set; }
            public Guid PREVID { get; set; }
            public Guid NEXTID { get; set; }
            public string FIASCODE { get; set; }
            public string FORMALNAME { get; set; }
            public string OFFNAME { get; set; }
            public int POSTALCODE { get; set; }
            public ulong OKATO { get; set; }
            public ulong OKTMO { get; set; }
            public int AOLEVEL { get; set; }
            public string KLADRCODE { get; set; }
        }

        static Node Create(NodeRecord r)
        {
            Node node = new Node();

            node.Id = r.AOGUID;
            node.FORMALNAME = r.FORMALNAME;
            node.FIASCODE = r.FIASCODE;
            node.OFFNAME = r.OFFNAME;
            node.POSTALCODE = r.POSTALCODE;
            node.OKATO = r.OKATO;
            node.OKTMO = r.OKTMO;
            node.AOLEVEL = r.AOLEVEL;
            node.KLADRCODE = r.KLADRCODE;

            if (r.PARENTGUID != Guid.Empty)
            {
                node.Parent = Nodes[r.PARENTGUID];
            }

            Nodes.Add(node.Id, node);

            return node;
        }
        
        public static void LoadData(string sqlConnStr)
        {
            var records = new Dictionary<Guid, List<NodeRecord>>();
            var q = new Queue<NodeRecord>();

            using (SqlConnection conn = new SqlConnection(sqlConnStr))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = NodeRecord.CommandText;

                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var n = NodeRecord.Read(reader);

                    if (records.Keys.Contains(n.AOGUID))
                    {
                        records[n.AOGUID].Add(n);
                    }
                    else
                    {
                        var l = new List<NodeRecord>() { n };
                        records.Add(n.AOGUID, l);
                    }
                }
                reader.Close();
                conn.Close();
            }

            foreach (var r in records)
            {
                if (r.Value.Count > 1)
                {
                    var prev = r.Value.Where(nr => nr.PREVID != Guid.Empty).Select(pr => pr.PREVID);
                    var lst = r.Value.Where(nr => !prev.Contains(nr.AOID) && nr.PREVID != Guid.Empty).ToList();

                    if (lst.Count > 1)
                    {
                        throw (new Exception("unexpected data"));
                    }
                    else
                    {
                        var nr = lst.First();
                        q.Enqueue(nr);
                    }
                }
                else
                {
                    var nr = r.Value.First();
                    q.Enqueue(nr);
                }
            }

            while (q.Count > 0)
            {
                var nr = q.Dequeue();
                if (nr.PARENTGUID == Guid.Empty || Node.Nodes.ContainsKey(nr.PARENTGUID))
                {
                    Node n = Node.Create(nr);

                    var cn = n;
                    do
                    {
                        foreach (var s in ParceTags(cn.FORMALNAME.ToUpper()))
                        {
                            NodeMap.Add(s, n);
                        }
                        cn = cn.Parent;
                    } while (cn != null);
                }
                else
                {
                    q.Enqueue(nr);
                }
            }
        }

        public static IEnumerable<Node> FindNodes(string query)
        {
            var pts = ParceTags(query.ToUpper()).ToList();

            var lp = pts.Last();
            pts.RemoveAt(pts.Count - 1);

            var result = new List<Node>();

            foreach (var s in NodeMap.Keys.Where(v => v.StartsWith(lp)))
            {
                result.AddRange(NodeMap[s]);
            }

            foreach (var n in pts)
            {
                result = result.Intersect(NodeMap[n]).ToList();
            }

            var model = new List<Node>();

            return result;
        }

        static string[] ParceTags(string name)
        {
            return name.Split(' ');
        }

        public IEnumerable<Node> GetAncestors()
        {
            for (Node curItem = Parent; !ReferenceEquals(curItem, null); curItem = curItem.Parent)
            {
                yield return curItem;
            }
        }
    }
}