using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIASWeb
{
    static class SqlHelpers
    {
        public static string SafeGetString(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return reader.GetString(index);
            else
                return string.Empty;
        }

        public static Guid SafeGetGuid(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return reader.GetGuid(index);
            else
                return Guid.Empty;
        }

        public static int SafeGetInt(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return reader.GetInt32(index);
            else
                return 0;
        }

        public static ulong SafeGetUlong(this SqlDataReader reader, string colName)
        {
            int index = reader.GetOrdinal(colName);
            if (!reader.IsDBNull(index))
                return (ulong)reader.GetInt64(index);
            else
                return 0;
        }
    }

    public class HouseNode
    {
        public Guid HOUSEGUID { get; set; }
        public Guid AOGUID { get; set; }
        public int POSTALCODE { get; set; }
        public ulong OKATO { get; set; }
        public ulong OKTMO { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string HOUSENUM { get; set; }
        public int ESTSTATUS { get; set; }
        public string BUILDNUM { get; set; }
        public string STRUCNUM { get; set; }
        public int STRSTATUS { get; set; }

        public AddrNode Parent { get; set; }

        public HouseNode(SqlDataReader reader)
        {
            HOUSEGUID = reader.SafeGetGuid("HOUSEGUID");
            AOGUID = reader.SafeGetGuid("AOGUID");
            POSTALCODE = reader.SafeGetInt("POSTALCODE");
            OKATO = reader.SafeGetUlong("OKATO");
            OKTMO = reader.SafeGetUlong("OKTMO");
            UPDATEDATE = reader.GetDateTime(reader.GetOrdinal("UPDATEDATE"));
            HOUSENUM = reader.SafeGetString("HOUSENUM");
            ESTSTATUS  = reader.SafeGetInt("ESTSTATUS");
            BUILDNUM = reader.SafeGetString("BUILDNUM");
            STRUCNUM = reader.SafeGetString("STRUCNUM");
            STRSTATUS = reader.SafeGetInt("STRSTATUS");
        }
    }

    public class RoomNode
    {
        public Guid ROOMGUID { get; set; }
        public Guid HOUSEGUID { get; set; }
        public int POSTALCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string FLATNUMBER { get; set; }
        public int FLATTYPE { get; set; }
        public string ROOMNUMBER { get; set; }
        public int ROOMTYPE { get; set; }

        public HouseNode Parent { get; set; }

        public RoomNode(SqlDataReader reader)
        {
            ROOMGUID = reader.SafeGetGuid("ROOMGUID");
            HOUSEGUID = reader.SafeGetGuid("HOUSEGUID");
            POSTALCODE = reader.SafeGetInt("POSTALCODE");
            UPDATEDATE = reader.GetDateTime(reader.GetOrdinal("UPDATEDATE"));
            FLATNUMBER = reader.SafeGetString("FLATNUMBER");
            FLATTYPE = reader.SafeGetInt("FLATTYPE");
            ROOMNUMBER = reader.SafeGetString("ROOMNUMBER");
            ROOMTYPE = reader.SafeGetInt("ROOMTYPE");
        }
    }

    public class AddrNode
    {
        public static Dictionary<Guid, AddrNode> Nodes = new Dictionary<Guid, AddrNode>();
        static Dictionary<string, List<AddrNode>> NodeDic = new Dictionary<string, List<AddrNode>>();
        static Dictionary<string, List<string>> NodeKey = new Dictionary<string, List<string>>();

        public static OKTMO.OKTMOService oktmo = new OKTMO.OKTMOService();

        public AddrNode(SqlDataReader reader)
        {
            AOGUID = reader.SafeGetGuid("AOGUID");
            PARENTGUID = reader.SafeGetGuid("PARENTGUID");
            FORMALNAME = reader.SafeGetString("FORMALNAME");

            var s = reader.SafeGetString("OFFNAME");
            OFFNAME = FORMALNAME == s ? FORMALNAME : s;
            SHORTNAME = reader.SafeGetString("SHORTNAME");
            POSTALCODE = reader.SafeGetInt("POSTALCODE");
            OKATO = reader.SafeGetUlong("OKATO");
            OKTMO = reader.SafeGetUlong("OKTMO");
            AOLEVEL = reader.SafeGetInt("AOLEVEL");
            KLADRCODE = reader.SafeGetString("KLADRCODE").PadRight(20, '0');
            UPDATEDATE = reader.GetDateTime(reader.GetOrdinal("UPDATEDATE"));
        }

        public Guid AOGUID { get; set; }
        public Guid PARENTGUID { get; set; }
        public AddrNode Parent { get; set; }

        public ulong Id { get;  set; }
        public ulong NextChildId;
        public ulong ChildIdMask;

        public string FIASCODE { get; set; }
        public string FORMALNAME { get; set; }
        public string OFFNAME { get; set; }
        public string SHORTNAME { get; set; }
        public int POSTALCODE { get; set; }
        public ulong OKATO { get; set; }
        public ulong OKTMO { get; set; }
        public int AOLEVEL { get; set; }
        public string KLADRCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string OKTMOName { get; set; }

        public string FullName
        {
            get
            {
                string result = OFFNAME + " " + SHORTNAME;

                for (AddrNode curItem = Parent; !ReferenceEquals(curItem, null); curItem = curItem.Parent)
                {
                    result += ", " + curItem.OFFNAME + " " + curItem.SHORTNAME;
                }
                return result;
            }
        }

        static string TagHead(string tag)
        {
            return tag.Count() >= 2 ? tag.Substring(0, 2) : tag;
        }

        public static IEnumerable<string> ParceTags(string text)
        {
            var q = text.ToUpper();
            return q.Split(' ').Where(t => t != "");
        }

        public static IEnumerable<AddrNode> FindNodes(string query)
        {
            var tags = new Queue<string>(AddrNode.ParceTags(query));

            var result = new List<AddrNode>();

            if (tags.Count == 0)
                return result;

            var firstTag = tags.Dequeue();

            var fkey = TagHead(firstTag);
            if (NodeKey.ContainsKey(fkey))
            {
                foreach (var subTag in NodeKey[fkey].Where(k => k.StartsWith(firstTag)))
                {
                    result.AddRange(NodeDic[subTag]);
                }
            }

            if (result.Count == 0)
                return result;

            foreach (var tag in tags)
            {
                var stepResult = new List<AddrNode>();

                var k1 = TagHead(tag);

                if (NodeKey.ContainsKey(k1))
                {
                    foreach (var subTag in NodeKey[k1].Where(k => k.StartsWith(tag)))
                    {
                        stepResult.AddRange(result.Intersect(NodeDic[subTag])); // оба tag ссылаются на оду и туже ноду

                        foreach (var prev in result.Where(pn => pn.HasChildNodes()))
                        {
                            foreach (var n in NodeDic[subTag])
                            {
                                if (prev.HasChild(n)) stepResult.Add(n); // ноды с subTag это дети
                            }
                        }

                        foreach (var n in NodeDic[subTag].Where(pn => pn.HasChildNodes()))
                        {
                            foreach (var prev in result)
                            {
                                if (n.HasChild(prev)) stepResult.Add(prev); // ноды с subTag это родители
                            }
                        }
                    }
                }
                result = stepResult.Distinct().ToList();
            }

            return result;
        }

        bool HasChild(AddrNode node)
        {
            return node.Id > Id && node.Id < NextChildId;
        }

        bool HasChildNodes()
        {
            return NextChildId > Id + ChildIdMask + 1;
        }

        public static void Load()
        {
            //var db = new FIASContext();
            var q = new Queue<AddrNode>();

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = "select [AOGUID], [PARENTGUID], [FORMALNAME], [OFFNAME], [SHORTNAME], [POSTALCODE], [OKATO], [OKTMO], [SHORTNAME], [AOLEVEL], [KLADRCODE], [UPDATEDATE] from  [dbo].[ADDROBJ]";

                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    q.Enqueue(new AddrNode(reader));
                }
                reader.Close();
                conn.Close();
            }

            const ulong rootIdMask = ulong.MaxValue >> 12;
            ulong rootNextId = rootIdMask + 1;

            while (q.Count > 0)
            {
                var nr = q.Dequeue();
                if (nr.PARENTGUID == Guid.Empty)
                {
                    nr.Id = rootNextId;
                    rootNextId += rootIdMask + 1;

                    nr.ChildIdMask = rootIdMask >> 12;
                    nr.NextChildId = nr.Id + nr.ChildIdMask + 1;

                    Nodes[nr.AOGUID] = nr;
                }
                else if (Nodes.ContainsKey(nr.PARENTGUID))
                {
                    Nodes[nr.PARENTGUID].AddChild(nr);
                    Nodes[nr.AOGUID] = nr;
                }
                else
                {
                    q.Enqueue(nr);
                }
            }

            foreach (var n in Nodes.Values)
            {
                foreach (var t in AddrNode.ParceTags(n.FORMALNAME))
                {
                    if (NodeDic.ContainsKey(t))
                    {
                        NodeDic[t].Add(n);
                    }
                    else
                    {
                        NodeDic[t] = new List<AddrNode> { n };
                    }
                }
            }

            foreach (var k in NodeDic.Keys)
            {
                var k1 = TagHead(k);

                if (NodeKey.ContainsKey(k1))
                {
                    NodeKey[k1].Add(k);
                }
                else
                {
                    NodeKey[k1] = new List<string> { k };
                }
            }

            //db.Dispose();
        }

        void AddChild(AddrNode n)
        {
            n.Parent = this;

            n.ChildIdMask = ChildIdMask >> 12;

            n.Id = NextChildId;

            NextChildId += ChildIdMask + 1;

            n.NextChildId = n.Id + n.ChildIdMask + 1;
        }

        public int Raiting(IEnumerable<string> src)
        {
            var names = new List<string>();
            names.AddRange(ParceTags(FORMALNAME));

            for (AddrNode curItem = Parent; !ReferenceEquals(curItem, null); curItem = curItem.Parent)
            {
                names.AddRange(ParceTags(curItem.FORMALNAME));
            }

            return names.Intersect(src).Count();
        }

        public void SetOKTMO()
        {
            string originalOktmoStr = OKTMO.ToString();
            string oktmoStr = "00000000000" + originalOktmoStr;
            var len = originalOktmoStr.Length;
            len += (len == 10 || len == 7 || len == 4 || len == 1) ? 1 : 0;
            oktmoStr = oktmoStr.Substring(oktmoStr.Length- len);
            OKTMOName = oktmo.GetOKTMO(oktmoStr);
            if (null != Parent)
            {
                Parent.SetOKTMO();
            }
        }
    }
}
