using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Web.Http;

namespace FIASWeb.Controllers
{
    public class FIASController : ApiController
    {
        // GET: api/FIAS
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/FIAS/5
        public IEnumerable<AddrNode> Get(string query, int skip = 0, int take = 10)
        {
            var lst = AddrNode.ParceTags(query);
            var q = AddrNode.FindNodes(query).OrderByDescending(n => n.Raiting(lst)).Skip(skip).Take(take).ToList();
            q.ForEach(e => e.SetOKTMO());
            return q;
        }

        // POST: api/FIAS
        public void Post([FromBody]string value)
        {
        }

        // PUT: api/FIAS/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/FIAS/5
        public void Delete(int id)
        {
        }
    }

    public class FIASHOUSEController : ApiController
    {
        // GET: api/FIAS
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/FIAS/5
        public IEnumerable<HouseNode> Get(Guid AddrId)
        {
            var result = new List<HouseNode>();

            if (AddrNode.Nodes.ContainsKey(AddrId))
            {
                var addr = AddrNode.Nodes[AddrId];
               
                using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
                {
                    conn.Open();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = string.Format("select * from [dbo].[HOUSE] WHERE [AOGUID] = '{0}'", addr.AOGUID);

                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        var n = new HouseNode(reader);
                        n.Parent = addr;
                        result.Add(n);
                    }
                    reader.Close();
                    conn.Close();
                }
            }
            var re = new Regex(@"(\d+).*");
            return result.OrderBy(n => {
                var d = string.IsNullOrEmpty(n.HOUSENUM) ? n.STRUCNUM : n.HOUSENUM;
                var m = re.Match(d); return m.Success ? int.Parse(m.Groups[1].Value) : int.MaxValue;
            });
            //return result;
        }

        // POST: api/FIAS
        public void Post([FromBody]string value)
        {
        }

        // PUT: api/FIAS/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/FIAS/5
        public void Delete(int id)
        {
        }
    }

    public class FIASROOMController : ApiController
    {
        // GET: api/FIAS
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/FIAS/5
        public IEnumerable<RoomNode> Get(Guid HouseId)
        {
            var result = new List<RoomNode>();

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select * from [dbo].[HOUSE] WHERE [HOUSEGUID] = '{0}'", HouseId);

                HouseNode house = null;
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    house = new HouseNode(reader);
                    house.Parent = AddrNode.Nodes[house.AOGUID];
                }
                reader.Close();

                cmd = conn.CreateCommand();
                cmd.CommandText = string.Format("select * from [dbo].[ROOM] WHERE [HOUSEGUID] = '{0}'", HouseId);

                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var n = new RoomNode(reader);
                    n.Parent = house;
                    result.Add(n);
                }
                reader.Close();

                conn.Close();
            }
            
            var re = new Regex(@"(\d+).*");
            return result.OrderBy(n => {
                var d = string.IsNullOrEmpty(n.FLATNUMBER) ? n.ROOMNUMBER : n.FLATNUMBER;
                var m = re.Match(d); return m.Success ? int.Parse(m.Groups[1].Value) : int.MaxValue;
            });
        }

        // POST: api/FIAS
        public void Post([FromBody]string value)
        {
        }

        // PUT: api/FIAS/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/FIAS/5
        public void Delete(int id)
        {
        }
    }
}
