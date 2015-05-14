using fpNode.FIAS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
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
        public IEnumerable<Node> Get(string query, int skip = 0, int take = 10)
        {
            return Node.FindNodes(query).Skip(skip).Take(take);
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
