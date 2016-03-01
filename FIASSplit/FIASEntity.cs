using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FIASSplit
{
    class FIASEntity
    {
        public static string GetAttrValue(string entity, string attrName)
        {
            var re = new Regex(".*?" + attrName + "=\"(.*?)\".*");
            return re.Match(entity).Groups[1].Value;
        }
    }
}
