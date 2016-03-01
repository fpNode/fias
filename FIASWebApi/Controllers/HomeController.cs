using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FIASWebApi.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            //return Redirect("http://fpnode.com/Page/farpoint-api");
            return View();
        }
    }
}