using NUnrar.Archive;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace FIASSplit
{
    class FIASFile
    {
        public static string GetEntityKey(FileInfo f)
        {
            if (f.Name.StartsWith("AS_ADDROBJ_"))
            {
                return "AOGUID";
            }
            else if (f.Name.StartsWith("AS_HOUSE_"))
            {
                return "HOUSEGUID";
            }
            else if (f.Name.StartsWith("AS_HOUSEINT_"))
            {
                return "INTGUID";
            }
            else if (f.Name.StartsWith("AS_LANDMARK_"))
            {
                return "LANDGUID";
            }
            return "";
        }

        public static string GetFilePref(FileInfo f)
        {
            if (f.Name.StartsWith("AS_ADDROBJ_"))
            {
                return "AS_ADDROBJ_";
            }
            else if (f.Name.StartsWith("AS_HOUSE_"))
            {
                return "AS_HOUSE_";
            }
            else if (f.Name.StartsWith("AS_HOUSEINT_"))
            {
                return "AS_HOUSEINT_";
            }
            else if (f.Name.StartsWith("AS_LANDMARK_"))
            {
                return "AS_LANDMARK_";
            }
            return "";
        }

        public static bool IsDataFile(FileInfo f)
        {
            if (f.Name.StartsWith("AS_ADDROBJ_"))
            {
                return true;
            }
            else if (f.Name.StartsWith("AS_HOUSE_"))
            {
                return true;
            }
            /*
            else if (f.Name.StartsWith("AS_HOUSEINT_"))
            {
                return true;
            }
            else if (f.Name.StartsWith("AS_LANDMARK_"))
            {
                return true;
            }
            */
            return false;
        }

        public static DirectoryInfo GetPrevDir(DirectoryInfo dir)
        {
            int curRev = int.Parse(dir.Name);
            int prevRev = curRev - 1;

            while (prevRev > 0)
            {
                var rDir = new DirectoryInfo(Path.Combine(dir.Parent.FullName, prevRev.ToString()));
                if (rDir.Exists)
                {
                    return rDir;
                }
                --prevRev;
            }
            return null;
        }

        public static DirectoryInfo ExtractFiles(DirectoryInfo dir)
        {
            var f = dir.GetFiles("*.rar").First();
            if (!f.Exists)
            {
                ConsoleHelper.WriteLine(string.Format("error rar file {0}", f.FullName));
                return null;
            }

            var outDir = new DirectoryInfo(Path.Combine(dir.FullName, "Src"));

            if (!outDir.Exists)
            {
                outDir.Create();

                ConsoleHelper.WriteLine(string.Format("start extract {0}", f.FullName));
                RarArchive archive = RarArchive.Open(f.FullName);
                foreach (RarArchiveEntry entry in archive.Entries)
                {
                    string path = Path.Combine(outDir.ToString(), Path.GetFileName(entry.FilePath));
                    entry.WriteToFile(path);
                }
            }
            ConsoleHelper.WriteLine("complete");
            return outDir;
        }

        public static DirectoryInfo SplitFiles(DirectoryInfo dir)
        {
            var srcDir = ExtractFiles(dir);

            var outDir = new DirectoryInfo(Path.Combine(dir.FullName, "Split"));
            if (!outDir.Exists)
            { 
                outDir.Create();

                foreach (var f in srcDir.GetFiles("*.xml"))
                {
                    if (FIASFile.IsDataFile(f))
                    {
                        ConsoleHelper.WriteLine(string.Format("split {0}", f.Name));
                        SplitFile(f, outDir);
                        ConsoleHelper.WriteLine("split ok");
                    }
                }
            }
            return outDir;
        }

        public static void SplitFile(FileInfo f, DirectoryInfo outDir)
        {
            var writers = new Dictionary<string, XmlWriter>();

            var keyName = FIASFile.GetEntityKey(f);

            var reader = XmlReader.Create(f.FullName);
            reader.MoveToContent();
            reader.Read();

            CursorHelper ch = null;

            int count = 0;

            // loop through Object elements
            while (reader.NodeType == XmlNodeType.Element)
            {
                var data = reader.ReadOuterXml();
                var part = FIASEntity.GetAttrValue(data, keyName).Substring(0, 2);

                if (writers.ContainsKey(part))
                {
                    writers[part].WriteRaw(data);
                }
                else
                {
                    var outFile = new FileInfo(Path.Combine(outDir.FullName, string.Format("{0}{1}.xml", FIASFile.GetFilePref(f), part)));
                    var writer = XmlWriter.Create(outFile.FullName);

                    writers[part] = writer;

                    writer.WriteStartDocument();
                    writer.WriteStartElement("ROOT");
                    writer.WriteRaw(data);
                }

                if (++count % 10000 == 0)
                {
                    if (ch == null)
                    {
                        ch = new CursorHelper();
                    }
                    ch.WriteLine(string.Format("split {0} rows", count));
                }
            }

            if (ch == null)
            {
                ch = new CursorHelper();
            }
            ch.WriteLine(string.Format("split {0} rows", count));

            foreach (var w in writers.Values)
            {
                w.WriteEndElement();
                w.WriteEndDocument();
                w.Close();
            }

            writers.Clear();
        }
    }
}
