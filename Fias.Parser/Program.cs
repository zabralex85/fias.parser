using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq.Mapping;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Fias.Parser
{
    class Program
    {
        private static void Main()
        {
            Thread t1 = new Thread(InsertHouses);
            Thread t2 = new Thread(InsertObjects);
            Thread t3 = new Thread(InsertHouseInterval);
            Thread t4 = new Thread(InsertNormativeDocument);
            Thread t5 = new Thread(InsertLandmarks);

            t1.IsBackground = true;
            t2.IsBackground = true;
            t3.IsBackground = true;
            t4.IsBackground = true;
            t5.IsBackground = true;

            t1.Start(@"E:\fias_xml\AS_HOUSE_20150716_0f1dd6d6-35b3-473b-9f15-37d5ce5bf75f.XML");
            t2.Start(@"E:\fias_xml\AS_ADDROBJ_20150716_8a8e1397-bd8f-4c1c-b208-62eb1481cf50.XML");
            t3.Start(@"E:\fias_xml\AS_HOUSEINT_20150716_d8e6227e-a3d9-4901-8ae5-46e72a352694.XML");
            t4.Start(@"E:\fias_xml\AS_NORMDOC_20150716_55782eed-5d66-4e04-b749-be696f08b9c1.XML");
            t5.Start(@"E:\fias_xml\AS_LANDMARK_20150716_34f7d768-5746-4dc2-bf86-63e282e00f5a.XML");


            do
            {
                Thread.Sleep(new TimeSpan(0, 0, 10));
            } 
            while (t1.IsAlive || t2.IsAlive || t3.IsAlive || t4.IsAlive);

            //InsertHouses(@"E:\fias_xml\AS_HOUSE_20150716_0f1dd6d6-35b3-473b-9f15-37d5ce5bf75f.XML");
            //InsertObjects(@"E:\fias_xml\AS_ADDROBJ_20150716_8a8e1397-bd8f-4c1c-b208-62eb1481cf50.XML");
            //InsertHouseInterval(@"E:\fias_xml\AS_HOUSEINT_20150716_d8e6227e-a3d9-4901-8ae5-46e72a352694.XML");
            //InsertNormativeDocument(@"E:\fias_xml\AS_NORMDOC_20150716_55782eed-5d66-4e04-b749-be696f08b9c1.XML");
        }

        private static void InsertLandmarks(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[Landmark]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                List<Landmark> oList = new List<Landmark>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "Landmark")
                    {
                        var t = reader.ReadString();
                        Console.WriteLine(t);


                        var obj = new Landmark();
                        obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        obj.IFNSFL = reader.GetAttribute("IFNSFL");
                        obj.IFNSUL = reader.GetAttribute("IFNSUL");
                        obj.OKATO = reader.GetAttribute("OKATO");
                        obj.OKTMO = reader.GetAttribute("OKTMO");
                        obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        obj.LANDGUID = reader.GetAttribute("LANDGUID");
                        obj.LANDID = reader.GetAttribute("LANDID");
                        obj.LOCATION = reader.GetAttribute("LOCATION");
                        obj.NORMDOC = reader.GetAttribute("NORMDOC");

                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            //context.Houses.InsertAllOnSubmit(houses);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                //context.Houses.InsertAllOnSubmit(houses);
                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }

        private static void InsertObjects(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[Object]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                List<Object> oList = new List<Object>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "Object")
                    {
                        var t = reader.ReadString();
                        Console.WriteLine(t);


                        var obj = new Object();
                        obj.ACTSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("ACTSTATUS")));
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.AOID = reader.GetAttribute("AOID");
                        obj.AOLEVEL = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("AOLEVEL")));
                        obj.AREACODE = reader.GetAttribute("AREACODE");
                        obj.AUTOCODE = reader.GetAttribute("AUTOCODE");
                        obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        obj.EXTRCODE = reader.GetAttribute("EXTRCODE");
                        obj.REGIONCODE = reader.GetAttribute("REGIONCODE");
                        obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        obj.IFNSFL = reader.GetAttribute("IFNSFL");
                        obj.IFNSUL = reader.GetAttribute("IFNSUL");
                        obj.OFFNAME = reader.GetAttribute("OFFNAME");
                        obj.OKATO = reader.GetAttribute("OKATO");
                        obj.OKTMO = reader.GetAttribute("OKTMO");
                        obj.OPERSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("OPERSTATUS")));
                        obj.PARENTGUID = reader.GetAttribute("PARENTGUID");
                        obj.PLACECODE = reader.GetAttribute("PLACECODE");
                        obj.PLAINCODE = reader.GetAttribute("PLAINCODE");
                        obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        obj.PREVID = reader.GetAttribute("PREVID");
                        obj.SEXTCODE = reader.GetAttribute("SEXTCODE");
                        obj.SHORTNAME = reader.GetAttribute("SHORTNAME");
                        obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        obj.STREETCODE = reader.GetAttribute("STREETCODE");
                        obj.FORMALNAME = reader.GetAttribute("FORMALNAME");
                        obj.LIVESTATUS = Convert.ToInt16(reader.GetAttribute("LIVESTATUS"));
                        obj.CENTSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("CENTSTATUS")));
                        obj.CITYCODE = reader.GetAttribute("CITYCODE");
                        obj.CODE = reader.GetAttribute("CODE");
                        obj.CTARCODE = reader.GetAttribute("CTARCODE");
                        obj.CURRSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("CURRSTATUS")));
                        obj.NEXTID = reader.GetAttribute("NEXTID");
                        obj.NORMDOC = reader.GetAttribute("NORMDOC");

                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            //context.Houses.InsertAllOnSubmit(houses);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                //context.Houses.InsertAllOnSubmit(houses);
                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }
        private static void InsertHouseInterval(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[HouseInterval]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                List<HouseInterval> oList = new List<HouseInterval>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "HouseInterval")
                    {
                        var t = reader.ReadString();
                        Console.WriteLine(t);


                        var obj = new HouseInterval();
                        obj.AOGUID = reader.GetAttribute("AOGUID");
                        obj.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        obj.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        obj.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        obj.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        obj.IFNSFL = reader.GetAttribute("IFNSFL");
                        obj.IFNSUL = reader.GetAttribute("IFNSUL");
                        obj.INTEND = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("INTEND")));
                        obj.INTGUID = reader.GetAttribute("INTGUID");
                        obj.INTSTART = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("INTSTART")));
                        obj.INTSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("INTSTATUS")));
                        obj.OKATO = reader.GetAttribute("OKATO");
                        obj.OKTMO = reader.GetAttribute("OKTMO");
                        obj.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        obj.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        obj.HOUSEINTID = reader.GetAttribute("HOUSEINTID");
                        obj.COUNTER = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("COUNTER")));
                        obj.NORMDOC = reader.GetAttribute("NORMDOC");

                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            //context.Houses.InsertAllOnSubmit(houses);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                //context.Houses.InsertAllOnSubmit(houses);
                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }
        private static void InsertNormativeDocument(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[NormativeDocument]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                List<NormativeDocument> oList = new List<NormativeDocument>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "NormativeDocument")
                    {
                        var t = reader.ReadString();
                        Console.WriteLine(t);


                        var obj = new NormativeDocument();
                        
                        if (reader.GetAttribute("DOCDATE") != null)
                        {
                            obj.DOCDATE = Convert.ToDateTime(reader.GetAttribute("DOCDATE"));
                        }

                        
                        if (reader.GetAttribute("DOCIMGID") != null)
                        {
                            obj.DOCIMGID = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("DOCIMGID")));
                        }

                        obj.DOCNAME = reader.GetAttribute("DOCNAME");
                        obj.DOCNUM = reader.GetAttribute("DOCNUM");
                        

                        if (reader.GetAttribute("DOCTYPE") != null)
                        {
                            obj.DOCTYPE = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("DOCTYPE")));
                        }

                        obj.NORMDOCID = reader.GetAttribute("NORMDOCID");


                        oList.Add(obj);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            //context.Houses.InsertAllOnSubmit(houses);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                //context.Houses.InsertAllOnSubmit(houses);
                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }
        private static void InsertHouses(object o)
        {
            using (var context = new MyDataContext())
            {
                context.ExecuteCommand("TRUNCATE TABLE [FIAS].[dbo].[House]");
                context.SubmitChanges();
            }

            var reader = XmlReader.Create(o.ToString());
            reader.Read();
            reader.Read();

            int counter = 0;

            using (var context = new MyDataContext())
            {
                List<House> oList = new List<House>();

                while (reader.Read())
                {
                    // your code here.
                    if (reader.Name == "House")
                    {
                        var t = reader.ReadString();
                        Console.WriteLine(t);


                        var house = new House();
                        house.AOGUID = reader.GetAttribute("AOGUID");
                        house.BUILDNUM = reader.GetAttribute("BUILDNUM");
                        house.COUNTER = Convert.ToInt32(reader.GetAttribute("COUNTER"));
                        house.ENDDATE = Convert.ToDateTime(reader.GetAttribute("ENDDATE"));
                        house.ESTSTATUS = Convert.ToInt32(reader.GetAttribute("ESTSTATUS"));
                        house.IFNSFL = reader.GetAttribute("IFNSFL");
                        house.IFNSUL = reader.GetAttribute("IFNSUL");
                        house.HOUSEGUID = reader.GetAttribute("HOUSEGUID");
                        house.HOUSEID = reader.GetAttribute("HOUSEID");
                        house.HOUSENUM = reader.GetAttribute("HOUSENUM");
                        house.NORMDOC = reader.GetAttribute("NORMDOC");
                        house.TERRIFNSFL = reader.GetAttribute("TERRIFNSFL");
                        house.TERRIFNSUL = reader.GetAttribute("TERRIFNSUL");
                        house.UPDATEDATE = Convert.ToDateTime(reader.GetAttribute("UPDATEDATE"));
                        house.OKATO = reader.GetAttribute("OKATO");
                        house.OKTMO = reader.GetAttribute("OKTMO");
                        house.POSTALCODE = reader.GetAttribute("POSTALCODE");
                        house.STARTDATE = Convert.ToDateTime(reader.GetAttribute("STARTDATE"));
                        house.STATSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("STATSTATUS")));

                        if (reader.GetAttribute("STRSTATUS") != null)
                        {
                            house.STRSTATUS = Convert.ToBoolean(Convert.ToInt32(reader.GetAttribute("STRSTATUS")));
                        }

                        house.STRUCNUM = reader.GetAttribute("STRUCNUM");

                        oList.Add(house);
                        counter++;

                        if (counter % 1000 == 0)
                        {
                            context.BulkInsertAll(oList);
                            //context.Houses.InsertAllOnSubmit(houses);
                            context.SubmitChanges();
                            oList.Clear();
                        }
                    }
                }

                //context.Houses.InsertAllOnSubmit(houses);
                context.BulkInsertAll(oList);
                context.SubmitChanges();
            }

            reader.Close();
            reader.Dispose();
        }
    }

    public partial class MyDataContext : FiasDBDataContext
    {
        void OnCreated()
        {
            CommandTimeout = 5*60;
        }

        public void BulkInsertAll<T>(IEnumerable<T> entities)
        {
            using (var conn = new SqlConnection(Connection.ConnectionString))
            {
                conn.Open();

                Type t = typeof (T);

                var tableAttribute = (TableAttribute) t.GetCustomAttributes(
                    typeof (TableAttribute), false).Single();
                var bulkCopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = tableAttribute.Name
                };

                var properties = t.GetProperties().Where(EventTypeFilter).ToArray();
                var table = new DataTable();

                foreach (var property in properties)
                {
                    Type propertyType = property.PropertyType;
                    if (propertyType.IsGenericType &&
                        propertyType.GetGenericTypeDefinition() == typeof (Nullable<>))
                    {
                        propertyType = Nullable.GetUnderlyingType(propertyType);
                    }

                    table.Columns.Add(new DataColumn(property.Name, propertyType));
                }

                foreach (var entity in entities)
                {
                    table.Rows.Add(
                        properties.Select(
                            property => property.GetValue(entity, null) ?? DBNull.Value
                            ).ToArray());
                }

                bulkCopy.WriteToServer(table);
            }
        }

        private bool EventTypeFilter(System.Reflection.PropertyInfo p)
        {
            var attribute = Attribute.GetCustomAttribute(p,
                typeof (AssociationAttribute)) as AssociationAttribute;

            if (attribute == null) return true;
            if (attribute.IsForeignKey == false) return true;

            return false;
        }
    }
}
