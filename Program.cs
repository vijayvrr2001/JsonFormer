using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

//cUSTOM
using MongoDB.Bson;
using MongoDB.Driver;

namespace JsonFormer
{
 

    class Program
    {
        static void Main(string[] args)
        {
            SqlDataAdapter dadapter = null;
            var client = new MongoClient("mongodb://admin:swaas@hdcluster-shard-00-00-03zxg.mongodb.net:27017,hdcluster-shard-00-01-03zxg.mongodb.net:27017,hdcluster-shard-00-02-03zxg.mongodb.net:27017?ssl=true&replicaSet=HDCluster-shard-0&authSource=admin");
            var database = client.GetDatabase("vijaydb");
            var collection = database.GetCollection<DoctorData>("vijaytable");

     

            try
            {

                //InsertDoc();


                
                string ConnString = "Data Source = SwaasAD.cloudapp.net,57500;Initial Catalog=AURA_Cachet;User ID=globaluser;Password=April25@!^";
                string SqlString = "SELECT TOP 100000 dv.[Company_Code] ,dv.[DCR_Code],dv.[User_Code],[DCR_Entered_Date] ,dv.[DCR_Actual_Date] ,[DCR_Status] ,[Place_Worked] ,[Category] ,[From_Place] ,[To_Place] ," +
                                    "[Travel_Mode] ,[Travelled_Kms] ,[Flag] ,dv.[Source_Of_Entry] ,[CP_Deviation] ,[TP_Deviation] ,[Doctor_Code] ,[Visit_Mode] ,[Remarks_By_User] ,[DCR_Visit_Status] ,[Doctor_Name] ," +
                                    "[Speciality_Name] " +
                                    "FROM[AURA_Cachet].[dbo].[tbl_SFA_DCR_Master] dcr " +
                                    "JOIN " +
                                    "[AURA_Cachet].[dbo].[tbl_SFA_DCR_Doctor_Visit] dv " +
                                    " ON dcr.DCR_Code = dv.DCR_Code " +
                                    " AND DCR_Status = 1 " +
                                   // " GROUP BY dv.[Company_Code] ,dv.[DCR_Code],dv.[User_Code],[DCR_Entered_Date], dv.[DCR_Actual_Date],[DCR_Status] ,[Place_Worked] ,[Category] ,[From_Place] ,[To_Place] ," +
                                    //" [Travel_Mode] ,[Travelled_Kms] ,[Flag] ,dv.[Source_Of_Entry] ,[CP_Deviation] ,[TP_Deviation] ,[Doctor_Code] ,[Visit_Mode] ,[Remarks_By_User] ,[DCR_Visit_Status] ,[Doctor_Name] ," +
                                    //" [Speciality_Name] " +
                                    " ORDER By DCR_Actual_Date Desc ";

                using (SqlConnection conn = new SqlConnection(ConnString))
                {
                    using (SqlCommand cmd = new SqlCommand(SqlString, conn))
                    {
                        conn.Open();
                        cmd.CommandType = CommandType.Text;
                        dadapter = new SqlDataAdapter();
                        dadapter.SelectCommand = cmd;
                        DataSet dset = new DataSet();
                        dadapter.Fill(dset);

                        var groupbyfilter = dset.Tables[0].AsEnumerable().GroupBy(row => new { actdate = row.Field<DateTime>("DCR_Actual_Date"), uc = row.Field<string>("User_Code") })
                                            .Select(g => g.CopyToDataTable()).ToList();


                        var item = groupbyfilter.ToArray().ToArray();

                        for (int i = 0; i < (item.Count()); i++)
                        {
                            DataRowCollection dr = item[i].Rows;

                            string strDoctorName = dr[0]["DCR_Actual_Date".ToString()] + " -- " + dr[0]["User_Code"].ToString() + " -- " + dr[0]["Doctor_Name"].ToString();
                            Console.WriteLine(strDoctorName);

                            DoctorData docData = new DoctorData();

                            docData.Company_Code = dr[0]["Company_Code"].ToString();
                            docData.User_Code = dr[0]["User_Code"].ToString();
                            docData.DCR_Code = dr[0]["DCR_Code"].ToString();
                            docData.DateOfDCR = dr[0]["DCR_Actual_Date"].ToString();
                            docData.DCREnteredDate = dr[0]["DCR_Entered_Date"].ToString();
                            docData.TravelMode = dr[0]["Travel_Mode"].ToString();
                            docData.DoctorInfo = new List<DoctorPersonInfo>();

                            foreach (DataRow drInner in dr)
                            {

                                docData.DoctorInfo.Add(new DoctorPersonInfo
                                {
                                    DoctorName = drInner["Doctor_Name"].ToString(),
                                    Speciality_Name = drInner["Speciality_Name"].ToString()
                                });

                            }

                            collection.InsertOne(docData);
                        }


                       // foreach (DataRow item in results)
                        {
                            //collection.InsertOneAsync(new BsonDocument("Name", item["Doctor_Name"].ToString()));

                        //,hdcluster-shard-00-01-03zxg.mongodb.net:27017,hdcluster-shard-00-02-03zxg.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=HDCluster-shard-0&authSource=admin

                        }
                    }
                }
                
            }
            catch (Exception ex)
            {

            }
            finally
            {
                // dadapter.Dispose();
            }
        }

        static void InsertDoc()
        {
           
        }

        public class DoctorData
        {
            public string Company_Code;
            public string DCR_Code;
            public string User_Code;
           
            public string DateOfDCR;
            public string DCREnteredDate;
            public string TravelMode;
            public List<DoctorPersonInfo> DoctorInfo;
        }

        public class DoctorPersonInfo
        {
            public string DoctorCode;
            public string DoctorName;
            public string Speciality_Name;

        }

    }
}


/*
 *   string SqlInnerString = "SELECT [Company_Code] ,[DCR_Visit_Code] ,[DCR_Code] ,[Doctor_Code] ,[Visit_Mode] ,[User_Code] ,[Doctor_Name] ,[Speciality_Name] ,[PO_Amount] ,[Source_Of_Entry] FROM[AURA_Cachet].[dbo].[tbl_SFA_DCR_Doctor_Visit] where DCR_Code = '@DCRCode'";
                            //Execute the inner command
                            SqlInnerString = SqlInnerString.Replace("@DCRCode", item[1].ToString());
                            using (SqlCommand cmd1 = new SqlCommand(SqlInnerString, conn))
                            {
                                cmd1.CommandType = CommandType.Text;
                                dadapter.SelectCommand = cmd1;
                                DataSet dset1 = new DataSet();
                                dadapter.Fill(dset1);
                                foreach (DataRow item1 in dset1.Tables[0].Rows)
                                {
*/