using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Configuration;
using System.Data.SqlClient;
using System.Data.Common;

namespace Import_Excel_SqlDB_MVC.Controllers
{
    public class HomeController : Controller
    {
        /// <summary>
        /// Author: Badr Shahin 
        /// Created: 18 / 9 / 2019
        /// Description: Read Excel File And Upload It Server Then Insert it Into SQLServer Database.
        /// </summary>
        /// <param name="postedFile"></param>
        /// <returns></returns>
        // declare table name 
        string destinationTableName = string.Empty;
        // GET: Home
        public ActionResult Index()
        {
            return View();
        }
        
        [HttpPost]
        public ActionResult Index(HttpPostedFileBase postedFile)
        {
            string filePath = string.Empty;
            
            if (postedFile != null)
            {
                // upload file from localhost to server
                string path = Server.MapPath("~/Uploads/");
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
                string extension = Path.GetExtension(postedFile.FileName);
                filePath = path + DateTime.Now.ToString("MMM-d-yyyy-hh-mm-ss") + "_" + Path.GetFileName(postedFile.FileName);
                postedFile.SaveAs(filePath);

                // check file extension
                string conString = string.Empty;
                switch (extension)
                {
                    case ".xls": //Excel 97-03.
                        conString = ConfigurationManager.ConnectionStrings["Excel03ConString"].ConnectionString;
                        break;
                    case ".xlsx": //Excel 07 and above.
                        conString = ConfigurationManager.ConnectionStrings["Excel07ConString"].ConnectionString;
                        break;
                }

                // datatable to get sheet structure 
                DataTable dtExcelSchema = new DataTable();

                // datatable to get all sheets data 
                List<DataTable> dtExcelDataList = new List<DataTable>();

                conString = string.Format(conString, filePath);

                // connect to excel file
                using (OleDbConnection connExcel = new OleDbConnection(conString))
                {
                    using (OleDbCommand cmdExcel = new OleDbCommand())
                    {
                        using (OleDbDataAdapter odaExcel = new OleDbDataAdapter())
                        {
                            cmdExcel.Connection = connExcel;

                            //Get the names of Excel Sheets.
                            List<string> sheetNamesList = new List<string>();

                            try
                            {
                                if (connExcel.State != ConnectionState.Open)
                                {
                                    connExcel.Open();
                                }
                                else
                                {
                                    System.Windows.Forms.MessageBox.Show("This file is already opened by another program, please colse it before then try again!");
                                }
                                dtExcelSchema = connExcel.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);

                                foreach (DataRow sheetRowSchema in dtExcelSchema.Rows)
                                {
                                    if (!sheetRowSchema["TABLE_NAME"].ToString().Contains("_xlnm#_FilterDatabase"))
                                    {
                                        sheetNamesList.Add(sheetRowSchema["TABLE_NAME"].ToString().Substring(0, sheetRowSchema["TABLE_NAME"].ToString().Length - 1));
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                System.Windows.Forms.MessageBox.Show(ex.Message);
                            }
                            finally
                            {
                                connExcel.Close();
                            }

                            //Read Data from Excel Sheets.
                            try
                            {
                                if (connExcel.State != ConnectionState.Open)
                                {
                                    connExcel.Open();
                                }
                                else
                                {
                                    System.Windows.Forms.MessageBox.Show("This file is already opened by another program, please colse it before then try again!");
                                }
                                foreach (string sheetName in sheetNamesList)
                                {
                                    // datatable to get sheet data 
                                    DataTable dtExcelSheet = new DataTable();
                                    cmdExcel.CommandText = "SELECT * FROM [" + sheetName + "$]";
                                    odaExcel.SelectCommand = cmdExcel;
                                    odaExcel.Fill(dtExcelSheet);

                                    // add time stamp column to datatable sheet to insert into database ImportedDateTime column
                                    DataColumn sheetDateTimeStamp = new DataColumn("ImportedDateTime", typeof(System.DateTime));
                                    sheetDateTimeStamp.DefaultValue = DateTime.Now;
                                    dtExcelSheet.Columns.Add(sheetDateTimeStamp);
                                    
                                    dtExcelDataList.Add(dtExcelSheet);
                                    dtExcelSheet.Dispose();
                                }
                            }
                            catch (Exception ex)
                            {
                                System.Windows.Forms.MessageBox.Show(ex.Message);
                            }
                            finally
                            {
                                connExcel.Close();
                            }
                        }
                    }
                }
                
                // connect to database
                conString = ConfigurationManager.ConnectionStrings["Constring"].ConnectionString;
                using (SqlConnection con = new SqlConnection(conString))
                {
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {
                        // counter to loop through dtExcelDataList 
                        int sheetNo = 0;
                        foreach (DataRow sheetRowSchema in dtExcelSchema.Rows)
                        {
                            if (!sheetRowSchema["TABLE_NAME"].ToString().Contains("_xlnm#_FilterDatabase"))
                            {
                                try
                                {
                                    con.Open();
                                    sqlBulkCopy.DestinationTableName = sheetRowSchema["TABLE_NAME"].ToString().Substring(0, sheetRowSchema["TABLE_NAME"].ToString().Length - 1);
                                    DbCommand command = con.CreateCommand();

                                    // (1) we're not interested in any data
                                    command.CommandText = "SELECT * FROM " + sqlBulkCopy.DestinationTableName + " WHERE 1 = 0";
                                    command.CommandType = CommandType.Text;
                                    DbDataReader reader = command.ExecuteReader();

                                    // (2) get the schema of the result set
                                    DataTable schemaTable = reader.GetSchemaTable();

                                    foreach (DataRow row in schemaTable.Rows)
                                    {
                                        foreach (DataColumn column in dtExcelDataList[sheetNo].Columns)
                                        {
                                            //[OPTIONAL]: Map the Excel columns with that of the database table
                                            if (row.Field<string>("ColumnName") == column.ColumnName)
                                            {
                                                sqlBulkCopy.ColumnMappings.Add(row.Field<string>("ColumnName"), column.ColumnName);
                                                break;
                                            }
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    System.Windows.Forms.MessageBox.Show(ex.Message);
                                }
                                finally
                                {
                                    con.Close();
                                }

                                try
                                {
                                    con.Open();

                                    // empty table before read new data
                                    SqlCommand sqlCommand = new SqlCommand("DELETE FROM " + sqlBulkCopy.DestinationTableName, con);
                                    sqlCommand.ExecuteNonQuery();

                                    sqlBulkCopy.WriteToServer(dtExcelDataList[sheetNo]);
                                    System.Windows.Forms.MessageBox.Show("Data inserted successfully in " + sqlBulkCopy.DestinationTableName + " table.");
                                }
                                catch (Exception ex)
                                {
                                    System.Windows.Forms.MessageBox.Show("Failed to insert Data in " + sqlBulkCopy.DestinationTableName + " table.");
                                    Console.WriteLine(ex.Message);
                                }
                                finally
                                {
                                    //Remove unnecessary mapping from sqlBulk
                                    sqlBulkCopy.ColumnMappings.Clear();
                                    con.Close();
                                }
                                sheetNo++;
                            }
                        }
                    }
                }
            }
            return View();
        }
    }
}