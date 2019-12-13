using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace R16
{
    class Program
    {
        static void Main(string[] args)
        {

            FormR16("2014", 12);


        }

        public static void FormR16(string year, int month)
        {
            //string CSV_PATH = @ConfigurationManager.AppSettings.Get("CSV_PATH");
            DateTime date = new DateTime();
            Decimal val = new Decimal();
            DateTime bookPeriod = new DateTime();
            bookPeriod = Convert.ToDateTime("01." + month + "." + year);
            string reestrName = "R16";
            string fileName = @ConfigurationManager.AppSettings.Get("CSV_PATH");
            fileName += reestrName + "_" + bookPeriod.ToString("MMyyyy") + ".csv";



            //Задаем формат чисел
            string specifier = "F2";
            CultureInfo culture = CultureInfo.CreateSpecificCulture("eu-ES");
            NumberFormatInfo nfi = culture.NumberFormat;
            nfi.NumberDecimalSeparator = ".";

            SqlConnectionStringBuilder csbuilder = new SqlConnectionStringBuilder("");

            csbuilder["Server"] = @ConfigurationManager.AppSettings.Get("MSSQL_Server");
            csbuilder["UID"] = @ConfigurationManager.AppSettings.Get("UID");
            csbuilder["Password"] = @ConfigurationManager.AppSettings.Get("Password");
            csbuilder["Connect Timeout"] = 20000;
            csbuilder["integrated Security"] = true; //для коннекта с локальным экземпляром

            string reestrQuery = $@"
                    SET DATEFORMAT DMY
                    SELECT [ID]
                    ,[VAL]
                    ,[CREATED]
                    ,[TARIF]
                    ,[VOLUME]
                    ,(select distinct nomer from [ORACLE].[dbo].[FLS] where ID = DN.FLS)[FLS]
                    ,[SERV]
                    ,[SERV_PERIOD]
                    ,[BOOK_PERIOD]
                    ,[REMARK]
                    FROM [ORACLE].[dbo].[DOC_NACH] as DN
                    where BOOK_PERIOD between '{bookPeriod.ToString("dd.MM.yyyy")}' and '{LastDayOfMonth(bookPeriod).ToString("dd.MM.yyyy")}'";


            string nachInQuery = $@"select 
                    (select distinct nomer from [ORACLE].[dbo].[FLS] where ID = DN.FLS)[FLS_LONG],
                    SUM (VAL) as NACH_SUM
                    from [ORACLE].[dbo].[DOC_NACH] as DN
                    where created between '01.10.2014' and '{LastDayOfMonth(bookPeriod.AddMonths(-1)).ToString("dd.MM.yyyy")}' 
                    group by [FLS]";

            Console.WriteLine(nachInQuery);
            /*
            string payQuery = $@"SELECT [ID] 
                  ,[DATE_INP]
                  ,[VAL]                    
                  ,[STATUS]
                  ,[FLS]
                  ,[PERIOD]
                  ,[PAY_DATE]
              FROM [ORACLE].[dbo].[DOC_PAY]
              where fls = {fls}
             order by [DATE_INP]";
            */

            DataTable reestr = new DataTable();
            DataColumn column;
            DataRow reestrRow;


            #region Задаем структуру таблицы reestr
            //1. AccountOperator (ИНН оператора ЛС)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "AccoountOperator";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //2. AccountNum (Номер ЛС (ФЛС))
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "AccountNum";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);

            //3. ServiceCode (Услуга)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "ServiceCode";
            column.DefaultValue = "22";
            reestr.Columns.Add(column);

            //4. ProviderCode (ИНН поставщика услуг)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "ProviderCode";
            column.DefaultValue = "5190996259";
            reestr.Columns.Add(column);

            //5. ChargeYear (Год отчетного периода)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.AllowDBNull = false;
            column.ColumnName = "ChargeYear";
            reestr.Columns.Add(column);

            //6. ChargeMonth (Отчетный месяц)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.AllowDBNull = false;
            column.ColumnName = "ChargeMonth";
            reestr.Columns.Add(column);

            //7. SaldoIn (Остаток задолженности по взносам на начало отчетного периода)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.AllowDBNull = false;
            column.DefaultValue = 0;
            column.ColumnName = "SaldoIn";
            reestr.Columns.Add(column);

            //8. ChargeVolume (Площадь помещения)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "ChargeVolume";
            reestr.Columns.Add(column);

            //9. Tarif (Тариф по взносам в фонд капитального ремонта)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "Tarif";
            reestr.Columns.Add(column);

            //10. ChargeSum (Сумма начисления в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "ChargeSum";
            reestr.Columns.Add(column);

            //11. RecalSum (Сумма перерасчета в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "RecalSum";
            reestr.Columns.Add(column);

            //12. PaySum (Оплата по взносам в фонд капитального ремонта)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PaySum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //13. SaldoOut (Остаток задолженности (только по начислениям) на конец отчетного месяца, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "SaldoOut";
            reestr.Columns.Add(column);

            //14. SaldoFineIn (Остаток задолженности по пенни на начало месяца, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "SaldoFineIn";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //15. FineSum (Сумма пенни начисленная в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "FineSum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //16. PayFineSum (Оплата по пенни в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PayFineSum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //17. CorrectFineSum (Корректировка пенни на конец месяца, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "CorrectFineSum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //18. SaldoFineOut (Остаток задолженности по пенни (только начисления) на конец месяца, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "SaldoFineOut";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //19. Дата последней оплаты
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.DateTime");
            column.ColumnName = "LastPayDay";
            reestr.Columns.Add(column);

            //20. PayAgent (Код платежного агента)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "PayAgent";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //21. PrivChargeSum (Сумма начисления льготы в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PrivChargeSum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //22. PrivRecalSum (Сумма перерасчета льготы в отчетном месяце, руб.)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PrivRecalSum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //23. PrivCategory (Код категории льготника)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "PrivCategory";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //24. PrivPaySum (оплата/возмещение бюджетом льгот)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PrivPaySum";
            column.AllowDBNull = true;
            column.DefaultValue = null;
            reestr.Columns.Add(column);

            //25. NachDate (дата начисления, служебное поле для упрощения расчета сальдо)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.DateTime");
            column.ColumnName = "NachDate";
            column.AllowDBNull = false;
            reestr.Columns.Add(column);
            #endregion

            DataTable pay = new DataTable();
            column = null;
            DataRow payRow;

            #region  Задаем структуру таблицы pay          
            //1. AccountNum (Номер ЛС (ФЛС))
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.String");
            column.ColumnName = "AccountNum";
            column.AllowDBNull = false;
            pay.Columns.Add(column);

            //2. PayInpDate (дата ввода платежа в систему)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.DateTime");
            column.ColumnName = "PayInpDate";
            column.AllowDBNull = false;
            pay.Columns.Add(column);

            //3. PaySum (сумма платежа)
            column = new DataColumn();
            column.DataType = System.Type.GetType("System.Decimal");
            column.ColumnName = "PaySum";
            column.AllowDBNull = false;
            pay.Columns.Add(column);

            /*  //4. PayStatus (Статус платежа, необходимо уточненить назначение данного поля)
              column = new DataColumn();
              column.DataType = System.Type.GetType("System.Int32");
              column.ColumnName = "PayStatus";
              column.AllowDBNull = true;
              column.DefaultValue = null;            
              pay.Columns.Add(column);

              //5. Period (За какой период платеж)
              column = new DataColumn();
              column.DataType = System.Type.GetType("System.DateTime");
              column.ColumnName = "PayPeriod";
              column.AllowDBNull = false;
              pay.Columns.Add(column);

              //5. PayDate (Дата совершения платежа)
              column = new DataColumn();
              column.DataType = System.Type.GetType("System.DateTime");
              column.ColumnName = "PayDate";
              column.AllowDBNull = false;
              pay.Columns.Add(column);*/

            #endregion

            //Подключаемся к БД и получаем данные для таблицы reestr
            using(DataTable qr = new DataTable())
            {

                using(SqlConnection conn = new SqlConnection(csbuilder.ConnectionString))
                {
                    Console.WriteLine("Подключаемся в БД");
                    conn.Open();
                    Console.WriteLine("Получаем данные о начислениях");
                    SqlCommand cmd = new SqlCommand(reestrQuery, conn);
                    cmd.CommandTimeout = 600;
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(qr);
                    conn.Close();
                }

                //Наполняем таблицу reestr данными из запроса
                foreach(DataRow row in qr.Rows)
                {
                    reestrRow = reestr.NewRow();

                    //Номер ЛС
                    reestrRow["AccountNum"] = Convert.ToString(row["FLS"]);

                    //Год и месяц начислений
                    date = Convert.ToDateTime(row["CREATED"]);
                    reestrRow["ChargeYear"] = date.ToString("yyyy");
                    reestrRow["ChargeMonth"] = date.ToString("MM");
                    reestrRow["NachDate"] = date;


                    //Текущие начисления
                    val = Convert.ToDecimal(row["VAL"]);
                    reestrRow["ChargeSum"] = val;// val.ToString(specifier, nfi);

                    //Тариф
                    val = Convert.ToDecimal(row["TARIF"]);
                    reestrRow["Tarif"] = val.ToString(specifier, nfi);


                    reestr.Rows.Add(reestrRow);
                }
            }

            //Подключаемся к БД и получаем данные по суммам начислений на начало периода
            using(DataTable nachIn = new DataTable())
            {

                using(SqlConnection conn = new SqlConnection(csbuilder.ConnectionString))
                {
                    conn.Open();
                    Console.WriteLine("Получаем данные о сумме начислений за предыдущие периоды");
                    SqlCommand cmd = new SqlCommand(nachInQuery, conn);
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(nachIn);
                    conn.Close();
                }

                //Считаем сальдо
                foreach(DataRow row in reestr.Rows)
                {
                    var nachSum = nachIn.AsEnumerable()
                                .Where(totals => totals.Field<string>("FLS_LONG") == Convert.ToString (row["FLS"]))
                                .Select(totals => totals.Field<decimal>("NACH_SUM"));
                    row["SaldoIn"] = nachSum;
                    
                    
                    //var nachSum = reestr.AsEnumerable()
                    //         .Where(totals => totals.Field<DateTime>("NachDate") < FirstDayOfMonth(Convert.ToDateTime(row["NachDate"])))
                    //         .Sum(totals => totals.Field<decimal>("ChargeSum"));
                    //var paySum = pay.AsEnumerable()
                    //    .Where(x => x.Field<DateTime>("PayInpDate") < FirstDayOfMonth(Convert.ToDateTime(row["NachDate"])))
                    //    .Sum(x => x.Field<decimal>("PaySum"));
                    //row["SaldoIn"] = nachSum - paySum;
                }


                //Наполняем таблицу reestr данными из запроса
                //foreach(DataRow row in qr.Rows)
                //{
                //    payRow = pay.NewRow();

                //    //Номер ЛС
                //    payRow["AccountNum"] = Convert.ToString(row["FLS"]);

                //    //Дата ввода платежа в систему
                //    payRow["PayInpDate"] = Convert.ToDateTime(row["DATE_INP"]);

                //    //Сумма платежа
                //    payRow["PaySum"] = Convert.ToDecimal(row["VAL"]);

                //    pay.Rows.Add(payRow);
                //}
            }

            //IEnumerable<DataRow> q =
            //    from dr in reestr.AsEnumerable()
            //    group dr by dr.Field("ChargeYear");


            //foreach(DataRow p in q)
            //{
            //    Console.WriteLine(p.Field<string>("ChargeYear"));
            //}

            //var q = reestr.AsEnumerable().
            //Select(dr => new
            //{
            //    chargeYear = dr.Field<string>("ChargeYear"),
            //    ChargeSum = dr.Field<decimal>("ChargeSum")

            //});

            //Считаем сальдо++++++++++++
            //foreach(DataRow row in reestr.Rows)
            //{
            //    var nachSum = reestr.AsEnumerable()
            //             .Where(totals => totals.Field<DateTime>("NachDate") < FirstDayOfMonth(Convert.ToDateTime(row["NachDate"])))
            //             .Sum(totals => totals.Field<decimal>("ChargeSum"));
            //    var paySum = pay.AsEnumerable()
            //        .Where(x => x.Field<DateTime>("PayInpDate") < FirstDayOfMonth(Convert.ToDateTime(row["NachDate"])))
            //        .Sum(x => x.Field<decimal>("PaySum"));
            //    row["SaldoIn"] = nachSum - paySum;
            //}

            //+++++
            //var q = from dr in reestr.AsEnumerable()

            //        group dr by new { year = dr.Field<string>("ChargeYear"), month = dr.Field<string>("ChargeMonth") } into g

            //        select new
            //        {
            //            ChargeYear = g.Key.year,
            //            ChargeMonth = g.Key.month,
            //            SaldoIn = g.Sum(years => (years.Field<decimal>("ChargeSum")))
            //            //YearSaldo = g.Sum(years => (years.Field<string>("ChargeSum")))
            //        };


            //+++++
            //foreach(var t in q)
            //{
            //    Console.WriteLine("Year: {0}, Month: {1}, Saldo: {2}",
            //        t.ChargeYear, t.ChargeMonth, t.SaldoIn);
            //}

            ToCSV(reestr, fileName);

            ////Проверка полученных значений
            //foreach(DataRow row in reestr.Rows)
            //{
            //    Console.WriteLine(Convert.ToString(row["AccountNum"])
            //        + " " + Convert.ToString(row["ChargeYear"])
            //        + " " + Convert.ToString(row["ChargeMonth"])
            //        + " " + Convert.ToString(row["ChargeVolume"])
            //        + " " + Convert.ToString(row["Tarif"])
            //        + " " + Convert.ToString(row["ChargeSum"])
            //        + " " + Convert.ToString(row["SaldoIn"])
            //        );
            //}


        }

        public static void ToCSV(DataTable dtDataTable, string strFilePath)
        {
            StreamWriter sw = new StreamWriter(strFilePath, false);

            //Заголовок реестра
            string header = "#RTYPE=R16\n" +
                "#AccountOperator;AccountNum;ServiceCode;ProviderCode;ChargeYear;ChargeMonth;SaldoIn;ChargeVolume;Tarif;ChargeSum;RecalSum;PaySum;SaldoOut;SaldoFineIn;FineSum;PayFineSum;CorrectFineSum;SaldoFineOut;LastPayDate;PayAgent;PrivChargeSum;PrivRecalSum;PrivCategory;PrivPaySum";
            sw.Write(header);

            //Паттерн для поиска разделителя в полях таблицы
            string pattern = ";+";

            ////Выводим имена столбцов
            //for(int i = 0; i < dtDataTable.Columns.Count; i++)
            //{
            //    sw.Write(dtDataTable.Columns[i]);
            //    if(i < dtDataTable.Columns.Count - 1)
            //    {
            //        sw.Write(";");
            //    }
            //}

            //Выводим данные
            sw.Write(sw.NewLine);
            foreach(DataRow dr in dtDataTable.Rows)
            {
                for(int i = 0; i < dtDataTable.Columns.Count; i++)
                {
                    if(!Convert.IsDBNull(dr[i]))
                    {
                        string value = dr[i].ToString();
                        //if (value.Contains(';'))
                        //{
                        //value = String.Format("\\{0}\\", value);
                        value = Regex.Replace(value, @"\n+", " ");
                        value = Regex.Replace(value, pattern, ":");
                        sw.Write(value);
                        //}
                        //else
                        //{
                        //    sw.Write(dr[i].ToString());
                        //}
                    }
                    else if(Convert.IsDBNull(dr[i]))
                    {
                        sw.Write("NULL");
                    }
                    if(i < dtDataTable.Columns.Count - 1)
                    {
                        sw.Write(";");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.Close();
        }

        public static DateTime FirstDayOfMonth(DateTime date)
        {
            return new DateTime(date.Year, date.Month, 1);
        }

        public static DateTime LastDayOfMonth(DateTime date)
        {
            DateTime d = new DateTime();
            d = FirstDayOfMonth(date);
            d = d.AddMonths(1);
            d = d.AddDays(-1);
            return d;
        }

    }

}
