using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Database
{
    //main database connection class.
    public class DB : IDisposable
    {
        public static String asString(double val)
        {
            return val.ToString(CultureInfo.InvariantCulture);
        }

        private MySqlConnection connection;
        private String server;
        private String database;
        private String uid;
        private String password;

        private Dictionary<String, String> insertStatements = new Dictionary<String, String>();
        private Dictionary<String, String> updateStatements = new Dictionary<String, String>();
        private Dictionary<String, String> deleteStatements = new Dictionary<String, String>();
        private Dictionary<String, String> selectStatements = new Dictionary<String, String>();

        //Constructor
        public DB(String server, String database, String uid, String password)
        {
            this.server = server;
            this.database = database;
            this.uid = uid;
            this.password = password;
            Initialize();
        }

        //Initialize values
        protected void Initialize()
        {
            String connectionString;
            connectionString = "SERVER=" + this.server + ";" + "DATABASE=" +
            this.database + ";" + "UID=" + this.uid + ";" + "PASSWORD=" + password + ";SslMode=none";

            connection = new MySqlConnection(connectionString);
        }

        public void Dispose(bool disposing)
        {
            if (connection != null)
            {
                connection.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        //open connection to database
        private void OpenConnection()
        {
            connection.Open();
        }

        //Close connection
        private void CloseConnection()
        {
            connection.Close();
        }


        public String GetStatement(Dictionary<String, String> statements, String statementKey)
        {
            try
            {
                return statements[statementKey];
            }
            catch (KeyNotFoundException)
            {
                return null;
            }
        }

        public void AddInsertStatement(String statementKey, String table, String paramString)
        {
            String query = "INSERT INTO " + table + " SET " + paramString;
            insertStatements.Add(statementKey, query);
        }

        public void AddInsertStatement(String table, String paramString)
        {
            AddInsertStatement(table, table, paramString);
        }

        private MySqlCommand ExecuteWriteStatement(Dictionary<String, String> statements, String statementKey, params string[] values)
        {
            String statement = GetStatement(statements, statementKey);
            if (statement == null) throw new Exception(statementKey + " does not produce a statement");

            values = Utilities.Format.AddSlashes(values);
            statement = String.Format(statement, values);

            MySqlCommand cmd;
            try
            {
                //open connection
                OpenConnection();

                //create command and assign the query and connection from the constructor
                cmd = new MySqlCommand(statement, connection);

                //Execute command
                cmd.ExecuteNonQuery();
            }
            finally
            {
                CloseConnection();
            }

            return cmd;
        }

        //Insert statement
        public long Insert(String statementKey, params string[] values)
        {
            MySqlCommand cmd = ExecuteWriteStatement(insertStatements, statementKey, values);
            return cmd.LastInsertedId;
        }

        public void AddUpdateStatement(String statementKey, String table, String paramString, String filterString)
        {
            String query = "UPDATE " + table + " SET " + paramString + " WHERE " + filterString;
            updateStatements.Add(statementKey, query);
        }

        public void AddUpdateStatement(String table, String paramString, String filterString)
        {
            AddUpdateStatement(table, table, paramString, filterString);
        }

        //Update statement
        public void Update(String statementKey, params string[] values)
        {
            ExecuteWriteStatement(updateStatements, statementKey, values);
        }

        //Delete statement
        public void AddDeleteStatement(String statementKey, String table, String filterString, String orderString, String limitString)
        {
            String query = "DELETE FROM " + table;
            if (filterString != null) query += " WHERE " + filterString;
            if (orderString != null) query += " ORDER BY " + orderString;
            if (limitString != null) query += " LIMIT " + limitString;
            deleteStatements.Add(statementKey, query);
        }

        public void AddDeleteStatement(String table, String filterString, String orderString, String limitString)
        {
            AddDeleteStatement(table, table, filterString, orderString, limitString);
        }

        //Delete action
        public void Delete(String statementKey, params string[] values)
        {
            ExecuteWriteStatement(deleteStatements, statementKey, values);
        }

        public void AddSelectStatement(String statementKey, String fieldList, String fromString, String filterString, String orderString)
        {
            String query = "SELECT " + fieldList + " FROM " + fromString;
            if (filterString != null) query += " WHERE " + filterString;
            if (orderString != null) query += " ORDERY BY " + orderString;
            selectStatements.Add(statementKey, query);
        }

        public void AddSelectStatement(String table, String fieldList, String filterString, String orderString)
        {
            AddSelectStatement(table, fieldList, table, filterString, orderString);
        }


        //Select statement
        public List<Dictionary<String, Object>> Select(String statementKey, String fieldList, params string[] values)
        {
            List<Dictionary<String, Object>> result = new List<Dictionary<String, Object>>();

            String statement = GetStatement(selectStatements, statementKey);
            if (statement == null) throw new Exception(statementKey + " does not produce a statement");

            values = Utilities.Format.AddSlashes(values);
            statement = String.Format(statement, values);
            try
            {
                //open connection
                OpenConnection();

                MySqlCommand cmd = new MySqlCommand(statement, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                String[] fields = fieldList.Split(',');

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    Dictionary<String, Object> row = new Dictionary<String, Object>();
                    foreach (String field in fields)
                    {
                        row[field] = dataReader[field];
                    }

                    result.Add(row);
                }

                //close Data Reader
                dataReader.Close();
            }
            finally
            {
                CloseConnection();
            }

            return result;
        }

        public Dictionary<String, Object> SelectRow(String statementKey, String fieldList, params string[] values)
        {
            List<Dictionary<String, Object>> result = Select(statementKey, fieldList, values);
            return result.Count == 0 ? null : result[0];
        }

        //Count statement
        public int Count(String fromString, String filterString)
        {
            int count = -1;
            String statement = "SELECT COUNT(*) FROM " + fromString;
            if (filterString != null) statement += " WHERE " + filterString;
            try
            {
                //open connection
                OpenConnection();

                MySqlCommand cmd = new MySqlCommand(statement, connection);
                count = int.Parse(cmd.ExecuteScalar() + "");
            }
            finally
            {
                CloseConnection();
            }
            return count;
        }

        public int Count(String fromString)
        {
            return Count(fromString, null);
        }

        //Backup
        /*public void Backup()
        {
        }

        //Restore
        public void Restore()
        {
        }*/
    }
}
