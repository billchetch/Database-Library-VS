using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Chetch.Database
{
    public class DBRow : Dictionary<String, Object>
    {
        public long ID { get; set; } = 0;
        public String IDFieldName { get; set; } = "id";

        virtual public void AddField(String fieldName, Object fieldValue)
        {
            this[fieldName] = fieldValue;
            if(fieldName.Equals(IDFieldName, StringComparison.Ordinal))
            {
                try
                {
                    ID = System.Convert.ToInt64(fieldValue);
                } catch (Exception)
                {
                    //allow to pass through
                }
            }
        }
    }

    public class LogEntry : DBRow
    {
        public enum LogEntryType
        {
            INFO,
            WARNING,
            ERROR
        }
        public DateTime Created { get; internal set; }
        public String LogName { get; internal set; }
        public LogEntryType LogType { get; internal set; }
        public String Entry { get; internal set; }


        override public void AddField(String fieldName, Object fieldValue)
        {
            base.AddField(fieldName, fieldValue);

            switch (fieldName)
            {
                case "created":
                    //TODO: parse in to date object
                    break;
                case "LogName":
                    LogName = (String)fieldValue;
                    break;
                case "log_entry_type":
                    LogType = (LogEntryType)Enum.Parse(typeof(LogEntryType), (String)fieldValue);
                    break;
                case "log_entry":
                    Entry = (String)fieldName;
                    break;
            }
        }
    }

    public class IDMap<T> : Dictionary<T, DBRow>
    {
        public static IDMap<T> Create(List<DBRow> rows, String idName = "id")
        {
            IDMap<T> idm = new IDMap<T>();
            if(rows.Count > 0 && !rows[0].ContainsKey(idName))
            {
                throw new Exception("First row does not contain id key " + idName);
            }
            T id;
            foreach(var r in rows)
            {
                if (r[idName] is long || r[idName] is System.UInt32 || r[idName] is int)
                {
                    id = (T)(Object)System.Convert.ToInt64(r[idName]);
                }
                else if (r[idName] is String)
                {
                    id = (T)r[idName];
                } else
                {
                    //TODO: deal with other types...
                    id = (T)r[idName];
                }
                if (idm.ContainsKey(id)) throw new Exception("Key is not unique");
                idm[id] = r;
            }
            return idm;
        }
    }

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

        public String LogTableName { get; internal set; } = "sys_logs";

        //static template factory method
        static public D Create<D>(String server, String database, String uid, String password) where D : DB, new()
        {
            var db = new D();
            db.Configure(server, database, uid, password);
            db.Initialize();
            return db;
        }

        static public D Create<D>(System.Configuration.ApplicationSettingsBase settings, String[] keys) where D : DB, new()
        {
            var db = new D();
            db.Configure(settings, keys);
            db.Initialize();
            return db;
        }

        static public D Create<D>(System.Configuration.ApplicationSettingsBase settings) where D : DB, new()
        {
            var keys = new String[] { "DBServer", "DBName", "DBUsername", "DBPassword" };
            return Create<D>(settings, keys);
        }

        static public D Create<D>(System.Configuration.ApplicationSettingsBase settings, String dbnameKey) where D : DB, new()
        {
            var keys = new String[] { "DBServer", dbnameKey, "DBUsername", "DBPassword" };
            return Create<D>(settings, keys);
        }


        //Constructor
        public DB()
        {
            //empty constructor for static template factory method
        }

        public DB(String server, String database, String uid, String password)
        {
            Configure(server, database, uid, password);
            Initialize();
        }

        //config
        public void Configure(String server, String database, String uid, String password)
        {
            this.server = server;
            this.database = database;
            this.uid = uid;
            this.password = password;
        }

        public void Configure(System.Configuration.ApplicationSettingsBase settings, String[] keys)
        {
            if (keys == null || keys.Length != 4) throw new Exception("Incorrect number of keys ... must be 4");

            var pwd = Chetch.Utilities.BasicEncryption.Decrypt((String)settings[keys[3]], "dbpasswd");
            Configure((String)settings[keys[0]],
                        (String)settings[keys[1]],
                        (String)settings[keys[2]],
                        pwd);
        }


        //Initialize values and connect
        virtual public void Initialize()
        {
            String connectionString;
            connectionString = "SERVER=" + this.server + ";" + "DATABASE=" +
            this.database + ";" + "UID=" + this.uid + ";" + "PASSWORD=" + password + ";SslMode=none";

            connection = new MySqlConnection(connectionString);

            //include logging
            if (LogTableName != null)
            {
                AddSelectStatement(LogTableName + "All", "*", LogTableName, null, "created DESC", "{0}");
                AddSelectStatement(LogTableName, "*", "log_name='{0}'", "created DESC", "{1}");

                AddInsertStatement(LogTableName, "log_name='{0}', log_entry_type='{1}', log_entry='{2}'");
            }
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
            return ExecuteWriteStatement(statement, values);
        }

        private MySqlCommand ExecuteWriteStatement(String statement, params string[] values)
        {
            if (statement == null) throw new Exception("No statement provied");

            //only if we have values...
            if (values.Length > 0)
            {
                //TODO: trim whitespace from values
                values = Utilities.Format.AddSlashes(values);
                statement = String.Format(statement, values);
            }

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

        protected String GenerateParamString(Dictionary<String, Object> vals)
        {
            String s = "";
            foreach (var v in vals)
            {
                s += (s.Length > 0 ? ", " : "") + v.Key + "='" + v.Value + "'";
            }
            return s;
        }

        //Insert statement
        public long Insert(String statementKey, params string[] values)
        {
            MySqlCommand cmd = ExecuteWriteStatement(insertStatements, statementKey, values);
            return cmd.LastInsertedId;
        }

        public long Insert(String tableName, Dictionary<String, Object> vals)
        {
            Dictionary<String, String> temp = new Dictionary<string, string>();
            String paramString = GenerateParamString(vals);
            String statement = "INSERT INTO " + tableName + " SET " + paramString;
            MySqlCommand cmd = ExecuteWriteStatement(statement);
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

        public void Update(String tableName, Dictionary<String, Object> vals, String filter)
        {
            Dictionary<String, String> temp = new Dictionary<string, string>();
            String paramString = GenerateParamString(vals);
            String statement = "UPDATE " + tableName + " SET " + paramString + " WHERE " + filter;
            ExecuteWriteStatement(statement);
        }

        public void Update(String tableName, Dictionary<String, Object> vals, long id, String idName = "id")
        {
            String filter = tableName + "." + idName + "=" + id;
            Update(tableName, vals, filter);
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

        public void AddSelectStatement(String statementKey, String fieldList, String fromString, String filterString, String orderString, String limitString = null)
        {
            String query = "SELECT " + fieldList + " FROM " + fromString;
            if (filterString != null) query += " WHERE " + filterString;
            if (orderString != null) query += " ORDER BY " + orderString;
            if (limitString != null) query += " LIMIT " + limitString;
            selectStatements.Add(statementKey, query);
        }

        public void AddSelectStatement(String table, String fieldList, String filterString, String orderString, String limitString = null)
        {
            AddSelectStatement(table, fieldList, table, filterString, orderString, limitString);
        }


        //Select statement

        public List<DBRow> Select(String statementKey, String fieldList, params string[] values)
        {
            return Select<DBRow>(statementKey, fieldList, values);
        }

        public List<T> Select<T>(String statementKey, String fieldList, params string[] values) where T : DBRow, new()
        {
            List<T> result = new List<T>();

            String statement = GetStatement(selectStatements, statementKey);
            if (statement == null) throw new Exception(statementKey + " does not produce a statement");

            if (values.Length > 0)
            {
                values = Utilities.Format.AddSlashes(values);
                statement = String.Format(statement, values);
            }
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
                    T row = new T();

                    foreach (String field in fields)
                    {
                        String f = field.Trim();
                        row.AddField(f, dataReader[f]);
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

        public T SelectRow<T>(String statementKey, String fieldList, params string[] values) where T : DBRow, new()
        {
            List<T> result = Select<T>(statementKey, fieldList, values);
            return result.Count == 0 ? null : result[0];
        }

        public DBRow SelectRow(String statementKey, String fieldList, params string[] values)
        {
            return SelectRow<DBRow>(statementKey, fieldList, values);
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

        //General methods for things such as logging or sys info

        public List<LogEntry> GetLogEntries(String logName = null, int numberOfRows = 100)
        {
            if (logName == null) {
                return Select<LogEntry>(LogTableName + "All", "id,created,log_entry_type,log_entry", numberOfRows.ToString());
            } else
            {
                return Select<LogEntry>(LogTableName, "id,created,log_entry_type,log_entry", logName, numberOfRows.ToString());
            }
        }

        public long Log(String logName, LogEntry.LogEntryType type, String entry)
        {
            return Insert(LogTableName, logName, type.ToString(), entry);
        }
           
        public long LogInfo(String logName, String entry)
        {
            return Log(logName, LogEntry.LogEntryType.INFO, entry);
        }
    }
}
